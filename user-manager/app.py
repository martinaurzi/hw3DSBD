import logging

from flask import Flask, jsonify, request, Response
import os
import pymysql
import bcrypt
import time

import grpc
from concurrent import futures
import threading

import prometheus_client

import user_service_pb2
import user_service_pb2_grpc

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)

LISTEN_PORT = int(os.getenv("LISTEN_PORT", 5003))
LISTEN_PORT_GRPC = int(os.getenv("LISTEN_PORT_GRPC", 50051))

GRPC_HOST = os.getenv("GRPC_HOST")
GRPC_SEND_PORT = int(os.getenv("GRPC_SEND_PORT", 50052))

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_USERNAME = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

DATA_COLLECTOR_ADDRESS = f"{GRPC_HOST}:{GRPC_SEND_PORT}"

cache_message_ids = {}

SERVICE_NAME = os.getenv("SERVICE_NAME", "unknown-service")
NODE_NAME = os.getenv("NODE_NAME", "unknown-node")

REQUEST_COUNTER = prometheus_client.Counter(
    "usermanager_requests_total",
    "Totale richieste ricevute",
    ["service", "node", "endpoint"]
)

DB_QUERY_DURATION = prometheus_client.Gauge(
    "usermanager_db_query_duration_seconds",
    "Durata ultima query in secondi",
    ["service", "node", "operation"]
)

# Funzione per la connessione al database MySQL
def get_connection():
    try:
        mysql_conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USERNAME,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            cursorclass=pymysql.cursors.DictCursor
        )

        mysql_conn.ping(reconnect=True)
        logging.info(f"Connessione MySQL stabilita su {MYSQL_HOST}:{MYSQL_PORT}, DB={MYSQL_DATABASE}")

        return mysql_conn

    except pymysql.MySQLError as e:
        logging.error(f"Errore connessione MySQL: {e}")
        return None

# Svuota la cache ogni 10 minuti
def clear_cache():
    cache_message_ids.clear()

    threading.Timer(600, clear_cache).start()

@app.route("/metrics")
def metrics():
    return Response(
        prometheus_client.generate_latest(),
        mimetype="text/plain"
    )

@app.route("/")
def home():
    return jsonify({"message": "Hello User Manager!"}), 200

@app.route("/create", methods=["POST"])
def create_user():
    start_time = time.time()
    endpoint = "/create"

    data = request.json

    message_id = data.get("messageID") # per at-most-once
    email = data.get("email")
    nome = data.get("nome")
    cognome = data.get("cognome")
    password = data.get("password")

    REQUEST_COUNTER.labels(
        service=SERVICE_NAME,
        node=NODE_NAME,
        endpoint=endpoint
    ).inc()

    # Verificare se message_id si trova nella cache
    if message_id and message_id in cache_message_ids:
        return jsonify({"error": "Richiesta già elaborata [At-most-once]"}), 409

    else:
        if not email or not password:
            return jsonify({"error": "Email e password obbligatorie"}), 400

        # Inserisco il messaggio nella cache
        cache_message_ids[message_id] = {
            "email": email,
            "timestamp": time.time()
        }

        mysql_conn = get_connection()
        if mysql_conn:
            with mysql_conn.cursor() as cursor:
                # Verfico se l'utente esiste
                sql_email = "SELECT * FROM users WHERE email=%s"
                cursor.execute(sql_email, (email,))
                existing = cursor.fetchone()

                if existing:
                    return jsonify({"error": f"Email {email} già in uso"}), 409

                hashed_pw = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())

                try:
                    # Inserisco l'utente nella tabella users
                    sql_insert_user = "INSERT INTO users (email, nome, cognome, password_hash) VALUES (%s, %s, %s, %s)"
                    cursor.execute(sql_insert_user,(email, nome, cognome, hashed_pw))

                    mysql_conn.commit()
                    return jsonify({"message": f"Utente {email} registrato con successo"}), 201

                except pymysql.MySQLError as e:
                    mysql_conn.rollback()
                    logging.error(f"Errore DB durante create_user: {e}")
                    return jsonify({"error": "Errore interno del database"}), 500

                finally:
                    mysql_conn.close()

                    duration = time.time() - start_time

                    DB_QUERY_DURATION.labels(
                        service=SERVICE_NAME,
                        node=NODE_NAME,
                        operation="create_user",
                    ).set(duration)
        else:
            return jsonify({"error": "Database MySQl non disponibile"}), 503


@app.route("/delete/<email>", methods=["DELETE"])
def delete_user(email):
    start_time = time.time()
    endpoint = "/delete/<email>"

    REQUEST_COUNTER.labels(
        service=SERVICE_NAME,
        node=NODE_NAME,
        endpoint=endpoint
    ).inc()

    mysql_conn = get_connection()

    if mysql_conn:
        with mysql_conn.cursor() as cursor:
            cursor.execute("SELECT * FROM users WHERE email=%s", (email,))

            user = cursor.fetchone()

            if not user:
                return jsonify({"error": "Utente non trovato"}), 404

            try:
                cursor.execute("DELETE FROM users WHERE email=%s", (email,))

                mysql_conn.commit()

                logging.info(f"Utente {email} cancellato con successo")

                mysql_conn.close()

                # Comunico tramite il canale gRPC col data-collector per eliminare le righe corrispondenti
                # all'utente eliminato dalla tabella user-airports
                try:
                    with grpc.insecure_channel(DATA_COLLECTOR_ADDRESS) as channel:
                        stub = user_service_pb2_grpc.DataServiceStub(channel)

                        response = stub.DeleteUserInterests(user_service_pb2.UserCheckRequest(email=email))

                        if response.deleted:
                            return jsonify({"message": f"Interessi dell'utente {email} cancellati con successo"}), 200

                        else:
                            return jsonify({"error": f"Errore nella cancellazione degli interessi dell'utente {email}"}), 500

                except grpc.RpcError as e:
                    logging.error(f"Errore gRPC durante delete_user: {e}")
                    return jsonify({"error": "Errore di comunicazione con il data-collector"}), 502

            except pymysql.MySQLError as e:
                    mysql_conn.rollback()
                    logging.error(f"Errore DB durante delete_user: {e}")
                    return jsonify({"error": "Errore interno del database"}), 500

            finally:
                duration = time.time() - start_time

                DB_QUERY_DURATION.labels(
                    service=SERVICE_NAME,
                    node=NODE_NAME,
                    operation="delete_user"
                ).set(duration)
                # garantisce la chiusura della connessione nel caso in cui non sia già stata chiusa
                try:
                    mysql_conn.close()
                except Exception:
                    pass
    else:
        return jsonify({"error": "Database MySQl non disponibile"}), 503

class UserManagerService(user_service_pb2_grpc.UserServiceServicer):
    def CheckIfUserExists(self, request, context):
        email = request.email

        mysql_conn = get_connection()

        if mysql_conn:
            with mysql_conn.cursor() as cursor:
                cursor.execute("SELECT * FROM users WHERE email=%s", (email,))
                user = cursor.fetchone()

                exists = user is not None

            mysql_conn.close()

            return user_service_pb2.UserCheckResponse(
                exists=exists,
                message="Utente trovato" if exists else "Utente non trovato"
            )
        else:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("MySQL non connesso")

            return user_service_pb2.UserCheckResponse(
                exists=False,
                message="Errore: MySQL non connesso"
            )

def serve():
    # Creo un server grpc con thread pool di 10 worker threads
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    user_service_pb2_grpc.add_UserServiceServicer_to_server(UserManagerService(), server)

    server.add_insecure_port(f'[::]:{LISTEN_PORT_GRPC}') # Lego il server alla porta

    server.start()

    logging.info(f"UserService è pronto ed in ascolto sulla porta {LISTEN_PORT_GRPC}")

    server.wait_for_termination()

if __name__ == "__main__":
    # Avvia gRPC in un thread separato
    threading.Thread(target=serve, daemon=True).start()

    # Thread per pulire la cache
    threading.Thread(target=clear_cache, daemon=True).start()

    # Avvia Flask
    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=False)


