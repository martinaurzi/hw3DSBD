from flask import Flask, jsonify, request, Response
import os
import pymysql.cursors
import requests
import grpc
import user_service_pb2
import user_service_pb2_grpc

from circuit_breaker import CircuitBreaker, CircuitBreakerOpenException

import time
from datetime import datetime, timezone

from concurrent import futures
import threading

import logging

from confluent_kafka import Producer
import json

import prometheus_client

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s',)

LISTEN_PORT = int(os.getenv("LISTEN_PORT", 5002))
LISTEN_PORT_GRPC = int(os.getenv("LISTEN_PORT_GRPC=50052", 50052))

GRPC_HOST = os.getenv("GRPC_HOST")
GRPC_SEND_PORT= int(os.getenv("GRPC_SEND_PORT", 50051))

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USERNAME = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

OPENSKY_CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")

OPENSKY_DEPARTURE_ENDPOINT = "https://opensky-network.org/api/flights/departure"
OPENSKY_ARRIVAL_ENDPOINT = "https://opensky-network.org/api/flights/arrival"
OPENSKY_TOKEN_ENDPOINT = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

TIMEOUT_SECONDS = 10

USER_MANAGER_ADDRESS = f"{GRPC_HOST}:{GRPC_SEND_PORT}"

circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5, expected_exception=requests.exceptions.RequestException)

# Configurazione producer Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'acks': 'all',
    'retries': 3,
    'linger.ms': 10,
    'batch.size': 16000,
    'max.in.flight.requests.per.connection': 1
}

producer = Producer(producer_config)
TOPIC_ALERT = "to-alert-system"

# Metriche Prometheus
SERVICE_NAME = os.getenv("SERVICE_NAME", "data-collector-service")
NODE_NAME = os.getenv("NODE_NAME", "data-collector-node")

KAFKA_SENT_MESSAGES = prometheus_client.Counter(
    'kafka_messages_total',
    'Numero totale dei messaggi pubblicati dal data-collector sul topic to-alert-system',
    ["service", "node"]
)

OPENSKY_FLIGHTS_UPDATE_DURATION = prometheus_client.Gauge(
    'opensky_flights_update_duration_seconds',
    'Durata chiamata opensky per aggiornamento dei voli',
    ["service", "node", "endpoint"]
)

OPENSKY_GET_TOKEN_DURATION = prometheus_client.Gauge(
    'opensky_get_token_duration',
    'Durata chiamata opensky per ottenere il token',
    ["service", "node"]
)

# Callback per confermare la consegna del messaggio su to-alert-system
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Errore nell'invio: {err}")
    else:
        logging.info(f"Messaggio consegnato a {msg.topic()}")

# Funzione per inviare un messaggio su to-alert-system
def send_kafka_message(message):
    if not message:
        return

    try:
        producer.produce(
            TOPIC_ALERT,
            json.dumps(message).encode("utf-8"),
            callback=delivery_report
        )

        # Incremento la metrica Prometheus
        KAFKA_SENT_MESSAGES.labels(service=SERVICE_NAME, node=NODE_NAME).inc()

        producer.poll(0)
    except BufferError:
        logging.error("Buffer pieno, il messaggio non è stato accodato")

# Funzione per la connessione al database MySQL
def get_connection():
    try:
        mysql_conn = pymysql.connect(host=MYSQL_HOST,
                                     user=MYSQL_USERNAME,
                                     password=MYSQL_PASSWORD,
                                     database=MYSQL_DATABASE,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        return mysql_conn
    except pymysql.MySQLError as e:
        logging.error(f"Impossibile connettersi a MySQL: {e}")
        return None

# Verifica se l'utente esiste comunicando tramite il canale gRPC con user-manager
def check_if_exists(email: str) -> bool:
    try:
        with grpc.insecure_channel(USER_MANAGER_ADDRESS) as channel:
            stub = user_service_pb2_grpc.UserServiceStub(channel)

            response = stub.CheckIfUserExists(user_service_pb2.UserCheckRequest(email=email))

            return response.exists
    except grpc.RpcError as e:
        logging.error("Errore gRPC")
        return False

def get_opensky_token():
    payload = {
        "grant_type": "client_credentials",
        "client_id": OPENSKY_CLIENT_ID,
        "client_secret": OPENSKY_CLIENT_SECRET
    }

    start_time = time.time()

    response = requests.post(OPENSKY_TOKEN_ENDPOINT, data=payload, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()

    duration = time.time() - start_time

    OPENSKY_GET_TOKEN_DURATION.labels(
        service=SERVICE_NAME,
        node=NODE_NAME,
    ).set(duration)

    data = response.json()

    return data.get("access_token")

# Funzione per recuperare gli aeroporti di interesse dell'utente con high_value e low_value
def get_user_airports(mysql_conn, email: str) -> list[dict]:
    try:
        with mysql_conn.cursor() as cursor:
            sql_get_aeroporti = """
                SELECT icao_aeroporto, high_value, low_value
                FROM user_airports
                WHERE email_utente = %s
            """
            cursor.execute(sql_get_aeroporti, (email,))
            rows = cursor.fetchall()

            return rows
    except pymysql.MySQLError as e:
        logging.error(f"Non è stato possibile recuperare gli interessi dell'utente: {e}")
        return []

# Calcola il parametro begin necessario per l'API di OpenSky
def get_begin_unix_time() -> int:
    current_time_utc = datetime.now(timezone.utc)

    current_time_timestamp = int(current_time_utc.timestamp())

    hours_in_seconds = 24 * 60 * 60

    return current_time_timestamp - hours_in_seconds

# Restituisce l'istante attuale in unix time
def get_current_unix_time() -> int:
    return int(time.time())

# Questa funzione effettua la chiamata all'API di OpenSky
def update_flights(mysql_conn, email_utente, opensky_endpoint, token):
    if token:
        # Recupero gli interessi dell'utente
        icao_list = get_user_airports(mysql_conn, email_utente)

        headers = {
            "Authorization": f"Bearer {token}"
        }

        begin = get_begin_unix_time()
        end = get_current_unix_time()

        # Per ogni aeroporto di interesse dell'utente prendo i voli
        for row in icao_list:
            icao = row["icao_aeroporto"]
            high_value = row["high_value"]
            low_value = row["low_value"]

            params = {
                "airport": icao,
                "begin": begin,
                "end": end
            }

            start_time = time.time()

            response = requests.get(opensky_endpoint, params=params, headers=headers)
            response.raise_for_status()

            data_flights = response.json()

            if data_flights:
                for flights in data_flights:
                    icao_aereo = flights.get("icao24")
                    first_seen = flights.get("firstSeen")
                    aeroporto_partenza = flights.get("estDepartureAirport")
                    last_seen = flights.get("lastSeen")
                    aeroporto_arrivo = flights.get("estArrivalAirport")

                    # Inserisco il volo corrente nella tabella flight
                    with mysql_conn.cursor() as cursor:
                        try:
                            sql_flights = ("INSERT IGNORE INTO flight (icao_aereo, first_seen, aeroporto_partenza, "
                                           "last_seen, aeroporto_arrivo) VALUES (%s, %s, %s, %s, %s)")

                            cursor.execute(sql_flights, (icao_aereo, first_seen, aeroporto_partenza, last_seen, aeroporto_arrivo))

                            mysql_conn.commit()
                        except pymysql.MySQLError as e:
                            mysql_conn.rollback()
                            logging.error(f"Errore nell'inserimento del volo corrente nella tabella flight {e}")

                duration = time.time() - start_time

                OPENSKY_FLIGHTS_UPDATE_DURATION.labels(
                    service=SERVICE_NAME,
                    node=NODE_NAME,
                    endpoint=opensky_endpoint,
                ).set(duration)

                timestamp = datetime.now().isoformat()

                # Creazione del messaggio da inviare sul topic to-alert-system
                message = {
                    "email": email_utente,
                    "airport": icao,
                    "new_flights": data_flights,
                    "high_value": high_value,
                    "low_value": low_value,
                    "timestamp": timestamp
                }
                send_kafka_message(message)

    else:
        logging.error("Token OpenSky non valido")

# Aggiorna i voli per tutti gli utenti
def update_all_flights():
    mysql_conn = get_connection()

    if mysql_conn:
        token = None

        try:
            token = circuit_breaker.call(get_opensky_token)
        except CircuitBreakerOpenException:
            logging.error("Scheduler: Chiamata per token OpenSky non effettuata (Circuito OPEN)")
        except requests.exceptions.RequestException as e:
            logging.error(f"Scheduler: Errore nella richiesta. Non è stato possibile recuperare il token: {e}")

        if token:
            with mysql_conn.cursor() as cursor:
                try:
                    cursor.execute("SELECT DISTINCT email_utente FROM user_airports")
                    users = cursor.fetchall()
                except pymysql.MySQLError as e:
                    logging.error("Scheduler: Impossibile recuperare gli utenti")

            for u in users:
                email = u["email_utente"]

                try:
                    circuit_breaker.call(update_flights,mysql_conn, email, OPENSKY_DEPARTURE_ENDPOINT, token) #aggiorna le partenze entro 1 giorno
                    circuit_breaker.call(update_flights,mysql_conn, email, OPENSKY_ARRIVAL_ENDPOINT, token) #aggiorna gli arrivi entro l'ultimo giorno
                except CircuitBreakerOpenException:
                    logging.error("Scheduler: Chiamata per l'aggiornamento dei voli OpenSky non effettuata (Circuito OPEN)")
                except requests.exceptions.RequestException as e:
                    logging.error(f"Errore nella richiesta. Non è stato possibile recuperare i voli da OpenSky: {e}")
        else:
            logging.error("Scheduler: token OpenSky non valido")

        mysql_conn.close()
    else:
        logging.error("Scheduler: impossibile connettersi al db")

    logging.info("Scheduler: Voli aggiornati")

# Questa funzione permette di aggiornare in modo ciclico (ogni 12 ore) i voli relativi agli aeroporti a cui gli utenti sono interessati
def scheduler_job():
    with app.app_context():
        update_all_flights()

    threading.Timer(12 * 3600, scheduler_job).start()

@app.route("/")
def home():
    return jsonify({"message": "Hello Data Collector!"}), 200

@app.route("/metrics")
def metrics():
    return Response(prometheus_client.generate_latest(), mimetype=prometheus_client.CONTENT_TYPE_LATEST)

@app.route("/user/interests", methods=["POST"])
def add_interest():
    data = request.json

    email_utente = data.get("email_utente")
    aeroporti_icao = data.get("aeroporti_icao")
    high_value = data.get("high_value")
    low_value = data.get("low_value")

    if high_value is not None and low_value is not None:
        if high_value <= low_value:
            return jsonify({"error": "high_value deve essere maggiore di low_value"})

    if check_if_exists(email_utente):
        # Utente esiste
        mysql_conn = get_connection()

        # Inserisco gli aeroporti indicati dall'utente nella tabella airport
        if mysql_conn:
            try:
                with mysql_conn.cursor() as cursor:
                    for icao in aeroporti_icao:

                        sql_interest ="""
                            INSERT INTO user_airports (email_utente, icao_aeroporto, high_value, low_value)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            high_value = VALUES(high_value),
                            low_value = VALUES(low_value)
                            """

                        cursor.execute(sql_interest, (email_utente, icao, high_value, low_value))

                mysql_conn.commit()
            except pymysql.MySQLError as e:
                mysql_conn.rollback()
                return jsonify({"error": f"Non è stato possibile aggiornare gli interessi dell'utente: {e}"})

            token = None
            try:
                token = circuit_breaker.call(get_opensky_token)
            except CircuitBreakerOpenException:
                logging.error("Interessi dell'utente salvati, ma chiamata per token OpenSky non effettuata (Circuito OPEN)")
            except requests.exceptions.RequestException as e:
                logging.error(f"Scheduler: Errore nella richiesta. Non è stato possibile recuperare il token: {e}")

            if token:
                try:
                    circuit_breaker.call(update_flights,mysql_conn, email_utente, OPENSKY_DEPARTURE_ENDPOINT, token) #aggiorna le partenze entro 1 giorno
                    circuit_breaker.call(update_flights,mysql_conn, email_utente, OPENSKY_ARRIVAL_ENDPOINT, token) #aggiorna gli arrivi entro l'ultimo giorno
                except CircuitBreakerOpenException:
                    mysql_conn.close()
                    return jsonify({"message": "Interessi dell'utente salvati, ma chiamata per l'aggiornamento dei voli OpenSky non effettuata (Circuito OPEN)"}), 503
                except requests.exceptions.RequestException as e:
                    mysql_conn.close()
                    return jsonify({"message": f"Errore nella richiesta. Non è stato possibile recuperare i voli da OpenSky: {e}"}), 404

            mysql_conn.close()

            return jsonify({"message": "Interessi dell'utente inseriti e voli aggiornati"}), 200
        else:
            return jsonify({"error": "Impossibile connettersi al db"}), 503
    else:
        return jsonify({"error": "L'utente non esiste"}), 404

# Dato un aeroporto restituisce l'ultimo volo in partenza e l'ultimo volo in arrivo
@app.route("/airport/<icao>/last", methods=["GET"])
def get_last_flight(icao):
    mysql_conn = get_connection()

    if not mysql_conn:
        return jsonify({"error": "Impossibile connettersi al db"}), 503

    try:
        with mysql_conn.cursor() as cursor:
            # Ultimo volo in partenza
            sql_last_departure = "SELECT * FROM flight WHERE aeroporto_partenza = %s ORDER BY last_seen DESC LIMIT 1"
            cursor.execute(sql_last_departure, (icao,))
            last_departure = cursor.fetchone()

            # Ultimo volo di arrivo
            sql_last_arrival = "SELECT * FROM flight WHERE aeroporto_arrivo = %s ORDER BY last_seen DESC LIMIT 1"
            cursor.execute(sql_last_arrival, (icao,))
            last_arrival = cursor.fetchone()
    except pymysql.MySQLError as e:
        return jsonify({"error": f"Errore MySQL {e}"}), 500
    finally:
        mysql_conn.close()

    if not last_departure and not last_arrival:
        return jsonify({"error": "Nessun volo trovato"}), 404

    return jsonify({
        "last_departure": last_departure,
        "last_arrival": last_arrival
    }), 200

@app.route("/airport/<icao>/media", methods=["GET"])
def get_media_voli(icao):
    days = int(request.args.get("giorni", 7))
    now = get_current_unix_time()
    start = now - days * 86400 # 86400 secondi in un giorno

    mysql_conn = get_connection()

    if mysql_conn:
        try:
            with mysql_conn.cursor() as cursor:
                sql_media_voli = "SELECT COUNT(*) AS totale FROM flight WHERE (aeroporto_partenza = %s OR aeroporto_arrivo = %s) AND first_seen >= %s"
                cursor.execute(sql_media_voli, (icao, icao, start))

                totale = cursor.fetchone()["totale"]

            mysql_conn.close()

            media = totale / days

            return jsonify({"media_voli_giornaliera": media}), 200
        except pymysql.MySQLError:
            logging.error("Errore MySQL. Non è stato possibile calcolare la media dei voli")
    else:
        return jsonify({"errore": "Impossibile connettersi al db"}), 503

class DataCollectorService(user_service_pb2_grpc.DataServiceServicer):
    def DeleteUserInterests(self, request, context):
        email = request.email

        mysql_conn = get_connection()

        if mysql_conn:
            with mysql_conn.cursor() as cursor:
                try:
                    cursor.execute("DELETE FROM user_airports WHERE email_utente=%s", (email,))
                    mysql_conn.commit()
                except pymysql.MySQLError:
                    mysql_conn.rollback()

                    return user_service_pb2.DeleteUserInterestsResponse(
                        deleted = False,
                        message=f"Non è stato possibile eliminare gli interssi dell'utente {email}"
                    )
                finally:
                    mysql_conn.close()

            return user_service_pb2.DeleteUserInterestsResponse(
                deleted = True,
                message=f"Interessi dell'utente {email} eliminati"
            )
        else:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("MySQL non connesso")

            return user_service_pb2.DeleteUserInterestsResponse(
                deleted = False,
                message="Errore: Non è stato possibile connettersi al database"
            )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    user_service_pb2_grpc.add_DataServiceServicer_to_server(DataCollectorService(), server)

    server.add_insecure_port(f'[::]:{LISTEN_PORT_GRPC}')

    server.start()

    logging.info(f"DataService è pronto ed in ascolto sulla porta {LISTEN_PORT_GRPC}")

    server.wait_for_termination()

if __name__ == "__main__":
    # Thread per aggiornare i voli in modo ciclico
    threading.Thread(target=scheduler_job, daemon=True).start()

    # Thread per gRPC
    threading.Thread(target=serve, daemon=True).start()

    # Avvio il server Prometheus HTTP sulla porta 9999
    # prometheus_client.start_http_server(9999)

    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=False)