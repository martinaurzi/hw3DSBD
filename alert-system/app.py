from confluent_kafka import Producer, Consumer
import os
import json
import logging

import time

import prometheus_client

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
)

# Metriche Prometheus
SERVICE_NAME = os.getenv("SERVICE_NAME", "data-collector-service")
NODE_NAME = os.getenv("NODE_NAME", "data-collector-node")

KAFKA_SENT_ALERTS = prometheus_client.Counter(
    'kafka_alert_total',
    'Numero totale dei messaggi pubblicati dal alert-system sul topic to-notifier',
    ["service", "node"]
)

KAFKA_PROCESSING_TIME = prometheus_client.Gauge(
    'kafka_processing_time_seconds',
    'Tempo di elaborazione di un messaggio Kafka',
    ["service", "node"]
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_INPUT = "to-alert-system"
TOPIC_OUTPUT = "to-notifier"

consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'to-alert-system',
    'auto.offset.reset': 'earliest',
}

producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'acks': 'all',
    'retries': 3,
    'linger.ms': 10,
    'batch.size': 16000,
    'max.in.flight.requests.per.connection': 1
}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

consumer.subscribe([TOPIC_INPUT])

# Callback per confermare la consegna del messaggio su to-notifier
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Errore nell'invio: {err}")
    else:
        logging.info(f"Messaggio consegnato a {msg.topic()}")

# Funzione per inviare un messaggio sul topic to-notifier
def send_to_notifier(message):
    try:
        producer.produce(
            TOPIC_OUTPUT,
            json.dumps(message).encode("utf-8"),
            callback = delivery_report
        )

        # Incremento la metrica Prometheus
        KAFKA_SENT_ALERTS.labels(service=SERVICE_NAME, node=NODE_NAME).inc()

        producer.poll(0)

        logging.info(f"Messaggio inviato a {TOPIC_OUTPUT}: {message}")
    except Exception as e:
        logging.error(f"Errore nell'invio al notifier: {e}")

# Funzione per processare il messaggio letto dal topic to-alert-system
def process_message(msg_value):
    try:
        start = time.time()

        data = json.loads(msg_value)
        email = data.get("email")
        airport = data.get("airport")
        new_flights = data.get("new_flights") or []
        high_value = data.get("high_value")
        low_value = data.get("low_value")

        num_flights = len(new_flights)

        if high_value is not None and num_flights > high_value:
            alert = {
                "email": email,
                "airport": airport,
                "condition": f"Superata soglia HIGH ({num_flights} > {high_value})"
            }
            send_to_notifier(alert)

        if low_value is not None and num_flights < low_value:
            alert = {
                "email": email,
                "airport": airport,
                "condition": f"Sotto soglia LOW ({num_flights} < {low_value})"
            }
            send_to_notifier(alert)

        processing_time = time.time() - start

        # Aggiorno la metrica Prometheus
        KAFKA_PROCESSING_TIME.labels(service=SERVICE_NAME, node=NODE_NAME).set(processing_time)

    except Exception as e:
        logging.error(f"Errore nella elaborazione messaggio: {e}")

def run_alert_system():
    try:
        logging.info("AlertSystem started")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logging.error(f"Errore consumer: {msg.error()}")
                continue

            process_message(msg.value().decode("utf-8"))
    except KeyboardInterrupt:
        logging.info("AlertSystem interrotto manualmente")

    except Exception as e:
        logging.error(f"Errore inatteso: {e}")

    finally:
        consumer.close()
        producer.flush()
        logging.info("Risorse Kafka chiuse correttamente")

if __name__ == "__main__":
    prometheus_client.start_http_server(5004)
    run_alert_system()