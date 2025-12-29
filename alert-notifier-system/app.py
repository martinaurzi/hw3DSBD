from confluent_kafka import Consumer
import os
import logging
import json
import smtplib # Per inviare email usando il protocollo SMTP
from email.mime.text import MIMEText

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_INPUT = "to-notifier"

consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'to-notifier',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC_INPUT])

# Configurazione SMTP (Gmail)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

def send_email(to_email, subject, body):
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = SMTP_USER
        msg['To'] = to_email

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls() # avviare sessione sicura
            server.login(SMTP_USER, SMTP_PASS) # autenticazione
            server.sendmail(SMTP_USER, [to_email], msg.as_string()) # invio email

        logging.info(f"Email inviata a {to_email}: {subject}")
    except Exception as e:
        logging.error(f"Errore nell'invio email: {e}")

# Funzione per processare i messaggi ricevuti sul topic to-notifier
def process_message(msg_value):
    try:
        data = json.loads(msg_value)
        email = data.get("email")
        airport = data.get("airport")
        condition = data.get("condition")

        subject = f"Alert voli aeroporto {airport}"
        body = f"Ciao,\n\nCondizione rilevata: {condition}\n\nSaluti,\nAlertNotifierSystem"

        send_email(email, subject, body)

    except Exception as e:
        logging.error(f"Errore nella elaborazione messaggio: {e}")

def run_notifier():
    try:
        logging.info("AlertNotifierSystem started")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logging.error(f"Errore consumer: {msg.error()}")
                continue

            process_message(msg.value().decode("utf-8"))

    except KeyboardInterrupt:
        logging.info("AlertNotifierSystem interrotto manualmente")
    finally:
        consumer.close()
        logging.info("Consumer Kafka chiuso correttamente")

if __name__ == "__main__":
    run_notifier()