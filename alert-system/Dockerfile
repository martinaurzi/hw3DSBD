FROM python:3.14-slim

LABEL authors="drusso"

WORKDIR /app

ARG BUILD_DIR
COPY ${BUILD_DIR}/. .

RUN pip install --no-cache-dir confluent-kafka

CMD ["python", "app.py"]

