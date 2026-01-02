FROM python:3.14-slim

WORKDIR /app

ARG BUILD_DIR
COPY ${BUILD_DIR}/. .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "app.py"]

