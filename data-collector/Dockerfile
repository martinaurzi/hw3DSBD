FROM python:3.14-slim
LABEL authors="urzim"
WORKDIR /app
#COPY . .
ARG BUILD_DIR
COPY ${BUILD_DIR}/. .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./proto/user_service.proto /app/proto/user_service.proto
RUN python -m grpc_tools.protoc -I/app/proto --python_out=. --pyi_out=. --grpc_python_out=. /app/proto/user_service.proto

CMD ["python", "app.py"]