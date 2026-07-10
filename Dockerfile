FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN python -m grpc_tools.protoc -I. --python_out=. proto/PushDataV3ApiWrapper.proto \
    && touch proto/__init__.py

CMD ["python", "main.py"]
