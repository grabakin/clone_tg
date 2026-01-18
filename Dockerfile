FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY mirror.py /app/mirror.py
COPY README.md /app/README.md

RUN mkdir -p /data
VOLUME ["/data"]

ENTRYPOINT ["python", "/app/mirror.py"]
