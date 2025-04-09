FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logger/ /app/logger/

COPY loader/ /app/

CMD ["tail", "-f", "/dev/null"]