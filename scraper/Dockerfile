FROM python:3.9-slim

WORKDIR /app

COPY scraper/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logger/ /app/logger/

COPY scraper/ /app/

CMD ["tail", "-f", "/dev/null"]