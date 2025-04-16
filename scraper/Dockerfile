FROM python:3.13-alpine

WORKDIR /app

COPY scraper/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logger/ /app/logger/

COPY scraper/ /app/

CMD ["python", "scraper.py"]