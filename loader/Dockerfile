FROM python:3.13-alpine

WORKDIR /app

COPY loader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logger/ /app/logger/

COPY loader/ /app/

CMD ["python", "loader.py"]