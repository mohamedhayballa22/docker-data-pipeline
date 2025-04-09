FROM python:3.13-alpine

WORKDIR /app

COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logger/ /app/logger/

COPY api/ /app/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]