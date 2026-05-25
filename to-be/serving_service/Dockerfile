FROM python:3.11-slim

WORKDIR /app

COPY serving_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY serving_service/ /app/serving_service/
COPY datalake/ /app/datalake/

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "serving_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
