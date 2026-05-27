FROM python:3.11-slim

WORKDIR /app

COPY apps/serving_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/serving_service/ /app/apps/serving_service/
COPY libs/datalake/ /app/datalake/

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "apps.serving_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
