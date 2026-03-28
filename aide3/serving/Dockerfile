FROM python:3.11-slim

WORKDIR /app

COPY aide3/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY aide3 /app/aide3

EXPOSE 8080

CMD ["uvicorn", "aide3.serving.api:app", "--host", "0.0.0.0", "--port", "8080"]

