FROM python:3.11-slim

RUN pip install --no-cache-dir "feast[aws]"

WORKDIR /feature_repo
COPY feature_store.yaml .

EXPOSE 6566

CMD ["feast", "serve", "--host", "0.0.0.0", "--port", "6566"]
