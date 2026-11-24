FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN useradd --create-home --uid 10001 app && chown app:app /app

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app consumer.py .
USER app

ENTRYPOINT ["python", "consumer.py"]
CMD ["--request-source", "sqs", "--request-queue", "cs5270-requests", "--storage", "dynamodb"]
