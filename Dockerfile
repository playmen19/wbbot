FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2

WORKDIR /bot

COPY bot.py .

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD python bot.py