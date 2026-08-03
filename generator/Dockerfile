FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir "psycopg[binary]"

COPY generator /app/generator
COPY sql /app/sql

CMD ["python", "-m", "generator.migrate", "--system", "all"]
