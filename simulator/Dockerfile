FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir duckdb pyarrow "psycopg[binary]"
COPY simulator /app/simulator

CMD ["python", "-m", "simulator.daemon"]
