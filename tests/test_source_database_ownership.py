from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_ops_and_erp_have_independent_schema_scripts() -> None:
    ops = (ROOT / "sql" / "migrations" / "ops" / "001_initial_schema.sql").read_text()
    erp = (ROOT / "sql" / "migrations" / "erp" / "001_initial_schema.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS ops.orders" in ops
    assert "CREATE TABLE IF NOT EXISTS ops.invoices" in ops
    assert "REFERENCES erp." not in ops
    assert "CONSTRAINT fk_orders_cust FOREIGN KEY" in ops
    assert "CONSTRAINT pk_orders PRIMARY KEY" in ops
    assert "CONSTRAINT fk_invoices_order FOREIGN KEY" in ops
    assert "CREATE TABLE IF NOT EXISTS erp.revenue_postings" in erp
    order_history = (ROOT / "sql" / "migrations" / "ops" / "005_add_order_status_history.sql").read_text()
    assert "REFERENCES erp." not in order_history
    assert "FOREIGN KEY (order_id) REFERENCES ops.orders" in order_history
    assert "UNIQUE (source_event_id)" in order_history
    assert "CREATE TABLE IF NOT EXISTS simulation.applied_events" in erp
    assert "REFERENCES ops." not in erp
    assert "CONSTRAINT fk_postings_co FOREIGN KEY" in erp
    assert "CONSTRAINT pk_postings PRIMARY KEY" in erp


def test_compose_uses_distinct_ops_and_erp_databases() -> None:
    compose = (ROOT / "docker-compose.yml").read_text()

    assert "  ops:\n" in compose
    assert "  erp:\n" in compose
    assert "POSTGRES_DB: ops" in compose
    assert "POSTGRES_DB: erp" in compose
    assert "POSTGRES_DB: rev_engine_pipeline" in compose
    assert "POSTGRES_DB: crm" in compose
    assert 'ports: ["5433:5432"]' in compose
    assert 'ports: ["5434:5432"]' in compose
    assert 'ports: ["5436:5432"]' in compose
    assert "  crm:\n" in compose
    assert 'northwind_crm:/var/lib/postgresql/data' in compose
    fakeforce = compose.split("  fakeforce:\n", 1)[1].split("  simulator:\n", 1)[0]
    assert 'ports: ["8081:8080"]' in fakeforce
    assert "restart: unless-stopped" in fakeforce
    assert "  simulator:\n" in compose
    assert "SIMULATION_BASELINE_DATE" in compose
    assert "SIMULATION_DUCKDB_MEMORY_LIMIT" in compose
    assert "shared_buffers=128MB" in compose
    assert "  migrations:\n" in compose
    assert "service_completed_successfully" in compose
    assert "ANALYTICS_PG_DSN" in compose
    assert "rev_engine_pipeline" in compose
    assert '"${NORTHWIND_SEED_DIR:-./seed}:/app/seed:ro"' in compose
    assert '"${NORTHWIND_STATE_DIR:-./state}:/app/state"' in compose
