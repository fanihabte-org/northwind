from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_ops_and_erp_have_independent_schema_scripts() -> None:
    ops = (ROOT / "sql" / "01_ops.sql").read_text()
    erp = (ROOT / "sql" / "02_erp.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS ops.orders" in ops
    assert "CREATE TABLE IF NOT EXISTS ops.invoices" in ops
    assert "REFERENCES erp." not in ops
    assert "CONSTRAINT fk_orders_cust FOREIGN KEY" in ops
    assert "CONSTRAINT pk_orders PRIMARY KEY" in ops
    assert "CONSTRAINT fk_invoices_order FOREIGN KEY" in ops
    assert "CREATE TABLE IF NOT EXISTS erp.revenue_postings" in erp
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
    assert 'ports: ["5433:5432"]' in compose
    assert 'ports: ["5434:5432"]' in compose
    assert "  simulator:\n" in compose
    assert "SIMULATION_BASELINE_DATE" in compose
