from __future__ import annotations

from generator import metadata_registry as registry


def test_fingerprint_is_stable_for_a_normalized_column_contract() -> None:
    columns = [
        {"ordinal_position": 1, "column_name": "id", "data_type": "bigint", "is_nullable": False},
        {"ordinal_position": 2, "column_name": "name", "data_type": "text", "is_nullable": True},
    ]

    assert registry.fingerprint(columns) == registry.fingerprint([dict(item) for item in columns])
    assert registry.fingerprint(columns) != registry.fingerprint(list(reversed(columns)))


def test_sync_system_records_each_source_table_once(monkeypatch) -> None:
    source_tables = {
        "orders": [{"ordinal_position": 1, "column_name": "order_id"}],
        "shipments": [{"ordinal_position": 1, "column_name": "shipment_id"}],
    }
    calls: list[tuple[str, str, str, list[dict[str, object]], bool]] = []
    monkeypatch.setattr(registry, "read_columns", lambda _connection, _schema: source_tables)

    def record(_connection, system, schema, table, columns, dry_run):
        calls.append((system, schema, table, columns, dry_run))
        return {"table": table, "changed": True}

    monkeypatch.setattr(registry, "record_table", record)

    assert registry.sync_system(object(), object(), "ops", dry_run=True) == [
        {"table": "orders", "changed": True},
        {"table": "shipments", "changed": True},
    ]
    assert [call[:3] for call in calls] == [("ops", "ops", "orders"), ("ops", "ops", "shipments")]
    assert all(call[-1] is True for call in calls)
