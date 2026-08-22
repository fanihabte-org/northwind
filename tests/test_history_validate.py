from generator.history_validate import CHECKS

def test_history_validation_covers_all_lifecycle_sources() -> None:
    assert set(CHECKS) == {"orders", "shipments", "invoices", "support_cases"}
    assert all(value[1].endswith("_status_history") for value in CHECKS.values())
