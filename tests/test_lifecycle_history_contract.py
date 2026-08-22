from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_data_dictionary_defines_the_lifecycle_history_contract() -> None:
    dictionary = (ROOT / "docs" / "DATA_DICTIONARY.md").read_text()

    assert "### Lifecycle-history contract" in dictionary
    for field in ("`occurred_at`", "`recorded_at`", "`source_event_id`", "`sla_due_at`", "`sla_status`", "`anomaly_type`"):
        assert field in dictionary
    assert "Dimension/master tables remain current-state only" in dictionary
    assert "`crm.opportunity_history` (`OpportunityHistory` over the API)" in dictionary
    for field in ("`PreviousStageName`", "`StageName`", "`SystemModstamp`"):
        assert field in dictionary
    assert "reconciliation reads (but never edits)" in dictionary
