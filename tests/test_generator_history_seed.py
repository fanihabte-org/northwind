from pathlib import Path

import pandas as pd

from generator.generate import generate


def test_generator_writes_empty_opportunity_history_schema(tmp_path: Path) -> None:
    generate(0.001, "parquet", tmp_path, 42)
    history = pd.read_parquet(tmp_path / "crm_opportunity_history.parquet")
    assert history.empty
    assert list(history.columns) == [
        "Id", "OpportunityId", "PreviousStageName", "StageName",
        "CreatedDate", "CreatedById", "SystemModstamp",
    ]
