"""Exercise the public read-only API and write portable timing measurements."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = os.getenv("PERF_BASE_URL", "http://localhost:8081").rstrip("/")
OUTPUT = Path(os.getenv("PERF_OUTPUT", "performance-report.json"))


def request(path: str, *, body: dict | None = None, token: str | None = None) -> tuple[int, dict[str, str], bytes, float]:
    payload = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if payload else {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    started = time.perf_counter()
    with urlopen(Request(f"{BASE_URL}{path}", data=payload, headers=headers), timeout=300) as response:
        return response.status, dict(response.headers.items()), response.read(), time.perf_counter() - started


def main() -> None:
    _, _, body, _ = request(
        "/services/oauth2/token",
        body={"grant_type": "client_credentials", "client_id": "performance", "client_secret": "performance"},
    )
    token = json.loads(body)["access_token"]
    report: dict[str, object] = {"base_url": BASE_URL, "queries": []}
    for soql in (
        "SELECT Id, Name FROM Account ORDER BY Id LIMIT 2000",
        "SELECT Id, AccountId, Amount FROM Opportunity ORDER BY Id LIMIT 2000",
    ):
        status, _headers, payload, elapsed = request(
            "/services/data/v60.0/query?" + urlencode({"q": soql}), token=token
        )
        result = json.loads(payload)
        assert status == 200, result
        report["queries"].append({"soql": soql, "seconds": elapsed, "records": len(result["records"])})

    status, _headers, payload, submit_seconds = request(
        "/services/data/v60.0/jobs/query",
        body={"operation": "query", "query": "SELECT Id, Name FROM Opportunity ORDER BY Id LIMIT 100000"},
        token=token,
    )
    job = json.loads(payload)
    assert status == 200, job
    deadline = time.monotonic() + 300
    polls = 0
    while True:
        status, _headers, payload, _ = request(f"/services/data/v60.0/jobs/query/{job['id']}", token=token)
        job = json.loads(payload)
        assert status == 200, job
        polls += 1
        if job["state"] == "JobComplete":
            break
        if job["state"] in {"Failed", "Aborted"} or time.monotonic() >= deadline:
            raise RuntimeError(f"Bulk Query did not complete: {job}")
        time.sleep(1)
    status, headers, payload, result_seconds = request(
        f"/services/data/v60.0/jobs/query/{job['id']}/results?maxRecords=50000", token=token
    )
    assert status == 200, payload.decode(errors="replace")
    report["bulk_query"] = {
        "submit_seconds": submit_seconds,
        "polls": polls,
        "result_seconds": result_seconds,
        "returned_records": int(headers["Sforce-NumberOfRecords"]),
        "returned_bytes": len(payload),
        "next_locator": headers.get("Sforce-Locator"),
    }
    OUTPUT.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(OUTPUT.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
