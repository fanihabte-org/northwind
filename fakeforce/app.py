"""
FakeForce — a Salesforce-shaped REST API that misbehaves on purpose.

Real Salesforce is well-run. That is exactly why it is a poor place to practise
failure handling: you cannot make it throttle you, drop a field, or return a 503
on demand. FakeForce imitates the *shape* of the Salesforce REST API (OAuth token,
SOQL query endpoint, `nextRecordsUrl` cursor pagination, `REQUEST_LIMIT_EXCEEDED`
errors, `/limits`) while letting you turn every failure mode on and off.

Run locally
-----------
    pip install -r requirements.txt
    uvicorn fakeforce.app:app --port 8080 --reload

Quick start
-----------
    TOKEN=$(curl -s -X POST localhost:8080/services/oauth2/token \\
      -d grant_type=client_credentials -d client_id=demo -d client_secret=demo \\
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

    curl -s -H "Authorization: Bearer $TOKEN" \\
      --get localhost:8080/services/data/v60.0/query \\
      --data-urlencode "q=SELECT Id, Name, AccountNumber FROM Account LIMIT 5" | jq

Chaos control
-------------
    curl -s localhost:8080/_chaos                                   # read knobs
    curl -s -X POST localhost:8080/_chaos -H 'content-type: application/json' \\
         -d '{"error_rate": 0.35, "latency_ms": 800}'               # break it
    curl -s -X POST localhost:8080/_chaos/outage -d '{"seconds": 120}'   # hard down
    curl -s -X POST localhost:8080/_chaos/reset                     # behave again

Every knob also has an env var (see DEFAULTS below), so you can start the
container already broken for a scheduled failure-drill run.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import os
import random
import secrets
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs
from typing import Any

import pyarrow as pa

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from fakeforce.catalog import DatasetCatalog
from fakeforce.bulk.dispatcher import BulkIngestDispatcher, BulkQueryDispatcher
from fakeforce.bulk.ingest import (
    BulkIngestWorker,
    IngestUploadStore,
    IngestUploadTooLarge,
    IngestValidationError,
    validate_ingest_create,
)
from fakeforce.bulk.jobs import BulkJob, BulkJobState, BulkJobType, InvalidJobTransition
from fakeforce.bulk.query import BulkQueryWorker
from fakeforce.bulk.results import BulkQueryResultStore, InvalidBulkLocator
from fakeforce.config import Settings
from fakeforce.cursor_artifacts import CursorArtifactStore
from fakeforce.deadline import QueryDeadlineExceeded
from fakeforce.diagnostics import Diagnostics
from fakeforce.engine import DuckDBEngine
from fakeforce.query_service import (
    LazyQueryService,
    QueryOptionError,
    QueryValidationError,
    parse_query_batch_size,
)
from fakeforce.recovery import recover_bulk_artifacts, recover_cursor_artifacts
from fakeforce.runtime import AdmissionController, classify_operation
from fakeforce.state import StateStore

API_VERSION = "v60.0"
SETTINGS = Settings.from_env()
CATALOG = DatasetCatalog.from_file(SETTINGS.catalog_path, SETTINGS.data_roots)
STATE_STORE = StateStore.from_settings(SETTINGS)
STATE_STORE.initialize()
STATE_STORE.register_snapshot(CATALOG.snapshot_id, CATALOG.snapshot_id)
ENGINE = DuckDBEngine(SETTINGS, CATALOG)
ENGINE.initialize_mutable_tables(STATE_STORE)
QUERY_SERVICE = LazyQueryService(CATALOG, ENGINE, API_VERSION)
CURSOR_ARTIFACTS = CursorArtifactStore(SETTINGS.state_directory / "artifacts" / "cursors")
recover_cursor_artifacts(STATE_STORE, CURSOR_ARTIFACTS)
RUNTIME = AdmissionController(
    SETTINGS.lightweight_request_workers,
    SETTINGS.heavy_query_workers,
    SETTINGS.bulk_workers,
)
DIAGNOSTICS = Diagnostics(SETTINGS, STATE_STORE, RUNTIME)
_BULK_QUERY_WORKER = BulkQueryWorker(SETTINGS, STATE_STORE, ENGINE, QUERY_SERVICE)
BULK_QUERY_DISPATCHER = BulkQueryDispatcher(SETTINGS, STATE_STORE, _BULK_QUERY_WORKER)
BULK_QUERY_RESULTS = BulkQueryResultStore(SETTINGS.bulk_query_results_directory)
BULK_INGEST_UPLOADS = IngestUploadStore(
    SETTINGS.state_directory / "bulk" / "ingest",
    disk_reserve_bytes=SETTINGS.disk_reserve_bytes,
)
_BULK_INGEST_WORKER = BulkIngestWorker(SETTINGS, STATE_STORE, ENGINE, BULK_INGEST_UPLOADS)
BULK_INGEST_DISPATCHER = BulkIngestDispatcher(
    SETTINGS, STATE_STORE, _BULK_INGEST_WORKER, BULK_QUERY_DISPATCHER.executor
)
recover_bulk_artifacts(STATE_STORE, BULK_QUERY_RESULTS, BULK_INGEST_UPLOADS)
BULK_QUERY_DISPATCHER.resume_pending()
BULK_INGEST_DISPATCHER.resume_pending()

# --------------------------------------------------------------------------
# Chaos knobs
# --------------------------------------------------------------------------

DEFAULTS: dict[str, Any] = {
    # probability that any data request returns a retryable 503
    "error_rate": float(os.getenv("FAKEFORCE_ERROR_RATE", 0.0)),
    # probability of a non-retryable 400 (bad field, malformed SOQL simulation)
    "bad_request_rate": float(os.getenv("FAKEFORCE_BAD_REQUEST_RATE", 0.0)),
    # artificial latency in milliseconds, applied to every data request
    "latency_ms": int(os.getenv("FAKEFORCE_LATENCY_MS", 0)),
    # probability a request hangs past a client timeout (adds 30s)
    "hang_rate": float(os.getenv("FAKEFORCE_HANG_RATE", 0.0)),
    # requests allowed per rolling 60s before 429 REQUEST_LIMIT_EXCEEDED
    "rate_limit_per_min": int(os.getenv("FAKEFORCE_RATE_LIMIT", 120)),
    # records per page; low values force you to implement cursor pagination
    "page_size": int(os.getenv("FAKEFORCE_PAGE_SIZE", 2000)),
    # ISO date; Accounts modified on/after this date gain a Territory__c field
    "drift_after": os.getenv("FAKEFORCE_DRIFT_AFTER", ""),
    # seconds remaining on a forced total outage
    "outage_until": 0.0,
    # daily API allocation reported by /limits
    "daily_api_limit": int(os.getenv("FAKEFORCE_DAILY_LIMIT", 15000)),
}

chaos: dict[str, Any] = dict(DEFAULTS)

# Request state. Cursor metadata is persisted in DuckDB, not process memory.
_tokens: dict[str, float] = {}
_request_times: list[float] = []

app = FastAPI(title="FakeForce", version="1.0", docs_url="/_docs")


# --------------------------------------------------------------------------
# Salesforce-shaped errors
# --------------------------------------------------------------------------

def sf_error(status: int, code: str, message: str, headers: dict | None = None) -> JSONResponse:
    """Salesforce returns a JSON *array* of error objects, not a bare object."""
    return JSONResponse(
        status_code=status,
        content=[{"message": message, "errorCode": code}],
        headers=headers or {},
    )


class SalesforceAuthenticationError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message


def _with_limit_info(response, usage: int) -> JSONResponse:
    response.headers["Sforce-Limit-Info"] = f"api-usage={usage}/{chaos['daily_api_limit']}"
    return response


def require_token(authorization: str | None) -> None:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise SalesforceAuthenticationError("Session expired or invalid")
    tok = authorization.split(" ", 1)[1].strip()
    exp = _tokens.get(tok)
    if exp is None:
        raise SalesforceAuthenticationError("Session expired or invalid")
    if exp < time.time():
        _tokens.pop(tok, None)
        raise SalesforceAuthenticationError("Session expired or invalid")


@app.exception_handler(SalesforceAuthenticationError)
async def salesforce_authentication_error(_, error: SalesforceAuthenticationError):
    return sf_error(401, "INVALID_SESSION_ID", error.message)


# --------------------------------------------------------------------------
# Middleware: chaos is applied to /services/data/* only
# --------------------------------------------------------------------------

@app.middleware("http")
async def chaos_middleware(request: Request, call_next):
    path = request.url.path

    if not path.startswith(f"/services/data/{API_VERSION}"):
        return await call_next(request)

    now = time.time()

    # 1. hard outage
    if chaos["outage_until"] > now:
        remaining = int(chaos["outage_until"] - now)
        return _with_limit_info(sf_error(
            503, "SERVER_UNAVAILABLE",
            f"Service unavailable (simulated outage, {remaining}s remaining)",
            {"Retry-After": str(min(remaining, 60))},
        ), STATE_STORE.record_api_usage(path))

    # 2. rolling rate limit -> 429 with Retry-After
    _request_times[:] = [t for t in _request_times if now - t < 60]
    if len(_request_times) >= chaos["rate_limit_per_min"]:
        oldest = min(_request_times)
        retry_after = max(1, int(60 - (now - oldest)))
        return _with_limit_info(sf_error(
            429, "REQUEST_LIMIT_EXCEEDED",
            "TotalRequests Limit exceeded.",
            {"Retry-After": str(retry_after)},
        ), STATE_STORE.record_api_usage(path))
    _request_times.append(now)
    usage = STATE_STORE.record_api_usage(path)

    # 3. latency and hangs
    if chaos["latency_ms"]:
        await asyncio.sleep(chaos["latency_ms"] / 1000.0)
    if random.random() < chaos["hang_rate"]:
        await asyncio.sleep(30)

    # 4. transient failure (retryable)
    if random.random() < chaos["error_rate"]:
        return _with_limit_info(sf_error(
            503, "SERVER_UNAVAILABLE",
            "Server error, please try again later",
            {"Retry-After": "5"},
        ), usage)

    # 5. permanent failure (must NOT be retried)
    if random.random() < chaos["bad_request_rate"]:
        return _with_limit_info(
            sf_error(400, "MALFORMED_QUERY", "unexpected token: simulated permanent failure"), usage
        )

    return _with_limit_info(await call_next(request), usage)


@app.middleware("http")
async def admission_middleware(request: Request, call_next):
    operation = classify_operation(request.url.path, API_VERSION)
    async with RUNTIME.slot(operation):
        return await call_next(request)


# --------------------------------------------------------------------------
# OAuth
# --------------------------------------------------------------------------

@app.post("/services/oauth2/token")
async def token(request: Request):
    # Salesforce expects application/x-www-form-urlencoded. Parsed by hand so
    # that python-multipart is not a required dependency; JSON is accepted too.
    raw = (await request.body()).decode("utf-8") or ""
    if raw.lstrip().startswith("{"):
        form = json.loads(raw)
    else:
        form = {k: v[0] for k, v in parse_qs(raw).items()}
    if not form.get("client_id") or not form.get("client_secret"):
        return sf_error(400, "invalid_client", "client identifier invalid")
    tok = secrets.token_urlsafe(32)
    ttl = int(os.getenv("FAKEFORCE_TOKEN_TTL", 3600))
    _tokens[tok] = time.time() + ttl
    return {
        "access_token": tok,
        "instance_url": str(request.base_url).rstrip("/"),
        "token_type": "Bearer",
        "issued_at": str(int(time.time() * 1000)),
        "expires_in": ttl,
    }


@app.get(f"/services/data/{API_VERSION}/query")
def query(
    q: str,
    authorization: str | None = Header(default=None),
    sforce_query_options: str | None = Header(default=None),
):
    """Excludes soft-deleted records, exactly as the real /query does."""
    return _run_query(q, authorization, include_deleted=False, query_options=sforce_query_options)


@app.get(f"/services/data/{API_VERSION}/queryAll")
def query_all(
    q: str,
    authorization: str | None = Header(default=None),
    sforce_query_options: str | None = Header(default=None),
):
    """Includes records in the recycle bin. IsDeleted is only meaningful here."""
    return _run_query(q, authorization, include_deleted=True, query_options=sforce_query_options)


def _run_query(
    q: str, authorization: str | None, include_deleted: bool, query_options: str | None
):
    require_token(authorization)
    try:
        requested_size = parse_query_batch_size(query_options)
        return _page(
            q, include_deleted, page_offset=0, page_size=min(chaos["page_size"], requested_size)
        )
    except QueryOptionError as error:
        return sf_error(400, "MALFORMED_QUERY", str(error))
    except QueryDeadlineExceeded:
        return sf_error(500, "QUERY_TIMEOUT", "query timeout")
    except QueryValidationError as error:
        return sf_error(400, error.code, str(error))


def _page(q: str, include_deleted: bool, page_offset: int, page_size: int | None = None) -> dict:
    size = chaos["page_size"] if page_size is None else page_size
    locator = secrets.token_hex(8)
    page, artifact = QUERY_SERVICE.fetch_page_with_cursor_index(
        q, include_deleted, size, page_offset, locator, CURSOR_ARTIFACTS
    )
    body = {
        "totalSize": page.total_size,
        "done": page_offset + len(page.records) >= page.total_size,
        "records": page.records,
    }
    if not body["done"]:
        assert artifact is not None
        STATE_STORE.create_cursor(
            locator_id=locator,
            api_version=f"{API_VERSION}:{'queryAll' if include_deleted else 'query'}",
            normalized_query=q,
            query_hash=hashlib.sha256(q.encode()).hexdigest(),
            object_name=page.object_name,
            dataset_snapshot_id=CATALOG.snapshot_id,
            batch_size=size,
            page_offset=0,
            total_size=page.total_size,
            result_artifact=str(artifact),
            expires_at=datetime.now(timezone.utc) + timedelta(days=2),
        )
        body["nextRecordsUrl"] = (
            f"/services/data/{API_VERSION}/query/{locator}-{page_offset + len(page.records)}"
        )
    return body


@app.get(f"/services/data/{API_VERSION}/query/{{locator}}")
def query_more(locator: str, authorization: str | None = Header(default=None)):
    require_token(authorization)
    locator_id, separator, offset_text = locator.rpartition("-")
    if not separator or not offset_text.isdecimal():
        return sf_error(400, "INVALID_QUERY_LOCATOR", "invalid query locator")
    cur = STATE_STORE.get_cursor(locator_id)
    if cur is None:
        return sf_error(400, "INVALID_QUERY_LOCATOR", "invalid query locator")
    if cur.dataset_snapshot_id != CATALOG.snapshot_id or not Path(cur.result_artifact).is_file():
        return sf_error(400, "INVALID_QUERY_LOCATOR", "invalid query locator")
    page_offset = int(offset_text)
    if page_offset < 0 or page_offset >= cur.total_size:
        return sf_error(400, "INVALID_QUERY_LOCATOR", "invalid query locator")
    try:
        with ENGINE.connection() as connection:
            record_ids = CURSOR_ARTIFACTS.read_page_ids(
                connection, Path(cur.result_artifact), page_offset, cur.batch_size
            )
        records = QUERY_SERVICE.fetch_records_by_ids(
            cur.normalized_query,
            cur.api_version == f"{API_VERSION}:queryAll",
            record_ids,
        )
        body = {
            "totalSize": cur.total_size,
            "done": page_offset + len(records) >= cur.total_size,
            "records": records,
        }
        if not body["done"]:
            body["nextRecordsUrl"] = (
                f"/services/data/{API_VERSION}/query/{locator_id}-{page_offset + len(records)}"
            )
        return body
    except QueryValidationError as error:
        return sf_error(400, error.code, str(error))
    except QueryDeadlineExceeded:
        return sf_error(500, "QUERY_TIMEOUT", "query timeout")


@app.get(f"/services/data/{API_VERSION}/sobjects/{{obj}}/describe")
def describe(obj: str, authorization: str | None = Header(default=None)):
    require_token(authorization)
    spec = CATALOG.get(obj)
    if spec is None:
        return sf_error(404, "NOT_FOUND", f"The requested resource does not exist: {obj}")

    def sf_type(name: str) -> str:
        if name == "Id" or name.endswith("Id"):
            return "id"
        t = spec.schema.field(name).type
        if pa.types.is_boolean(t):
            return "boolean"
        if pa.types.is_integer(t):
            return "int"
        if pa.types.is_floating(t):
            return "double"
        if pa.types.is_date(t) or pa.types.is_timestamp(t):
            return "datetime"
        return "string"

    fields = [{"name": c, "type": sf_type(c), "nillable": c != spec.id_field,
               "custom": c.endswith("__c")} for c in spec.schema.names]
    if chaos.get("drift_after") and obj == "Account":
        fields.append({"name": "Territory__c", "type": "string", "nillable": True, "custom": True})
    return {"name": obj, "label": obj, "queryable": True, "fields": fields}


@app.get(f"/services/data/{API_VERSION}/sobjects/{{obj}}/deleted")
def get_deleted(obj: str, start: str = "", end: str = "",
                authorization: str | None = Header(default=None)):
    """Mirrors Salesforce getDeleted(): the ids removed in a window."""
    require_token(authorization)
    spec = CATALOG.get(obj)
    if spec is None:
        return sf_error(404, "NOT_FOUND", f"The requested resource does not exist: {obj}")
    if spec.soft_delete_field is None:
        records = []
    else:
        date_field = "LastModifiedDate" if "LastModifiedDate" in spec.schema.names else spec.id_field
        from fakeforce.engine import _quote_identifier
        with ENGINE.connection() as conn:
            rows = conn.execute(
                f"SELECT {_quote_identifier(spec.id_field)}, {_quote_identifier(date_field)} "
                f"FROM {_quote_identifier(ENGINE.relation_name(obj))} "
                f"WHERE {_quote_identifier(spec.soft_delete_field)}"
            ).fetchall()
        records = [{"id": row[0], "deletedDate": row[1]} for row in rows]
    return {
        "deletedRecords": records,
        "earliestDateAvailable": start or None,
        "latestDateCovered": end or None,
    }


@app.get(f"/services/data/{API_VERSION}/limits")
def limits(authorization: str | None = Header(default=None)):
    require_token(authorization)
    cap = chaos["daily_api_limit"]
    usage = STATE_STORE.api_usage_last_24_hours()
    return {
        "DailyApiRequests": {"Max": cap, "Remaining": max(0, cap - usage)},
        "DataStorageMB": {"Max": 1024, "Remaining": 1019},
    }


@app.post(f"/services/data/{API_VERSION}/jobs/query")
async def create_bulk_query_job(request: Request, authorization: str | None = Header(default=None)):
    require_token(authorization)
    body = await request.json()
    if body.get("operation", "query").lower() != "query" or not isinstance(body.get("query"), str):
        return sf_error(400, "MALFORMED_QUERY", "Bulk query jobs require operation=query and a query")
    try:
        QUERY_SERVICE.plan(body["query"], include_deleted=False)
    except QueryValidationError as error:
        return sf_error(400, error.code, str(error))
    job = BulkJob(
        job_id=f"750{secrets.token_hex(8)}",
        job_type=BulkJobType.QUERY,
        api_version=API_VERSION,
        state=BulkJobState.UPLOAD_COMPLETE,
        object_name=None,
        operation="query",
        query_text=body["query"],
        dataset_snapshot_id=CATALOG.snapshot_id,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=SETTINGS.bulk_job_retention_hours),
    )
    STATE_STORE.create_job(job)
    BULK_QUERY_DISPATCHER.submit(job.job_id)
    return _bulk_query_job_response(job)


@app.post(f"/services/data/{API_VERSION}/jobs/ingest")
async def create_bulk_ingest_job(request: Request, authorization: str | None = Header(default=None)):
    require_token(authorization)
    body = await request.json()
    try:
        config = validate_ingest_create(body, CATALOG)
    except IngestValidationError as error:
        return sf_error(400 if error.code != "NOT_FOUND" else 404, error.code, str(error))
    job = BulkJob(
        job_id=f"750{secrets.token_hex(8)}",
        job_type=BulkJobType.INGEST,
        api_version=API_VERSION,
        state=BulkJobState.OPEN,
        object_name=config.object_name,
        operation=config.operation,
        query_text=None,
        dataset_snapshot_id=CATALOG.snapshot_id,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=SETTINGS.bulk_job_retention_hours),
        options={
            "contentType": config.content_type,
            "lineEnding": config.line_ending,
            "columnDelimiter": config.column_delimiter,
        },
    )
    STATE_STORE.create_job(job)
    return _bulk_ingest_job_response(job)


@app.put(f"/services/data/{API_VERSION}/jobs/ingest/{{job_id}}/batches")
async def upload_bulk_ingest_batch(
    job_id: str, request: Request, authorization: str | None = Header(default=None)
):
    require_token(authorization)
    job = STATE_STORE.get_job(job_id)
    if job is None or job.job_type != BulkJobType.INGEST:
        return sf_error(404, "NOT_FOUND", "The requested resource does not exist")
    if job.state != BulkJobState.OPEN:
        return sf_error(400, "INVALIDJOB", "CSV data can be uploaded only while the job is open")
    try:
        await BULK_INGEST_UPLOADS.write(
            job_id, request.stream(), SETTINGS.bulk_ingest_max_upload_bytes
        )
    except IngestUploadTooLarge as error:
        return sf_error(413, "REQUEST_ENTITY_TOO_LARGE", str(error))
    return JSONResponse(status_code=201, content={})


@app.patch(f"/services/data/{API_VERSION}/jobs/ingest/{{job_id}}")
async def update_bulk_ingest_job(
    job_id: str, request: Request, authorization: str | None = Header(default=None)
):
    require_token(authorization)
    body = await request.json()
    requested_state = body.get("state")
    job = STATE_STORE.get_job(job_id)
    if job is None or job.job_type != BulkJobType.INGEST:
        return sf_error(404, "NOT_FOUND", "The requested resource does not exist")
    if requested_state == BulkJobState.UPLOAD_COMPLETE.value:
        if not BULK_INGEST_UPLOADS.path_for(job_id).is_file():
            return sf_error(400, "INVALIDJOB", "CSV data must be uploaded before closing the job")
    elif requested_state != BulkJobState.ABORTED.value:
        return sf_error(400, "INVALIDJOB", "invalid Bulk ingest job state transition")
    try:
        job = STATE_STORE.transition_job(job_id, BulkJobState(requested_state))
    except InvalidJobTransition:
        return sf_error(400, "INVALIDJOB", "invalid Bulk ingest job state transition")
    if job.state == BulkJobState.UPLOAD_COMPLETE:
        BULK_INGEST_DISPATCHER.submit(job.job_id)
    return _bulk_ingest_job_response(job)


@app.get(f"/services/data/{API_VERSION}/jobs/ingest/{{job_id}}")
def get_bulk_ingest_job(job_id: str, authorization: str | None = Header(default=None)):
    require_token(authorization)
    job = STATE_STORE.get_job(job_id)
    if job is None or job.job_type != BulkJobType.INGEST:
        return sf_error(404, "NOT_FOUND", "The requested resource does not exist")
    return _bulk_ingest_job_response(job)


def _bulk_ingest_job_response(job: BulkJob) -> dict[str, str | None]:
    return {
        "id": job.job_id,
        "operation": job.operation,
        "object": job.object_name,
        "state": job.state.value,
        "contentType": job.options.get("contentType"),
        "lineEnding": job.options.get("lineEnding"),
        "columnDelimiter": job.options.get("columnDelimiter"),
        "errorCode": job.error_code,
        "errorMessage": job.error_message,
    }


@app.get(f"/services/data/{API_VERSION}/jobs/ingest/{{job_id}}/successfulResults")
def get_bulk_ingest_successful_results(
    job_id: str, authorization: str | None = Header(default=None)
):
    return _bulk_ingest_results(job_id, "success", authorization)


@app.get(f"/services/data/{API_VERSION}/jobs/ingest/{{job_id}}/failedResults")
def get_bulk_ingest_failed_results(
    job_id: str, authorization: str | None = Header(default=None)
):
    return _bulk_ingest_results(job_id, "failed", authorization)


def _bulk_ingest_results(job_id: str, status: str, authorization: str | None):
    require_token(authorization)
    job = STATE_STORE.get_job(job_id)
    if job is None or job.job_type != BulkJobType.INGEST:
        return sf_error(404, "NOT_FOUND", "The requested resource does not exist")
    if job.state not in {BulkJobState.JOB_COMPLETE, BulkJobState.FAILED}:
        return sf_error(400, "INVALIDJOB", "Bulk ingest results are not available until processing ends")
    path = BULK_INGEST_UPLOADS.path_for(job_id)
    try:
        with path.open("r", encoding="utf-8", newline="") as stream:
            source_fields = next(csv.reader(stream))
    except (FileNotFoundError, StopIteration):
        return sf_error(400, "INVALIDJOB", "Bulk ingest CSV upload is unavailable")
    return StreamingResponse(
        STATE_STORE.stream_ingest_results(job_id, status, source_fields),
        media_type="text/csv",
        headers={"Sforce-NumberOfRecords": str(STATE_STORE.ingest_result_count(job_id, status))},
    )


@app.get(f"/services/data/{API_VERSION}/jobs/query/{{job_id}}")
def get_bulk_query_job(job_id: str, authorization: str | None = Header(default=None)):
    require_token(authorization)
    job = STATE_STORE.get_job(job_id)
    if job is None or job.job_type != BulkJobType.QUERY:
        return sf_error(404, "NOT_FOUND", "The requested resource does not exist")
    return _bulk_query_job_response(job)


@app.patch(f"/services/data/{API_VERSION}/jobs/query/{{job_id}}")
async def update_bulk_query_job(
    job_id: str, request: Request, authorization: str | None = Header(default=None)
):
    require_token(authorization)
    body = await request.json()
    if body.get("state") != BulkJobState.ABORTED.value:
        return sf_error(400, "INVALIDJOB", "Bulk query jobs can only be aborted through this resource")
    try:
        job = STATE_STORE.transition_job(job_id, BulkJobState.ABORTED)
    except KeyError:
        return sf_error(404, "NOT_FOUND", "The requested resource does not exist")
    except InvalidJobTransition:
        return sf_error(400, "INVALIDJOB", "invalid Bulk job state transition")
    return _bulk_query_job_response(job)


def _bulk_query_job_response(job: BulkJob) -> dict[str, str | None]:
    return {
        "id": job.job_id,
        "operation": job.operation,
        "state": job.state.value,
        "query": job.query_text,
        "errorCode": job.error_code,
        "errorMessage": job.error_message,
    }


@app.get(f"/services/data/{API_VERSION}/jobs/query/{{job_id}}/results")
def get_bulk_query_results(
    job_id: str, locator: str | None = None, maxRecords: int | None = None,
    authorization: str | None = Header(default=None)
):
    require_token(authorization)
    job = STATE_STORE.get_job(job_id)
    if job is None or job.job_type != BulkJobType.QUERY:
        return sf_error(404, "NOT_FOUND", "The requested resource does not exist")
    if job.api_version != API_VERSION:
        return sf_error(409, "API_VERSION_MISMATCH", "Bulk job API version does not match request")
    if job.state != BulkJobState.JOB_COMPLETE:
        return sf_error(400, "INVALIDJOB", "Bulk query results are not available until the job is complete")
    try:
        page = (BULK_QUERY_RESULTS.window(job_id, locator, maxRecords)
                if maxRecords is not None else BULK_QUERY_RESULTS.page(job_id, locator))
    except (InvalidBulkLocator, FileNotFoundError):
        return sf_error(400, "INVALIDLOCATOR", "invalid bulk result locator")
    headers = {"Sforce-NumberOfRecords": str(page.record_count)}
    if page.next_locator is not None:
        headers["Sforce-Locator"] = page.next_locator
    stream = (BULK_QUERY_RESULTS.stream_window(job_id, locator, page.record_count)
              if maxRecords is not None else BULK_QUERY_RESULTS.stream(page.path))
    return StreamingResponse(stream, media_type="text/csv", headers=headers)


# --------------------------------------------------------------------------
# Chaos admin (not part of the Salesforce surface)
# --------------------------------------------------------------------------

@app.get("/_chaos")
def get_chaos():
    now = time.time()
    return {
        **{k: v for k, v in chaos.items() if k != "outage_until"},
        "outage_seconds_remaining": max(0, int(chaos["outage_until"] - now)),
        "requests_last_60s": len([t for t in _request_times if now - t < 60]),
        "api_calls_last_24_hours": STATE_STORE.api_usage_last_24_hours(),
    }


@app.post("/_chaos")
async def set_chaos(request: Request):
    body = await request.json()
    unknown = set(body) - set(DEFAULTS)
    if unknown:
        raise HTTPException(400, f"unknown knobs: {sorted(unknown)}")
    chaos.update(body)
    return get_chaos()


@app.post("/_chaos/outage")
async def outage(request: Request):
    body = await request.json() if await request.body() else {}
    chaos["outage_until"] = time.time() + int(body.get("seconds", 60))
    return get_chaos()


@app.post("/_chaos/reset")
def reset_chaos():
    chaos.update(DEFAULTS)
    chaos["outage_until"] = 0.0
    _request_times.clear()
    return get_chaos()


@app.get("/")
def index():
    return {
        "service": "FakeForce",
        "api_version": API_VERSION,
        "objects": list(CATALOG.object_names),
        "endpoints": [
            f"POST /services/oauth2/token",
            f"GET  /services/data/{API_VERSION}/query?q=<SOQL>",
            f"GET  /services/data/{API_VERSION}/queryAll?q=<SOQL>",
            f"GET  /services/data/{API_VERSION}/query/{{locator}}",
            f"GET  /services/data/{API_VERSION}/sobjects/{{obj}}/describe",
            f"GET  /services/data/{API_VERSION}/sobjects/{{obj}}/deleted",
            f"GET  /services/data/{API_VERSION}/limits",
            f"POST /services/data/{API_VERSION}/jobs/query",
            f"GET  /services/data/{API_VERSION}/jobs/query/{{id}}/results",
            f"POST /services/data/{API_VERSION}/jobs/ingest",
            "GET  /health", "GET|POST /_chaos", "GET /_docs",
        ],
    }


@app.get("/health")
def health():
    query_metrics = DIAGNOSTICS.queries()
    return {
        "status": "ok",
        "objects": list(CATALOG.object_names),
        "resident_mb": _rss_mb(),
        "open_cursors": query_metrics["open_cursors"],
        "data_plane": "lazy_duckdb",
        "active_sync_queries": query_metrics["active_sync_queries"],
        "queued_sync_queries": query_metrics["queued_sync_queries"],
        "server_time": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/_diagnostics/memory")
def diagnostics_memory():
    return DIAGNOSTICS.memory()


@app.get("/_diagnostics")
def diagnostics_overview():
    """Internal operator snapshot; not part of the Salesforce API surface."""
    return DIAGNOSTICS.overview(
        api_calls_last_24_hours=STATE_STORE.api_usage_last_24_hours()
    )


@app.get("/_diagnostics/queries")
def diagnostics_queries():
    return DIAGNOSTICS.queries()


@app.get("/_diagnostics/jobs")
def diagnostics_jobs():
    return DIAGNOSTICS.jobs()


def _rss_mb() -> int:
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        pass
    import resource
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss // (1024 * 1024) if sys.platform == "darwin" else rss // 1024


print(
    f"[fakeforce] lazy catalog: {', '.join(CATALOG.object_names)} | rss {_rss_mb()} MB",
    flush=True,
)
