"""Bounded synchronous SOQL-lite execution over the lazy DuckDB data plane.

The grammar remains intentionally compatible with the existing public surface.
FF-006 will replace this parser with a full AST, but execution already avoids
long-lived Arrow tables and full-result NumPy arrays.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fakeforce.catalog import DatasetCatalog
from fakeforce.cursor_artifacts import CursorArtifactStore
from fakeforce.deadline import QueryDeadlineExceeded, run_with_deadline
from fakeforce.engine import DuckDBEngine, _quote_identifier
from fakeforce.soql import (
    Comparison,
    InComparison,
    Predicate,
    SelectQuery,
    SoqlSyntaxError,
    parse_soql,
)

MAX_OFFSET = 2000
DEFAULT_QUERY_BATCH_SIZE = 2000
MIN_QUERY_BATCH_SIZE = 200
MAX_QUERY_BATCH_SIZE = 2000


class QueryValidationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


class QueryOptionError(ValueError):
    """The Sforce-Query-Options header is outside the supported contract."""


@dataclass(frozen=True)
class QueryPlan:
    object_name: str
    fields: tuple[str, ...]
    id_field: str
    source_sql: str
    sql: str
    parameters: tuple[Any, ...]


@dataclass(frozen=True)
class QueryPage:
    object_name: str
    total_size: int
    records: list[dict[str, Any]]


_INTERNAL_ID_FIELD = "__fakeforce_record_id"


def parse_query_batch_size(header: str | None) -> int:
    """Parse Salesforce's supported `Sforce-Query-Options: batchSize=n`."""
    if header is None or not header.strip():
        return DEFAULT_QUERY_BATCH_SIZE
    options: dict[str, str] = {}
    for part in header.split(";"):
        key, separator, value = part.partition("=")
        if not separator or not key.strip() or not value.strip():
            raise QueryOptionError("Sforce-Query-Options must use batchSize=<integer>")
        options[key.strip().lower()] = value.strip()
    if set(options) != {"batchsize"}:
        raise QueryOptionError("Sforce-Query-Options only supports batchSize")
    try:
        batch_size = int(options["batchsize"])
    except ValueError as error:
        raise QueryOptionError("batchSize must be an integer") from error
    if not MIN_QUERY_BATCH_SIZE <= batch_size <= MAX_QUERY_BATCH_SIZE:
        raise QueryOptionError(
            f"batchSize must be between {MIN_QUERY_BATCH_SIZE} and {MAX_QUERY_BATCH_SIZE}"
        )
    return batch_size


class LazyQueryService:
    def __init__(self, catalog: DatasetCatalog, engine: DuckDBEngine, api_version: str) -> None:
        self.catalog = catalog
        self.engine = engine
        self.api_version = api_version

    def plan(self, query: str, include_deleted: bool) -> QueryPlan:
        try:
            ast = parse_soql(query)
        except SoqlSyntaxError as error:
            raise QueryValidationError("MALFORMED_QUERY", str(error)) from error
        object_name = ast.object_name
        spec = self.catalog.get(object_name)
        if spec is None:
            raise QueryValidationError("INVALID_TYPE", f"sObject type '{object_name}' is not supported")

        fields = self._fields(ast, spec.schema.names)
        clauses: list[str] = []
        parameters: list[Any] = []
        if not include_deleted and spec.soft_delete_field is not None:
            clauses.append(f"NOT {_quote_identifier(spec.soft_delete_field)}")
        if ast.predicates:
            where_sql, where_parameters = self._where(ast.predicates, spec.schema.names)
            clauses.extend(where_sql)
            parameters.extend(where_parameters)

        public_projection = "*" if fields == ("*",) else ", ".join(
            _quote_identifier(field) for field in fields
        )
        projection = (
            f"{public_projection}, {_quote_identifier(spec.id_field)} "
            f"AS {_quote_identifier(_INTERNAL_ID_FIELD)}"
        )
        relation = _quote_identifier(self.engine.relation_name(object_name))
        source_sql = f"SELECT {projection} FROM {relation}"
        if clauses:
            source_sql += " WHERE " + " AND ".join(f"({clause})" for clause in clauses)
        if ast.order_by is not None:
            if ast.order_by.field not in spec.schema.names:
                raise QueryValidationError(
                    "INVALID_FIELD", f"No such column '{ast.order_by.field}' on entity '{object_name}'"
                )
            source_sql += (
                f" ORDER BY {_quote_identifier(ast.order_by.field)} {ast.order_by.direction} "
                f"NULLS {ast.order_by.nulls}"
            )

        sql = source_sql

        if ast.offset > MAX_OFFSET:
            raise QueryValidationError(
                "NUMBER_OUTSIDE_VALID_RANGE", f"maximum SOQL offset allowed is {MAX_OFFSET}"
            )
        if ast.limit is not None:
            sql += f" LIMIT {ast.limit}"
        if ast.offset:
            sql += f" OFFSET {ast.offset}"
        return QueryPlan(object_name, fields, spec.id_field, source_sql, sql, tuple(parameters))

    def fetch_page(
        self, query: str, include_deleted: bool, page_size: int, page_offset: int = 0
    ) -> QueryPage:
        if page_size <= 0 or page_offset < 0:
            raise ValueError("page size and page offset must be non-negative")
        return run_with_deadline(
            self.engine.connection,
            lambda connection: self._execute_page(
                connection, query, include_deleted, page_size, page_offset
            ),
            self.engine.settings.sync_query_timeout_seconds,
        )

    def fetch_page_with_cursor_index(
        self,
        query: str,
        include_deleted: bool,
        page_size: int,
        page_offset: int,
        locator_id: str,
        artifacts: CursorArtifactStore,
    ) -> tuple[QueryPage, Path | None]:
        if page_size <= 0 or page_offset < 0:
            raise ValueError("page size and page offset must be non-negative")
        return run_with_deadline(
            self.engine.connection,
            lambda connection: self._execute_page_with_cursor_index(
                connection,
                query,
                include_deleted,
                page_size,
                page_offset,
                locator_id,
                artifacts,
            ),
            self.engine.settings.sync_query_timeout_seconds,
        )

    def _execute_page(
        self,
        connection: Any,
        query: str,
        include_deleted: bool,
        page_size: int,
        page_offset: int,
    ) -> QueryPage:
        plan = self.plan(query, include_deleted)
        total_size = connection.execute(
            f"SELECT count(*) FROM ({plan.sql}) AS result", plan.parameters
        ).fetchone()[0]
        cursor = connection.execute(
            f"SELECT * FROM ({plan.sql}) AS result LIMIT ? OFFSET ?",
            [*plan.parameters, page_size, page_offset],
        )
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        records = self._records(plan, columns, rows)
        return QueryPage(plan.object_name, total_size, records)

    def _execute_page_with_cursor_index(
        self,
        connection: Any,
        query: str,
        include_deleted: bool,
        page_size: int,
        page_offset: int,
        locator_id: str,
        artifacts: CursorArtifactStore,
    ) -> tuple[QueryPage, Path | None]:
        plan = self.plan(query, include_deleted)
        total_size = connection.execute(
            f"SELECT count(*) FROM ({plan.sql}) AS result", plan.parameters
        ).fetchone()[0]
        cursor = connection.execute(
            f"SELECT * FROM ({plan.sql}) AS result LIMIT ? OFFSET ?",
            [*plan.parameters, page_size, page_offset],
        )
        columns = [column[0] for column in cursor.description]
        page = QueryPage(plan.object_name, total_size, self._records(plan, columns, cursor.fetchall()))
        if page_offset + len(page.records) >= total_size:
            return page, None
        artifact = artifacts.write_index(
            connection, locator_id, plan.sql, plan.parameters, _INTERNAL_ID_FIELD
        )
        return page, artifact

    def fetch_records_by_ids(
        self, query: str, include_deleted: bool, record_ids: list[str]
    ) -> list[dict[str, Any]]:
        if not record_ids:
            return []
        return run_with_deadline(
            self.engine.connection,
            lambda connection: self._execute_records_by_ids(
                connection, query, include_deleted, record_ids
            ),
            self.engine.settings.sync_query_timeout_seconds,
        )

    def build_cursor_index(
        self,
        query: str,
        include_deleted: bool,
        locator_id: str,
        artifacts: CursorArtifactStore,
    ) -> Path:
        return run_with_deadline(
            self.engine.connection,
            lambda connection: self._write_cursor_index(
                connection, query, include_deleted, locator_id, artifacts
            ),
            self.engine.settings.sync_query_timeout_seconds,
        )

    def _write_cursor_index(
        self,
        connection: Any,
        query: str,
        include_deleted: bool,
        locator_id: str,
        artifacts: CursorArtifactStore,
    ) -> Path:
        plan = self.plan(query, include_deleted)
        return artifacts.write_index(
            connection, locator_id, plan.sql, plan.parameters, _INTERNAL_ID_FIELD
        )

    def _execute_records_by_ids(
        self, connection: Any, query: str, include_deleted: bool, record_ids: list[str]
    ) -> list[dict[str, Any]]:
        plan = self.plan(query, include_deleted)
        placeholders = ", ".join("?" for _ in record_ids)
        cursor = connection.execute(
            f"SELECT * FROM ({plan.source_sql}) AS matching "
            f"WHERE {_quote_identifier(_INTERNAL_ID_FIELD)} IN ({placeholders})",
            [*plan.parameters, *record_ids],
        )
        columns = [column[0] for column in cursor.description]
        records_by_id = {
            record[_INTERNAL_ID_FIELD]: record
            for record in self._records(plan, columns, cursor.fetchall(), include_internal_id=True)
        }
        ordered_records = [
            records_by_id[record_id] for record_id in record_ids if record_id in records_by_id
        ]
        for record in ordered_records:
            record.pop(_INTERNAL_ID_FIELD)
        return ordered_records

    def _records(
        self,
        plan: QueryPlan,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        include_internal_id: bool = False,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in rows:
            record = dict(zip(columns, row, strict=True))
            record_id = record.pop(_INTERNAL_ID_FIELD)
            record = {
                "attributes": {
                    "type": plan.object_name,
                    "url": f"/services/data/{self.api_version}/sobjects/{plan.object_name}/{record_id}",
                },
                **record,
            }
            if include_internal_id:
                record[_INTERNAL_ID_FIELD] = record_id
            records.append(record)
        return records

    @staticmethod
    def _fields(ast: SelectQuery, schema_names: list[str]) -> tuple[str, ...]:
        if ast.fields == ("*",):
            return ("*",)
        fields = ast.fields
        unknown = [field for field in fields if field not in schema_names]
        if unknown:
            raise QueryValidationError("INVALID_FIELD", f"No such column '{unknown[0]}' on entity")
        return fields

    @staticmethod
    def _where(
        predicates: tuple[Predicate, ...], schema_names: list[str]
    ) -> tuple[list[str], list[Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        for predicate in predicates:
            if isinstance(predicate, InComparison):
                field, operator, value = predicate.field, "IN", predicate.values
            else:
                assert isinstance(predicate, Comparison)
                field, operator, value = predicate.field, predicate.operator, predicate.value
            if field not in schema_names:
                raise QueryValidationError("INVALID_FIELD", f"No such column '{field}' on entity")
            quoted_field = _quote_identifier(field)
            if operator == "IN":
                clauses.append(f"{quoted_field} IN ({', '.join('?' for _ in value)})")
                parameters.extend(value)
            elif value is None:
                if operator == "=":
                    clauses.append(f"{quoted_field} IS NULL")
                elif operator == "!=":
                    clauses.append(f"{quoted_field} IS NOT NULL")
                else:
                    raise QueryValidationError("MALFORMED_QUERY", "NULL only supports = or !=")
            else:
                clauses.append(f"{quoted_field} {operator} ?")
                parameters.append(value)
        return clauses, parameters
