"""Streaming access to durable Bulk Query result parts."""

from __future__ import annotations

import json
import csv
import io
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


class InvalidBulkLocator(ValueError):
    pass


@dataclass(frozen=True)
class BulkResultPage:
    path: Path
    record_count: int
    next_locator: str | None


class BulkQueryResultStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def page(self, job_id: str, locator: str | None) -> BulkResultPage:
        try:
            part_index = int(locator) if locator is not None else 0
        except ValueError as error:
            raise InvalidBulkLocator("invalid bulk result locator") from error
        if part_index < 0:
            raise InvalidBulkLocator("invalid bulk result locator")
        manifest_path = self.root / job_id / "manifest.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(job_id)
        parts = json.loads(manifest_path.read_text()).get("parts", [])
        if part_index >= len(parts):
            raise InvalidBulkLocator("invalid bulk result locator")
        part = parts[part_index]
        path = manifest_path.parent / part["path"]
        if not path.is_file():
            raise FileNotFoundError(path)
        next_locator = str(part_index + 1) if part_index + 1 < len(parts) else None
        return BulkResultPage(path, part["record_count"], next_locator)

    def window(self, job_id: str, locator: str | None, max_records: int) -> BulkResultPage:
        """Return a replayable row window across durable CSV parts."""
        if max_records <= 0:
            raise InvalidBulkLocator("maxRecords must be positive")
        try:
            offset = 0 if locator is None else int(locator.removeprefix("r:"))
        except ValueError as error:
            raise InvalidBulkLocator("invalid bulk result locator") from error
        if locator is not None and not locator.startswith("r:"):
            raise InvalidBulkLocator("invalid bulk result locator")
        manifest_path = self.root / job_id / "manifest.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(job_id)
        parts = json.loads(manifest_path.read_text()).get("parts", [])
        total = sum(int(part["record_count"]) for part in parts)
        if offset < 0 or offset >= total:
            raise InvalidBulkLocator("invalid bulk result locator")
        count = min(max_records, total - offset)
        next_locator = f"r:{offset + count}" if offset + count < total else None
        return BulkResultPage(manifest_path, count, next_locator)

    def stream_window(self, job_id: str, locator: str | None, count: int) -> Iterator[bytes]:
        """Stream exactly one CSV header and ``count`` rows without buffering them."""
        offset = 0 if locator is None else int(locator.removeprefix("r:"))
        manifest = json.loads((self.root / job_id / "manifest.json").read_text())
        skipped = emitted = 0
        wrote_header = False
        for part in manifest.get("parts", []):
            with (self.root / job_id / part["path"]).open(newline="") as raw:
                reader = csv.reader(raw)
                header = next(reader, None)
                if header and not wrote_header:
                    yield self._csv_row(header)
                    wrote_header = True
                for row in reader:
                    if skipped < offset:
                        skipped += 1
                        continue
                    if emitted >= count:
                        return
                    yield self._csv_row(row)
                    emitted += 1

    @staticmethod
    def _csv_row(row: list[str]) -> bytes:
        buffer = io.StringIO()
        csv.writer(buffer).writerow(row)
        return buffer.getvalue().encode()

    @staticmethod
    def stream(path: Path, chunk_size: int = 1 << 16) -> Iterator[bytes]:
        with path.open("rb") as stream:
            while chunk := stream.read(chunk_size):
                yield chunk

    def remove_job(self, job_id: str) -> None:
        """Delete a job-owned directory without allowing path traversal."""
        root = self.root.resolve()
        target = (root / job_id).resolve()
        if root not in target.parents:
            raise ValueError("bulk job artifact path escapes configured root")
        if target.is_dir():
            shutil.rmtree(target)
