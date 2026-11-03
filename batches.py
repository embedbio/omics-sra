"""Incremental batches: each run writes only WGS runs that no earlier batch has output.

Layout under OUTPUT_URI (a local directory or s3://bucket/prefix):

    seen_runs.parquet                   every run_accession output so far, with its batch_id
    batches/<batch_id>/runs.parquet     full WgsRuns table for runs new in this batch
    batches/<batch_id>/run_ids.txt      one run accession per line
    batches/<batch_id>/manifest.json    parameters, release and counts; written LAST

batch_id is the dated OmicIDX release (e.g. v2026-11-10), so each batch is reproducible.

Crash safety: manifest.json is the commit marker. A batch whose manifest exists is
done and is skipped. When working out what is new, rows in seen_runs that belong to
the batch being written are ignored, so re-running after a crash at any point
reproduces exactly the same batch.
"""

import dataclasses
import io
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import pandera.polars as pa
import polars as pl

from wgs_runs import Params, Release, WgsRuns, find_wgs_runs, resolve_release

SEEN_KEY = "seen_runs.parquet"


def batch_key(batch_id: str, name: str) -> str:
    return f"batches/{batch_id}/{name}"


class SeenRuns(pa.DataFrameModel):
    """seen_runs.parquet: one row per run accession ever output."""

    run_accession: pl.String = pa.Field(unique=True)
    batch_id: pl.String

    class Config:
        strict = True


SEEN_SCHEMA = {SeenRuns.run_accession: pl.String, SeenRuns.batch_id: pl.String}


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

class Store(Protocol):
    def exists(self, key: str) -> bool: ...
    def read_bytes(self, key: str) -> bytes: ...
    def write_bytes(self, key: str, data: bytes) -> None: ...
    def uri(self, key: str = "") -> str: ...


@dataclasses.dataclass(frozen=True)
class LocalStore:
    root: Path

    def exists(self, key: str) -> bool:
        return (self.root / key).exists()

    def read_bytes(self, key: str) -> bytes:
        return (self.root / key).read_bytes()

    def write_bytes(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(path)  # atomic on the same filesystem

    def uri(self, key: str = "") -> str:
        return str(self.root / key)


@dataclasses.dataclass(frozen=True)
class S3Store:
    bucket: str
    prefix: str = ""
    client: object = None

    def __post_init__(self) -> None:
        if self.client is None:
            import boto3

            object.__setattr__(self, "client", boto3.client("s3"))
        object.__setattr__(self, "prefix", self.prefix.strip("/"))

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def read_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=self._key(key))["Body"].read()

    def write_bytes(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)  # S3 puts are atomic

    def uri(self, key: str = "") -> str:
        return f"s3://{self.bucket}/{self._key(key)}"


def store_from_uri(uri: str) -> Store:
    if uri.startswith("s3://"):
        bucket, _, prefix = uri.removeprefix("s3://").partition("/")
        if not bucket:
            raise ValueError(f"no bucket in {uri!r}")
        return S3Store(bucket, prefix)
    return LocalStore(Path(uri))


def _parquet_bytes(df: pl.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def load_seen(store: Store) -> pl.DataFrame:
    if not store.exists(SEEN_KEY):
        return pl.DataFrame(schema=SEEN_SCHEMA)
    return SeenRuns.validate(pl.read_parquet(io.BytesIO(store.read_bytes(SEEN_KEY))))


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class BatchResult:
    batch_id: str
    publish_date: str
    skipped: bool          # True if this release's batch already existed
    matched: int           # runs passing the filters in this release
    new: int               # runs written in this batch
    previously_seen: int   # runs already output by earlier batches
    uri: str
    taxon_id: int | None = None
    organism: str | None = None

    def summary(self) -> str:
        if self.skipped:
            return f"batch {self.batch_id} already exists at {self.uri}; nothing to do"
        return (f"batch {self.batch_id}: {self.new:,} new runs "
                f"({self.matched:,} matched, {self.previously_seen:,} already seen) -> {self.uri}")


def run_batch(
    params: Params,
    store: Store,
    query: Callable[[Params], pl.DataFrame] = find_wgs_runs,
    resolve: Callable[[str], Release] = resolve_release,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> BatchResult:
    release = resolve(params.release)
    batch_id = release.name
    manifest_key = batch_key(batch_id, "manifest.json")
    batch_uri = store.uri(batch_key(batch_id, ""))

    if store.exists(manifest_key):
        done = json.loads(store.read_bytes(manifest_key))
        return BatchResult(batch_id, release.publish_date, True, done["matched"], done["new"],
                           done["previously_seen"], batch_uri, params.taxon_id, params.organism)

    params = dataclasses.replace(params, release=batch_id)  # pin the query to the dated release
    seen = load_seen(store)
    # Ignore rows from an earlier, interrupted attempt at this same batch.
    seen_before = seen.filter(pl.col(SeenRuns.batch_id) != batch_id)

    matched = query(params)
    new = matched.join(seen_before, left_on=WgsRuns.run_accession, right_on=SeenRuns.run_accession, how="anti")
    new = WgsRuns.validate(new, lazy=True)

    # 1. batch data
    store.write_bytes(batch_key(batch_id, "runs.parquet"), _parquet_bytes(new))
    store.write_bytes(batch_key(batch_id, "run_ids.txt"),
                      "".join(f"{r}\n" for r in new[WgsRuns.run_accession]).encode())

    # 2. cumulative seen list
    added = new.select(pl.col(WgsRuns.run_accession).alias(SeenRuns.run_accession),
                       pl.lit(batch_id).alias(SeenRuns.batch_id))
    seen_after = SeenRuns.validate(pl.concat([seen_before, added]))
    store.write_bytes(SEEN_KEY, _parquet_bytes(seen_after))

    # 3. manifest last: marks the batch complete
    result = BatchResult(batch_id, release.publish_date, False, matched.height, new.height,
                         matched.height - new.height, batch_uri, params.taxon_id, params.organism)
    manifest = {
        "batch_id": batch_id,
        "release": batch_id,
        "publish_date": release.publish_date,
        "created_at": now().isoformat(),
        "parameters": {Params.yaml_key(k): list(v) if isinstance(v, tuple) else v
                       for k, v in dataclasses.asdict(params).items()},
        "matched": result.matched,
        "new": result.new,
        "previously_seen": result.previously_seen,
        "total_seen": seen_after.height,
        "files": ["runs.parquet", "run_ids.txt"],
    }
    store.write_bytes(manifest_key, json.dumps(manifest, indent=2).encode())
    return result
