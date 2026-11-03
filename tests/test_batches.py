"""Tests for incremental batches (batches.py) and the Lambda handler. No network."""

import io
import json
from datetime import UTC, datetime

import boto3
import polars as pl
import pytest
from moto import mock_aws

import batches as b
import lambda_handler
from batches import SEEN_KEY, LocalStore, S3Store, SeenRuns, batch_key, run_batch, store_from_uri
from test_schemas import make_output_row, output_frame
from wgs_runs import Params, Release, WgsRuns

PARAMS = Params(taxon_id=9685, genome_size=2.7e9, min_coverage=1.0)


def fake_query(*run_ids: str):
    """A query that returns these run accessions and records the params it was called with."""
    calls = []

    def query(params: Params) -> pl.DataFrame:
        calls.append(params)
        return output_frame(*[make_output_row(run_accession=r) for r in run_ids]) if run_ids else output_frame().head(0)

    query.calls = calls
    return query


def release(name: str):
    return lambda _requested: Release(name=name, publish_date=name.removeprefix("v"))


def batch(store, release_name, *run_ids, **kw):
    return run_batch(PARAMS, store, query=fake_query(*run_ids), resolve=release(release_name),
                     now=lambda: datetime(2026, 11, 10, tzinfo=UTC), **kw)


def read_runs(store, batch_id) -> list[str]:
    df = pl.read_parquet(io.BytesIO(store.read_bytes(batch_key(batch_id, "runs.parquet"))))
    return df[WgsRuns.run_accession].to_list()


def read_ids(store, batch_id) -> list[str]:
    return store.read_bytes(batch_key(batch_id, "run_ids.txt")).decode().split()


def read_seen(store) -> pl.DataFrame:
    return pl.read_parquet(io.BytesIO(store.read_bytes(SEEN_KEY)))


@pytest.fixture
def store(tmp_path):
    return LocalStore(tmp_path / "out")


# ---------------------------------------------------------------------------
# run_batch
# ---------------------------------------------------------------------------

def test_first_batch_outputs_all_runs(store):
    result = batch(store, "v2026-11-03", "SRR1", "SRR2", "SRR3")
    assert (result.new, result.matched, result.previously_seen, result.skipped) == (3, 3, 0, False)
    assert read_runs(store, "v2026-11-03") == ["SRR1", "SRR2", "SRR3"]
    assert read_ids(store, "v2026-11-03") == ["SRR1", "SRR2", "SRR3"]
    WgsRuns.validate(pl.read_parquet(io.BytesIO(store.read_bytes(batch_key("v2026-11-03", "runs.parquet")))))


def test_query_is_pinned_to_dated_release(store):
    query = fake_query("SRR1")
    run_batch(PARAMS, store, query=query, resolve=release("v2026-11-03"))
    assert query.calls[0].release == "v2026-11-03"


def test_next_release_outputs_only_new_runs(store):
    batch(store, "v2026-11-03", "SRR1", "SRR2")
    result = batch(store, "v2026-11-10", "SRR1", "SRR2", "SRR3", "ERR4")
    assert (result.new, result.matched, result.previously_seen) == (2, 4, 2)
    assert read_runs(store, "v2026-11-10") == ["SRR3", "ERR4"]
    seen = read_seen(store)
    assert dict(seen.iter_rows()) == {"SRR1": "v2026-11-03", "SRR2": "v2026-11-03",
                                      "SRR3": "v2026-11-10", "ERR4": "v2026-11-10"}


def test_same_release_twice_is_a_noop(store):
    batch(store, "v2026-11-03", "SRR1")
    before = store.read_bytes(batch_key("v2026-11-03", "manifest.json"))
    query = fake_query("SRR1", "SRR2")
    result = run_batch(PARAMS, store, query=query, resolve=release("v2026-11-03"))
    assert result.skipped and result.new == 1
    assert query.calls == []  # did not even query
    assert store.read_bytes(batch_key("v2026-11-03", "manifest.json")) == before


def test_zero_new_runs_writes_empty_batch_and_manifest(store):
    batch(store, "v2026-11-03", "SRR1")
    result = batch(store, "v2026-11-10", "SRR1")
    assert (result.new, result.previously_seen, result.skipped) == (0, 1, False)
    assert read_runs(store, "v2026-11-10") == []
    assert read_ids(store, "v2026-11-10") == []
    assert json.loads(store.read_bytes(batch_key("v2026-11-10", "manifest.json")))["new"] == 0
    assert batch(store, "v2026-11-10", "SRR1").skipped  # empty batch still counts as done


def test_runs_that_disappear_and_return_are_not_repeated(store):
    batch(store, "v2026-11-03", "SRR1", "SRR2")
    batch(store, "v2026-11-10", "SRR1")             # SRR2 temporarily filtered out
    result = batch(store, "v2026-11-17", "SRR1", "SRR2")
    assert result.new == 0


def test_manifest_contents(store):
    batch(store, "v2026-11-03", "SRR1", "SRR2")
    m = json.loads(store.read_bytes(batch_key("v2026-11-03", "manifest.json")))
    assert m["batch_id"] == m["release"] == "v2026-11-03"
    assert m["publish_date"] == "2026-11-03"
    assert m["created_at"] == "2026-11-10T00:00:00+00:00"
    assert m["parameters"]["TAXON_ID"] == 9685
    assert m["parameters"]["RELEASE"] == "v2026-11-03"
    assert m["parameters"]["LIBRARY_SOURCE"] == ["GENOMIC"]
    assert (m["matched"], m["new"], m["previously_seen"], m["total_seen"]) == (2, 2, 0, 2)


class FailingStore(LocalStore):
    """Raises when writing a given key, to simulate a crash part-way through."""

    def __init__(self, root, fail_on):
        super().__init__(root)
        object.__setattr__(self, "fail_on", fail_on)

    def write_bytes(self, key, data):
        if key.endswith(self.fail_on):
            raise RuntimeError(f"simulated crash writing {key}")
        super().write_bytes(key, data)


@pytest.mark.parametrize("fail_on", ["runs.parquet", "run_ids.txt", SEEN_KEY, "manifest.json"])
def test_crash_mid_batch_is_recovered_on_rerun(tmp_path, fail_on):
    root = tmp_path / "out"
    batch(LocalStore(root), "v2026-11-03", "SRR1")
    with pytest.raises(RuntimeError, match="simulated crash"):
        batch(FailingStore(root, fail_on), "v2026-11-10", "SRR1", "SRR2", "SRR3")

    store = LocalStore(root)
    result = batch(store, "v2026-11-10", "SRR1", "SRR2", "SRR3")
    assert not result.skipped
    assert read_runs(store, "v2026-11-10") == ["SRR2", "SRR3"]
    assert sorted(read_seen(store)[SeenRuns.run_accession]) == ["SRR1", "SRR2", "SRR3"]


def test_seen_runs_passes_schema(store):
    batch(store, "v2026-11-03", "SRR1", "SRR2")
    SeenRuns.validate(read_seen(store))


def test_corrupt_seen_runs_is_rejected(store):
    batch(store, "v2026-11-03", "SRR1")
    store.write_bytes(SEEN_KEY, b._parquet_bytes(pl.DataFrame({"run_accession": ["SRR1", "SRR1"],
                                                               "batch_id": ["a", "b"]})))
    with pytest.raises(Exception, match="unique|duplicate"):
        batch(store, "v2026-11-10", "SRR2")


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------

def test_store_from_uri():
    assert isinstance(store_from_uri("/tmp/x"), LocalStore)
    with mock_aws():
        s = store_from_uri("s3://my-bucket/a/b/")
        assert (s.bucket, s.prefix) == ("my-bucket", "a/b")
        assert s.uri("seen_runs.parquet") == "s3://my-bucket/a/b/seen_runs.parquet"
    with pytest.raises(ValueError):
        store_from_uri("s3:///nobucket")


@pytest.fixture
def s3_store(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-1")
    with mock_aws():
        client = boto3.client("s3", region_name="us-west-1")
        client.create_bucket(Bucket="test-bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-1"})
        yield S3Store("test-bucket", "wgs_runs/taxon_id=9685", client=client)


def test_s3_store_round_trip(s3_store):
    assert not s3_store.exists("a.txt")
    s3_store.write_bytes("a.txt", b"hello")
    assert s3_store.exists("a.txt")
    assert s3_store.read_bytes("a.txt") == b"hello"
    keys = [o["Key"] for o in s3_store.client.list_objects_v2(Bucket="test-bucket")["Contents"]]
    assert keys == ["wgs_runs/taxon_id=9685/a.txt"]


def test_batches_on_s3(s3_store):
    batch(s3_store, "v2026-11-03", "SRR1", "SRR2")
    result = batch(s3_store, "v2026-11-10", "SRR1", "SRR3")
    assert result.new == 1
    assert read_runs(s3_store, "v2026-11-10") == ["SRR3"]
    assert result.uri == "s3://test-bucket/wgs_runs/taxon_id=9685/batches/v2026-11-10/"


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def test_handler_params_from_env_and_event(monkeypatch, tmp_path):
    monkeypatch.setenv("OUTPUT_URI", str(tmp_path / "env"))
    p = lambda_handler.build_params({"MIN_COVERAGE": 10})
    assert p.output_uri == str(tmp_path / "env")
    assert p.min_coverage == 10.0
    assert p.taxon_id == 9685  # from the bundled config.yaml
    assert lambda_handler.build_params({"OUTPUT_URI": "s3://b/p"}).output_uri == "s3://b/p"


def test_handler_requires_output_uri(monkeypatch):
    monkeypatch.delenv("OUTPUT_URI", raising=False)
    with pytest.raises(ValueError, match="OUTPUT_URI"):
        lambda_handler.build_params({})


def test_handler_rejects_unknown_event_keys(monkeypatch, tmp_path):
    monkeypatch.setenv("OUTPUT_URI", str(tmp_path))
    with pytest.raises(ValueError, match="unknown parameter"):
        lambda_handler.build_params({"MIN_COVRAGE": 1})


def test_handler_runs_batch(monkeypatch, tmp_path):
    monkeypatch.setenv("OUTPUT_URI", str(tmp_path / "out"))
    monkeypatch.setattr(lambda_handler, "run_batch",
                        lambda params, store: batch(store, "v2026-11-03", "SRR1", "SRR2"))
    result = lambda_handler.handler({}, None)
    assert result["batch_id"] == "v2026-11-03" and result["new"] == 2
    assert read_ids(LocalStore(tmp_path / "out"), "v2026-11-03") == ["SRR1", "SRR2"]
