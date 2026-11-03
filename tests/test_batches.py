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


# ---------------------------------------------------------------------------
# Batch notification (notify.py)
# ---------------------------------------------------------------------------

import notify  # noqa: E402


def test_notify_subject():
    r = b.BatchResult("v2026-11-10", "2026-11-10", False, 10, 1, 9, "s3://b/p/")
    assert notify.subject(r) == "omics-sra: 1 new WGS run in batch v2026-11-10"
    r2 = b.BatchResult("v2026-11-10", "2026-11-10", False, 1600, 1234, 366, "s3://b/p/")
    assert notify.subject(r2) == "omics-sra: 1,234 new WGS runs in batch v2026-11-10"
    assert len(notify.subject(r2)) <= notify.SNS_SUBJECT_LIMIT


def test_notify_message_lists_counts_studies_and_files(store):
    batch(store, "v2026-11-03", "SRR1")
    result = batch(store, "v2026-11-10", "SRR1", "SRR2", "SRR3")
    msg = notify.message(result, store)
    assert "release v2026-11-10 (published 2026-11-10)" in msg
    assert "New runs in this batch:      2" in msg
    assert "Already in earlier batches:  1" in msg
    assert "Total runs output so far:    3" in msg
    assert "TAXON_ID=9685" in msg and "LIBRARY_SOURCE=GENOMIC" in msg and "PLATFORM=ALL" in msg
    assert "OUTPUT_URI" not in msg
    assert "SRP0000001" in msg and "Cat genomes" in msg  # study from make_output_row
    assert "Runs: SRR2, SRR3" in msg
    assert "batches/v2026-11-10/run_ids.txt" in msg
    assert "S3 console" not in msg and "aws s3 cp" not in msg  # local store


def test_notify_message_empty_batch(store):
    batch(store, "v2026-11-03", "SRR1")
    msg = notify.message(batch(store, "v2026-11-10", "SRR1"), store)
    assert "New runs in this batch:      0" in msg
    assert "No new runs this time" in msg
    assert "Runs:" not in msg


def test_notify_message_truncates_long_run_list(store):
    ids = [f"SRR{i}" for i in range(notify.MAX_RUN_IDS + 5)]
    msg = notify.message(batch(store, "v2026-11-03", *ids), store)
    assert "(+5 more)" in msg
    assert f"SRR{notify.MAX_RUN_IDS + 4}" not in msg


def test_notify_message_s3_console_link(s3_store):
    msg = notify.message(batch(s3_store, "v2026-11-03", "SRR1"), s3_store)
    assert ("S3 console: https://s3.console.aws.amazon.com/s3/buckets/test-bucket?region=us-west-1"
            "&prefix=wgs_runs/taxon_id%3D9685/batches/v2026-11-03/") in msg
    assert "aws s3 cp s3://test-bucket/wgs_runs/taxon_id=9685/batches/v2026-11-03/run_ids.txt ." in msg


@pytest.fixture
def sns_topic(monkeypatch):
    """A moto SNS topic with an SQS queue subscribed, so published messages can be read back."""
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-1")
    with mock_aws():
        sns = boto3.client("sns", region_name="us-west-1")
        sqs = boto3.client("sqs", region_name="us-west-1")
        topic = sns.create_topic(Name="omics-sra-batches")["TopicArn"]
        queue = sqs.create_queue(QueueName="q")["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=queue_arn)

        def received() -> list[dict]:
            msgs = sqs.receive_message(QueueUrl=queue, MaxNumberOfMessages=10).get("Messages", [])
            return [json.loads(m["Body"]) for m in msgs]

        yield topic, received


def run_handler(monkeypatch, tmp_path, topic, release_name, *run_ids):
    monkeypatch.setenv("OUTPUT_URI", str(tmp_path / "out"))
    if topic:
        monkeypatch.setenv("NOTIFY_TOPIC_ARN", topic)
    else:
        monkeypatch.delenv("NOTIFY_TOPIC_ARN", raising=False)
    monkeypatch.setattr(lambda_handler, "run_batch",
                        lambda params, store: batch(store, release_name, *run_ids))
    return lambda_handler.handler({}, None)


def test_handler_emails_new_batch(monkeypatch, tmp_path, sns_topic):
    topic, received = sns_topic
    run_handler(monkeypatch, tmp_path, topic, "v2026-11-03", "SRR1", "SRR2")
    [msg] = received()
    assert msg["Subject"] == "omics-sra: 2 new WGS runs in batch v2026-11-03"
    assert "Runs: SRR1, SRR2" in msg["Message"]


def test_handler_does_not_email_skipped_batch(monkeypatch, tmp_path, sns_topic):
    topic, received = sns_topic
    run_handler(monkeypatch, tmp_path, topic, "v2026-11-03", "SRR1")
    received()  # drain the first notice
    result = run_handler(monkeypatch, tmp_path, topic, "v2026-11-03", "SRR1")
    assert result["skipped"]
    assert received() == []


def test_handler_without_topic_sends_nothing(monkeypatch, tmp_path):
    called = []
    monkeypatch.setattr(notify, "publish", lambda *a, **k: called.append(a))
    run_handler(monkeypatch, tmp_path, None, "v2026-11-03", "SRR1")
    assert called == []


def test_handler_notification_failure_raises_after_batch_is_saved(monkeypatch, tmp_path):
    def boom(*a, **k):
        raise RuntimeError("SNS down")

    monkeypatch.setattr(notify, "publish", boom)
    with pytest.raises(RuntimeError, match="SNS down"):
        run_handler(monkeypatch, tmp_path, "arn:aws:sns:us-west-1:123:t", "v2026-11-03", "SRR1")
    store = LocalStore(tmp_path / "out")
    assert store.exists(batch_key("v2026-11-03", "manifest.json"))  # batch committed anyway
    assert read_ids(store, "v2026-11-03") == ["SRR1"]
