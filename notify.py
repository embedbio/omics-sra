"""Email notification (via an SNS topic) when a new batch is written."""

import io
import json
import os
from urllib.parse import quote

import polars as pl

from batches import BatchResult, S3Store, Store, batch_key
from wgs_runs import WgsRuns

MAX_RUN_IDS = 20
MAX_STUDIES = 10
SNS_SUBJECT_LIMIT = 100


def subject(result: BatchResult) -> str:
    runs = "run" if result.new == 1 else "runs"
    text = f"omics-sra: {result.new:,} new WGS {runs} in batch {result.batch_id}"
    return text[:SNS_SUBJECT_LIMIT]


def console_url(store: Store, key: str) -> str | None:
    if not isinstance(store, S3Store):
        return None
    region = os.environ.get("AWS_REGION", "us-west-1")
    prefix = quote(store._key(key), safe="/")
    return f"https://s3.console.aws.amazon.com/s3/buckets/{store.bucket}?region={region}&prefix={prefix}"


def message(result: BatchResult, store: Store) -> str:
    manifest = json.loads(store.read_bytes(batch_key(result.batch_id, "manifest.json")))
    runs = pl.read_parquet(io.BytesIO(store.read_bytes(batch_key(result.batch_id, "runs.parquet"))))
    params = manifest["parameters"]
    filters = ", ".join(f"{k}={'ALL' if v is None else ','.join(v) if isinstance(v, list) else v}"
                        for k, v in params.items() if k != "OUTPUT_URI")

    lines = [
        f"A new batch of SRA WGS runs was written from OmicIDX release {result.batch_id} "
        f"(published {result.publish_date}).",
        "",
        f"  New runs in this batch:      {result.new:,}",
        f"  Runs matching the filters:   {result.matched:,}",
        f"  Already in earlier batches:  {result.previously_seen:,}",
        f"  Total runs output so far:    {manifest['total_seen']:,}",
        "",
        f"Filters: {filters}",
    ]

    if runs.is_empty():
        lines += ["", "No new runs this time; the batch files are empty."]
    else:
        studies = (
            runs.group_by(WgsRuns.study_accession, WgsRuns.study_title)
            .agg(pl.len().alias("runs"), pl.col(WgsRuns.total_gb).sum().alias("gb"))
            .sort("runs", descending=True)
        )
        lines += ["", f"Studies ({studies.height:,}):"]
        for acc, title, n, gb in studies.head(MAX_STUDIES).iter_rows():
            title = (title or "").strip()
            title = title if len(title) <= 70 else title[:67] + "..."
            lines.append(f"  {n:>5,} runs  {gb:>8,.1f} Gb  {acc or '(no study)'}  {title}")
        if studies.height > MAX_STUDIES:
            lines.append(f"  ... and {studies.height - MAX_STUDIES:,} more")

        ids = runs[WgsRuns.run_accession].to_list()
        shown = ", ".join(ids[:MAX_RUN_IDS])
        more = f" ... (+{len(ids) - MAX_RUN_IDS:,} more)" if len(ids) > MAX_RUN_IDS else ""
        lines += ["", f"Runs: {shown}{more}"]

    lines += [
        "",
        "Files:",
        f"  {store.uri(batch_key(result.batch_id, 'runs.parquet'))}",
        f"  {store.uri(batch_key(result.batch_id, 'run_ids.txt'))}",
    ]
    if url := console_url(store, batch_key(result.batch_id, "")):
        lines.append(f"  S3 console: {url}")
        lines += ["", f"Download the run IDs:  aws s3 cp {store.uri(batch_key(result.batch_id, 'run_ids.txt'))} ."]
    return "\n".join(lines) + "\n"


def publish(topic_arn: str, result: BatchResult, store: Store, client=None) -> str:
    """Publish the batch notice to SNS; returns the message ID."""
    if client is None:
        import boto3

        client = boto3.client("sns")
    resp = client.publish(TopicArn=topic_arn, Subject=subject(result), Message=message(result, store))
    return resp["MessageId"]
