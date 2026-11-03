"""AWS Lambda entry point: write this week's batch of new WGS runs to S3, per organism.

Parameters come from the bundled config.yaml (its ORGANISMS section lists the
organisms), then the OUTPUT_ROOT environment variable (set by the CloudFormation
stack; each organism writes to <OUTPUT_ROOT>/taxon_id=<id>/), then the invocation
event. Later sources win. The event can hold:

    {"ORGANISM": "dog"}          only these organisms (a name or a list)
    {"MIN_COVERAGE": 10}         any YAML key, applied to every organism
    {"OUTPUT_URI": "s3://..."}   a different root, e.g. for a test run

If NOTIFY_TOPIC_ARN is set, each organism's newly written batch is announced by a
separate SNS message (one email each). Skipped batches send nothing.

Organisms are processed one at a time. If one fails, the rest still run, and the
invocation then raises so the failure alarm fires.
"""

import dataclasses
import json
import logging
import os
from pathlib import Path

import yaml

import notify
from batches import run_batch, store_from_uri
from wgs_runs import Params, organism_params

CONFIG = Path(__file__).with_name("config.yaml")
SELECT_KEY = "ORGANISM"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def build_params(event: dict | None) -> list[Params]:
    config = yaml.safe_load(CONFIG.read_text()) or {}
    if env_root := os.environ.get("OUTPUT_ROOT"):
        config["OUTPUT_URI"] = env_root
    event = dict(event or {})
    only = event.pop(SELECT_KEY, None)
    if isinstance(only, str):
        only = [only]
    if event.get("OUTPUT_URI"):
        config["OUTPUT_URI"] = event.pop("OUTPUT_URI")  # a root, like OUTPUT_ROOT
    all_params = organism_params(config, Params.overrides_from_yaml(event), only=only)
    if not all(p.output_uri for p in all_params):
        raise ValueError("OUTPUT_ROOT is not set (environment, or OUTPUT_URI in config.yaml or the event)")
    return all_params


def run_one(params: Params, topic_arn: str | None) -> dict:
    logger.info("%s parameters: %s", params.label, json.dumps(dataclasses.asdict(params), default=list))
    store = store_from_uri(params.output_uri)
    result = run_batch(params, store)
    logger.info("%s: %s", params.label, result.summary())

    if topic_arn and not result.skipped:
        # The batch is already committed, so a failure here can't lose data.
        message_id = notify.publish(topic_arn, result, store)
        logger.info("%s: notification sent (SNS message %s)", params.label, message_id)
    return dataclasses.asdict(result)


def handler(event: dict | None, context: object = None) -> dict:
    topic_arn = os.environ.get("NOTIFY_TOPIC_ARN")
    results, errors = [], []
    for params in build_params(event):
        try:
            results.append(run_one(params, topic_arn))
        except Exception as e:
            logger.exception("%s failed", params.label)
            errors.append((params.label, e))

    if errors:
        # Re-raise so the invocation counts as an error and the failure alarm fires.
        names = ", ".join(label for label, _ in errors)
        raise RuntimeError(f"{len(errors)} of {len(errors) + len(results)} organism(s) failed: {names}") \
            from errors[0][1]
    return {"batches": results}
