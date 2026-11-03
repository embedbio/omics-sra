"""AWS Lambda entry point: write this week's batch of new WGS runs to S3.

Parameters come from the bundled config.yaml, then the OUTPUT_URI environment
variable (set by the CloudFormation stack), then any YAML-style keys in the
invocation event, e.g. {"MIN_COVERAGE": 10}. Later sources win.

If NOTIFY_TOPIC_ARN is set, a summary of each newly written batch is published
to that SNS topic (which emails its subscribers). Skipped runs send nothing.
"""

import dataclasses
import json
import logging
import os
from pathlib import Path

import yaml

import notify
from batches import run_batch, store_from_uri
from wgs_runs import Params

CONFIG = Path(__file__).with_name("config.yaml")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def build_params(event: dict | None) -> Params:
    config = yaml.safe_load(CONFIG.read_text()) or {}
    if env_uri := os.environ.get("OUTPUT_URI"):
        config["OUTPUT_URI"] = env_uri
    config |= event or {}
    params = Params.from_sources(config, {})
    if not params.output_uri:
        raise ValueError("OUTPUT_URI is not set (config.yaml, environment or event)")
    return params


def handler(event: dict | None, context: object = None) -> dict:
    params = build_params(event)
    logger.info("parameters: %s", json.dumps(dataclasses.asdict(params), default=list))
    store = store_from_uri(params.output_uri)
    result = run_batch(params, store)
    logger.info(result.summary())

    topic_arn = os.environ.get("NOTIFY_TOPIC_ARN")
    if topic_arn and not result.skipped:
        # The batch is already committed, so a failure here can't lose data. Re-raise
        # so the invocation counts as an error and the failure alarm fires.
        try:
            message_id = notify.publish(topic_arn, result, store)
        except Exception:
            logger.exception("batch %s was written, but the notification failed", result.batch_id)
            raise
        logger.info("notification sent (SNS message %s)", message_id)
    return dataclasses.asdict(result)
