# omics-sra

`wgs_runs.py` finds SRA whole genome sequencing (WGS) runs for one organism that have enough sequencing depth. It queries the [OmicIDX](https://omicidx.cancerdatasci.org) table `run_with_study.parquet` and writes out the runs whose estimated coverage meets a minimum you set.

It can also run in [batch mode](#weekly-batches), where each run writes only the runs that no earlier batch has output. On AWS this runs [once a week](#running-weekly-on-aws) and writes each batch to S3.

## What it does

1. **Reads** `https://omicidx.cancerdatasci.org/data/<RELEASE>/sradb/run_with_study.parquet`. Each row of this table is one SRA run, already joined to its experiment, sample and study. The file is scanned lazily, so polars downloads only the columns it needs instead of the whole table (about 46 million rows).
2. **Checks the input schema** before fetching any data. If OmicIDX renames a column or changes a type, the script stops with an error instead of producing wrong results.
3. **Filters** to runs where:
   - `taxon_id` equals `TAXON_ID`
   - `library_strategy` is `WGS`
   - `library_source` is one of `LIBRARY_SOURCE` (default `GENOMIC`)
   - `platform` is one of `PLATFORM` (default: any platform)

   Values are compared with surrounding whitespace removed, because a few SRA records have padded values.
4. **Estimates coverage** for each run and adds two columns:
   - `total_gb` = `total_bases / 1e9`
   - `est_coverage` = `total_bases / GENOME_SIZE`
5. **Keeps** runs with `est_coverage >= MIN_COVERAGE`. Runs with no `total_bases` are dropped.
6. **Checks the results** (unique run IDs in SRR/ERR/DRR format, non-negative bases and coverage, expected columns only), then writes them out.

`est_coverage` is a rough estimate of genome depth for a single run. It doesn't account for read quality, duplicates or mapping rate, and a sample sequenced over several runs is counted per run, not combined.

## Parameters

Parameters are read from `config.yaml`, and any of them can be overridden on the command line.

| YAML key | CLI flag | Meaning | Default in `config.yaml` |
|---|---|---|---|
| `TAXON_ID` | `--taxon-id` | NCBI taxonomy ID of the organism | `9685` (*Felis catus*) |
| `GENOME_SIZE` | `--genome-size` | Genome size in bases, used for coverage | `2.7e9` |
| `MIN_COVERAGE` | `--min-coverage` | Minimum estimated coverage to keep a run | `1.0` |
| `RELEASE` | `--release` | OmicIDX release: `latest` or a dated release such as `v2026-11-02` | `latest` |
| `LIBRARY_SOURCE` | `--library-source` | Library source(s) to keep | `GENOMIC` |
| `PLATFORM` | `--platform` | Sequencing platform(s) to keep | `ALL` |
| `OUTPUT_URI` | `--output-uri` | [Batch mode](#weekly-batches): local directory or `s3://bucket/prefix` | `null` (off) |

`LIBRARY_SOURCE` and `PLATFORM` take one value or a list, such as `PLATFORM: [ILLUMINA, PACBIO_SMRT]` in YAML or `--platform ILLUMINA PACBIO_SMRT` on the command line. `ALL` (or `null` in YAML) turns the filter off. Values are not case-sensitive.

- **Library sources:** `GENOMIC`, `GENOMIC SINGLE CELL`, `METAGENOMIC`, `METATRANSCRIPTOMIC`, `TRANSCRIPTOMIC`, `TRANSCRIPTOMIC SINGLE CELL`, `VIRAL RNA`, `SYNTHETIC`, `OTHER`
- **Common platforms:** `ILLUMINA`, `PACBIO_SMRT`, `OXFORD_NANOPORE`, `BGISEQ`, `DNBSEQ`, `ION_TORRENT`, `LS454`, `ULTIMA`, `ELEMENT`

Use `--config path/to/file.yaml` to read a different file. The script stops with an error on an unknown or missing key, a `GENOME_SIZE` of zero or less, or a negative `MIN_COVERAGE`. It prints a warning if no runs match, which usually means a misspelled `LIBRARY_SOURCE` or `PLATFORM`.

## Setup

Requires [uv](https://docs.astral.sh/uv/). The project uses Python 3.15, polars 2.x, pandera and PyYAML.

```bash
uv sync
```

## Usage

```bash
# Print run accessions, one per line, using config.yaml
uv run wgs_runs.py

# Only runs with at least 10x estimated coverage, full table to a TSV
uv run wgs_runs.py --min-coverage 10 -o cat_wgs_10x.tsv

# Illumina only
uv run wgs_runs.py --platform ILLUMINA

# Include metagenomic runs too
uv run wgs_runs.py --library-source GENOMIC METAGENOMIC

# A different organism (dog, ~2.4 Gb genome)
uv run wgs_runs.py --taxon-id 9615 --genome-size 2.4e9

# Pin a release so results are reproducible
uv run wgs_runs.py --release v2026-11-02 -o runs.parquet
```

The settings used and a run count are printed to stderr, so stdout holds only run IDs and can be piped into a download step:

```bash
uv run wgs_runs.py --min-coverage 10 > run_ids.txt
```

A query takes about a minute, depending on your connection.

## Output

Without `-o`, the script prints the `run_accession` of each run that passes. With `-o`, it writes the full table as `.parquet`, `.csv` or `.tsv` (chosen by file extension), with these columns:

| Column | Description |
|---|---|
| `run_accession` | SRA run ID (SRR/ERR/DRR) |
| `experiment_accession`, `sample_accession`, `study_accession` | Parent SRA records |
| `study_title`, `bioproject` | Study title and BioProject ID |
| `taxon_id` | NCBI taxonomy ID |
| `library_strategy`, `library_source`, `library_layout` | Library details (`WGS`; `GENOMIC`/`METAGENOMIC`; `PAIRED`/`SINGLE`) |
| `platform`, `instrument_model` | Sequencing platform and instrument |
| `total_spots`, `total_bases` | Reads (spots) and bases in the run |
| `total_gb` | `total_bases` in gigabases |
| `est_coverage` | `total_bases / GENOME_SIZE` |

Example counts from the 2026-11-02 release, for cat at ≥1x coverage:

| Settings | Runs | Studies |
|---|---|---|
| Default (`GENOMIC`, any platform) | 1,550 | 64 |
| `--platform ILLUMINA` | 1,455 | 60 |
| `--library-source ALL` | 1,954 | 68 |

## Weekly batches

With `OUTPUT_URI` set, each run writes a **batch** containing only runs that weren't in any earlier batch:

```bash
uv run wgs_runs.py --output-uri ./batches               # local directory
uv run wgs_runs.py --output-uri s3://my-bucket/wgs_runs  # S3 (uses your AWS credentials)
```

Each batch is named after the dated OmicIDX release it was built from. `latest` is resolved to that date first, so a batch can be reproduced exactly. The layout under `OUTPUT_URI` is:

```
seen_runs.parquet                  every run accession output so far, with the batch it first appeared in
batches/v2026-11-03/runs.parquet   full table (columns as above) for the runs new in this batch
batches/v2026-11-03/run_ids.txt    the same runs, one accession per line
batches/v2026-11-03/manifest.json  parameters, release date and counts (matched, new, already seen)
```

- **The first batch contains every run that currently matches.** For cat with the defaults, the 2026-11-03 release gives 1,550.
- **Later batches contain only new runs.** A week with no new runs still writes a batch, with an empty `runs.parquet` and `run_ids.txt`.
- **Running again for the same release does nothing.** The batch is complete once its `manifest.json` exists. If a run fails partway, the next run rebuilds the same batch from scratch, so no runs are skipped or repeated.
- **A run that is filtered out for a while and then comes back is not output again.** It is still listed in `seen_runs.parquet`.
- **Changing the filters affects what counts as new.** For example, lowering `MIN_COVERAGE` makes runs that were excluded before appear in the next batch. To keep separate histories for different settings or organisms, give each its own `OUTPUT_URI`.
- **Batch mode and `-o` can't be combined.**

## Running weekly on AWS

EventBridge Scheduler invokes a Lambda function (`omics-sra-wgs-batches`) every Monday at 13:00 Pacific. The function runs batch mode and writes to `s3://omics-sra-<account-id>/wgs_runs/taxon_id=9685/`. Its settings come from `config.yaml`, which is built into the image. The infrastructure is defined in two CloudFormation templates in [infra/](infra/):

- **`infra/bootstrap.yaml`:** the ECR image repository, plus an IAM role that GitHub Actions on `main` can assume (through OIDC) to push images and update the function. It can do nothing else.
- **`infra/app.yaml`:**
  - the S3 bucket: private, versioned and encrypted, and kept if the stack is deleted
  - the Lambda function: arm64, 1 GB memory, 15-minute timeout, at most one copy running at a time
  - the weekly schedule
  - a CloudWatch alarm that emails you (optionally) when the function fails
  - an SNS topic, `omics-sra-batches`, that emails you each time a new batch is written

**Deploying** (needs the AWS CLI, and the `gh` CLI for step 2):

```bash
infra/deploy.sh bootstrap                                     # 1. once
gh variable set AWS_DEPLOY_ROLE_ARN --body <DeployRoleArn>    # 2. once, from the bootstrap output
# 3. merge to main: the deploy workflow builds the image and pushes it to ECR
infra/deploy.sh app "" you@example.com                        # 4. bucket, function, schedule, alarm
```

After that, every merge to `main` builds a new image (see [.github/workflows/deploy.yml](.github/workflows/deploy.yml)) and updates the function. Re-running `infra/deploy.sh app` keeps whatever image the function currently runs.

**Running on demand, and checking results:**

```bash
aws lambda invoke --function-name omics-sra-wgs-batches --cli-read-timeout 900 out.json && cat out.json
aws lambda invoke --function-name omics-sra-wgs-batches --cli-binary-format raw-in-base64-out \
    --payload '{"MIN_COVERAGE": 10}' out.json                 # override any config.yaml key for one run
aws s3 ls --recursive s3://omics-sra-<account-id>/wgs_runs/
aws logs tail /aws/lambda/omics-sra-wgs-batches --since 7d
```

**Email notifications.** Each new batch sends an email with the subject `omics-sra: <N> new WGS runs in batch <release>`. It contains the counts, the filters used, the top studies in the batch (runs and Gb), the first 20 run IDs, and links to the files and the S3 console.
- Weeks with zero new runs still send an email, so you know the job ran.
- A rerun for a release that was already processed sends nothing.
- By default, notices go to the same address as the failure alerts. To use a different one, pass `BatchEmail` when you deploy.
- AWS sends a confirmation email for each subscription, and nothing is delivered until you click its link.
- You can add more subscribers to the `omics-sra-batches` topic in the SNS console.

To change the schedule, redeploy with a different `ScheduleExpression`, e.g. `cron(0 6 ? * SUN *)`. You can also edit the `omics-sra-weekly` schedule in the EventBridge Scheduler console. To track a different organism, change `config.yaml` and the stack's `OutputPrefix` parameter.

The image is built on `python:3.15.2-slim` with [awslambdaric](https://github.com/aws/aws-lambda-python-runtime-interface-client), because AWS doesn't offer a Python 3.15 Lambda base image yet.

## Tests

```bash
uv run pytest
```

None of the tests use the network:
- **`tests/test_schemas.py`:** checks the pandera input and output schemas, and runs `find_wgs_runs` against small local parquet files.
- **`tests/test_batches.py`:** covers batch mode: first and later batches, reruns, empty weeks, recovery from a crash at each write, S3 storage (with [moto](https://github.com/getmoto/moto)), the Lambda handler and the batch emails (SNS).

## Caveats

- **`GENOMIC` is the default library source for a reason.** Some runs tagged with the organism's taxon ID are metagenomes (for example, a gut microbiome with the cat as host). Turning the filter off with `--library-source ALL` brings these back.
- **Data quality depends on SRA submitters.** Taxon IDs, library strategies and base counts come from what each submitter entered.
