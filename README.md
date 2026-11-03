# omics-sra

`wgs_runs.py` finds SRA whole genome sequencing (WGS) runs for one organism that have enough sequencing depth. It queries the [OmicIDX](https://omicidx.cancerdatasci.org) table `run_with_study.parquet` and writes out the runs whose estimated coverage meets a minimum you set.

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

## Tests

```bash
uv run pytest
```

The tests in `tests/test_schemas.py` check the pandera input and output schemas, and run `find_wgs_runs` against small local parquet files. They don't use the network.

## Caveats

- **`GENOMIC` is the default library source for a reason.** Some runs tagged with the organism's taxon ID are metagenomes (for example, a gut microbiome with the cat as host). Turning the filter off with `--library-source ALL` brings these back.
- **Data quality depends on SRA submitters.** Taxon IDs, library strategies and base counts come from what each submitter entered.
