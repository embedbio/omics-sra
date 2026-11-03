"""Find SRA whole genome sequencing (WGS) runs for one organism in OmicIDX.

Parameters come from a YAML config file (default: config.yaml next to this script).
Command-line flags override values from the file.

Examples:
    uv run wgs_runs.py
    uv run wgs_runs.py --taxon-id 9615 --genome-size 2.4e9 --min-coverage 5
    uv run wgs_runs.py --config other.yaml --output runs.tsv
"""

import argparse
import dataclasses
import sys
from enum import StrEnum
from pathlib import Path
from typing import Self

import pandera.polars as pa
import polars as pl
import yaml
from pandera.errors import SchemaError, SchemaErrors

DEFAULT_CONFIG = Path(__file__).with_name("config.yaml")
OMICIDX_URL = "https://omicidx.cancerdatasci.org/data/{release}/sradb/run_with_study.parquet"


class LibraryStrategy(StrEnum):
    WGS = "WGS"


class OutputFormat(StrEnum):
    PARQUET = ".parquet"
    CSV = ".csv"
    TSV = ".tsv"


# ---------------------------------------------------------------------------
# Schemas. Column names are the class attributes (RunWithStudy.taxon_id == "taxon_id"),
# so a typo is an AttributeError instead of a silent wrong string, and renaming
# a column is a single edit.
# ---------------------------------------------------------------------------

class RunWithStudy(pa.DataFrameModel):
    """Columns this script reads from OmicIDX sradb/run_with_study.parquet.

    Validated against the remote file's schema before any data is fetched,
    so an upstream rename or type change fails fast with a clear error.
    """

    run_accession: pl.String
    experiment_accession: pl.String
    sample_accession: pl.String
    study_accession: pl.String = pa.Field(nullable=True)
    study_title: pl.String = pa.Field(nullable=True)
    bioproject: pl.String = pa.Field(nullable=True)
    taxon_id: pl.Int32
    library_strategy: pl.String = pa.Field(nullable=True)
    library_source: pl.String = pa.Field(nullable=True)
    library_layout: pl.String = pa.Field(nullable=True)
    platform: pl.String = pa.Field(nullable=True)
    instrument_model: pl.String = pa.Field(nullable=True)
    total_spots: pl.Int64 = pa.Field(nullable=True)
    total_bases: pl.Int64 = pa.Field(nullable=True)


class WgsRuns(RunWithStudy):
    """Output table: filtered WGS runs plus derived size and coverage columns."""

    run_accession: pl.String = pa.Field(unique=True, str_matches=r"^[SED]RR\d+$")
    library_strategy: pl.String = pa.Field(isin=[s.value for s in LibraryStrategy])
    total_bases: pl.Int64 = pa.Field(ge=0)
    total_gb: pl.Float64 = pa.Field(ge=0)
    est_coverage: pl.Float64 = pa.Field(ge=0)

    class Config:
        strict = True  # no columns beyond those declared


def columns(model: type[pa.DataFrameModel]) -> list[str]:
    return list(model.to_schema().columns)


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

NO_FILTER = "ALL"  # LIBRARY_SOURCE / PLATFORM value meaning "don't filter"


def _filter_values(value: object) -> tuple[str, ...] | None:
    """Normalize a filter parameter: one value or a list -> upper-case tuple; null/ALL -> None."""
    if value is None:
        return None
    items = [value] if isinstance(value, str) else list(value)
    values = tuple(str(v).strip().upper() for v in items)
    return None if not values or NO_FILTER in values else values


@dataclasses.dataclass(frozen=True, slots=True)
class Params:
    """Run parameters. YAML keys are the field names in upper case (TAXON_ID, ...)."""

    taxon_id: int
    genome_size: float
    min_coverage: float
    release: str = "latest"
    library_source: tuple[str, ...] | None = ("GENOMIC",)
    platform: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        # Coerce types: PyYAML reads 2.7e9 as a string, not a float.
        object.__setattr__(self, "taxon_id", int(self.taxon_id))
        object.__setattr__(self, "genome_size", float(self.genome_size))
        object.__setattr__(self, "min_coverage", float(self.min_coverage))
        object.__setattr__(self, "release", str(self.release))
        object.__setattr__(self, "library_source", _filter_values(self.library_source))
        object.__setattr__(self, "platform", _filter_values(self.platform))
        if self.genome_size <= 0:
            raise ValueError("GENOME_SIZE must be > 0")
        if self.min_coverage < 0:
            raise ValueError("MIN_COVERAGE must be >= 0")

    @staticmethod
    def yaml_key(field_name: str) -> str:
        return field_name.upper()

    @classmethod
    def from_sources(cls, config: dict, overrides: dict) -> Self:
        """Build from YAML config then CLI overrides (CLI wins)."""
        names = {f.name for f in dataclasses.fields(cls)}
        by_key = {cls.yaml_key(n): n for n in names}

        unknown = set(config) - set(by_key)
        if unknown:
            raise ValueError(f"unknown parameter(s): {', '.join(sorted(unknown))}")

        values = {by_key[k]: v for k, v in config.items()}
        values |= {n: v for n, v in overrides.items() if n in names and v is not None}

        required = [f.name for f in dataclasses.fields(cls) if f.default is dataclasses.MISSING]
        missing = [cls.yaml_key(n) for n in required if values.get(n) is None]
        if missing:
            raise ValueError(f"missing parameter(s): {', '.join(missing)} (set in YAML or on the command line)")
        return cls(**values)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="YAML parameter file (default: %(default)s)")
    p.add_argument("--taxon-id", type=int, help="NCBI taxonomy ID")
    p.add_argument("--genome-size", type=float, help="genome size in bases, e.g. 2.7e9")
    p.add_argument("--min-coverage", type=float, help="minimum estimated coverage")
    p.add_argument("--release", help='OmicIDX release, "latest" or e.g. "v2026-11-02"')
    p.add_argument("--library-source", nargs="+", metavar="SOURCE",
                   help=f'one or more library sources, e.g. GENOMIC METAGENOMIC; "{NO_FILTER}" for no filter')
    p.add_argument("--platform", nargs="+", metavar="PLATFORM",
                   help=f'one or more platforms, e.g. ILLUMINA PACBIO_SMRT; "{NO_FILTER}" for no filter')
    p.add_argument("-o", "--output", type=Path,
                   help=f"write the full table ({', '.join(OutputFormat)}); otherwise print run IDs to stdout")
    return p.parse_args()


def load_params(args: argparse.Namespace) -> Params:
    if args.config.exists():
        config = yaml.safe_load(args.config.read_text()) or {}
    elif args.config != DEFAULT_CONFIG:
        sys.exit(f"error: config file not found: {args.config}")
    else:
        config = {}
    try:
        return Params.from_sources(config, vars(args))
    except (ValueError, TypeError) as e:
        sys.exit(f"error: {e}")


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def find_wgs_runs(params: Params) -> pl.DataFrame:
    # run_with_study already joins run -> experiment -> sample -> study.
    # The lazy scan lets polars push the filters down and fetch only these columns.
    source = pl.scan_parquet(OMICIDX_URL.format(release=params.release)).select(columns(RunWithStudy))
    source = RunWithStudy.validate(source)  # schema-level check only; no data fetched yet

    C = WgsRuns
    filters = [pl.col(C.library_strategy) == LibraryStrategy.WGS]
    if params.library_source:
        filters.append(pl.col(C.library_source).is_in(params.library_source))
    if params.platform:
        filters.append(pl.col(C.platform).is_in(params.platform))

    runs = (
        source
        .filter(pl.col(C.taxon_id) == params.taxon_id)
        # A few SRA records have whitespace-padded values (e.g. "\n\tGENOMIC\n").
        # Strip them in the output too, so the output schema's isin check holds.
        .with_columns(pl.col(C.library_strategy, C.library_source, C.platform).str.strip_chars())
        .filter(*filters)
        .with_columns(
            (pl.col(C.total_bases) / 1e9).alias(C.total_gb),
            (pl.col(C.total_bases) / params.genome_size).alias(C.est_coverage),
        )
        .filter(pl.col(C.est_coverage) >= params.min_coverage)
        .collect()
    )
    return WgsRuns.validate(runs, lazy=True)  # full data checks, report every failure


def write_table(df: pl.DataFrame, path: Path) -> None:
    match OutputFormat(path.suffix.lower()):
        case OutputFormat.PARQUET:
            df.write_parquet(path)
        case OutputFormat.CSV:
            df.write_csv(path)
        case OutputFormat.TSV:
            df.write_csv(path, separator="\t")


def main() -> None:
    args = parse_args()
    if args.output and args.output.suffix.lower() not in set(OutputFormat):
        sys.exit(f"error: unsupported output format {args.output.suffix!r} (use {', '.join(OutputFormat)})")
    params = load_params(args)
    def show(v: object) -> str:
        return ",".join(v) if isinstance(v, tuple) else NO_FILTER if v is None else str(v)

    print("parameters: " + ", ".join(f"{Params.yaml_key(k)}={show(v)}" for k, v in dataclasses.asdict(params).items()),
          file=sys.stderr)

    try:
        runs = find_wgs_runs(params)
    except (SchemaError, SchemaErrors) as e:
        sys.exit(f"error: OmicIDX data failed schema validation:\n{e}")
    print(f"{runs.height:,} runs across {runs[WgsRuns.study_accession].n_unique()} studies", file=sys.stderr)
    if runs.is_empty():
        print("warning: no runs matched; check TAXON_ID, LIBRARY_SOURCE and PLATFORM spelling", file=sys.stderr)

    if args.output:
        write_table(runs, args.output)
        print(f"wrote {args.output}", file=sys.stderr)
    else:
        sys.stdout.write("".join(f"{r}\n" for r in runs[WgsRuns.run_accession]))


if __name__ == "__main__":
    main()
