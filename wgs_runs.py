"""Find SRA whole genome sequencing (WGS) runs for one or more organisms in OmicIDX.

Parameters come from a YAML config file (default: config.yaml next to this script).
Its ORGANISMS section lists the organisms to query; top-level keys are defaults
shared by all of them. Command-line flags override values from the file.

Examples:
    uv run wgs_runs.py                          # every organism in config.yaml
    uv run wgs_runs.py --organism dog --min-coverage 5
    uv run wgs_runs.py --taxon-id 9913 --genome-size 2.7e9   # one-off organism not in the config
    uv run wgs_runs.py --config other.yaml --output runs.tsv
"""

import argparse
import dataclasses
import json
import sys
import urllib.request
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Self

import pandera.polars as pa
import polars as pl
import yaml
from pandera.errors import SchemaError, SchemaErrors

DEFAULT_CONFIG = Path(__file__).with_name("config.yaml")
OMICIDX_URL = "https://omicidx.cancerdatasci.org/data/{release}/sradb/run_with_study.parquet"
MANIFEST_URL = "https://omicidx.cancerdatasci.org/data/{release}/manifest.json"
USER_AGENT = "omics-sra/0.1"  # the server rejects urllib's default User-Agent


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
    output_uri: str | None = None  # batch mode: local directory or s3://bucket/prefix
    organism: str | None = None    # name from the ORGANISMS section, used in logs and emails

    def __post_init__(self) -> None:
        # Coerce types: PyYAML reads 2.7e9 as a string, not a float.
        object.__setattr__(self, "taxon_id", int(self.taxon_id))
        object.__setattr__(self, "genome_size", float(self.genome_size))
        object.__setattr__(self, "min_coverage", float(self.min_coverage))
        object.__setattr__(self, "release", str(self.release))
        object.__setattr__(self, "library_source", _filter_values(self.library_source))
        object.__setattr__(self, "platform", _filter_values(self.platform))
        if self.output_uri is not None:
            object.__setattr__(self, "output_uri", str(self.output_uri))
        if self.organism is not None:
            object.__setattr__(self, "organism", str(self.organism))
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

    @classmethod
    def overrides_from_yaml(cls, values: dict) -> dict:
        """Turn YAML-style keys ({"MIN_COVERAGE": 5}) into overrides for from_sources."""
        by_key = {cls.yaml_key(f.name): f.name for f in dataclasses.fields(cls)}
        unknown = set(values) - set(by_key)
        if unknown:
            raise ValueError(f"unknown parameter(s): {', '.join(sorted(unknown))}")
        return {by_key[k]: v for k, v in values.items()}

    @property
    def label(self) -> str:
        """How this organism is named in logs and emails, e.g. "dog (taxon 9615)"."""
        return f"{self.organism} (taxon {self.taxon_id})" if self.organism else f"taxon {self.taxon_id}"


ORGANISMS_KEY = "ORGANISMS"


def taxon_uri(root: str, taxon_id: int) -> str:
    """Batch location for one organism under the shared OUTPUT_URI root."""
    return f"{root.rstrip('/')}/taxon_id={taxon_id}/"


def organism_params(config: dict, overrides: dict | None = None,
                    only: Sequence[str] | None = None) -> list[Params]:
    """One Params per organism: top-level config keys, then the organism's entry, then overrides.

    In batch mode each organism writes to <OUTPUT_URI>/taxon_id=<id>/, so organisms
    never share seen_runs.parquet. A config without an ORGANISMS section is a single
    organism, and OUTPUT_URI is then used as given.

    only: organism names to keep (default: all). Overriding taxon_id without naming
    an organism queries that one taxon with the top-level defaults instead.
    """
    overrides = {k: v for k, v in (overrides or {}).items() if v is not None}
    defaults = dict(config)
    organisms = defaults.pop(ORGANISMS_KEY, None)

    if organisms is None:
        if only:
            raise ValueError(f"no {ORGANISMS_KEY} section in the config, so --organism can't be used")
        return [Params.from_sources(defaults, overrides)]
    if not isinstance(organisms, dict) or not organisms:
        raise ValueError(f"{ORGANISMS_KEY} must map organism names to their parameters")

    def place(p: Params) -> Params:
        return dataclasses.replace(p, output_uri=taxon_uri(p.output_uri, p.taxon_id)) if p.output_uri else p

    if "taxon_id" in overrides and not only:
        return [place(Params.from_sources(defaults, overrides))]

    unknown = set(only or ()) - set(organisms)
    if unknown:
        raise ValueError(f"unknown organism(s): {', '.join(sorted(unknown))} "
                         f"(the config has {', '.join(organisms)})")

    result, taxa = [], {}
    for name, entry in organisms.items():
        if only and name not in only:
            continue
        entry = entry or {}
        if not isinstance(entry, dict):
            raise ValueError(f"{ORGANISMS_KEY}.{name} must be a mapping of parameters")
        if blocked := {"OUTPUT_URI", "ORGANISM"} & set(entry):
            raise ValueError(f"{ORGANISMS_KEY}.{name}: {', '.join(sorted(blocked))} can't be set per organism")
        try:
            p = Params.from_sources(defaults | entry | {"ORGANISM": name}, overrides)
        except (ValueError, TypeError) as e:
            raise ValueError(f"{ORGANISMS_KEY}.{name}: {e}") from None
        if p.taxon_id in taxa:
            raise ValueError(f"{ORGANISMS_KEY}: {taxa[p.taxon_id]} and {name} have the same TAXON_ID {p.taxon_id}")
        taxa[p.taxon_id] = name
        result.append(place(p))
    return result


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="YAML parameter file (default: %(default)s)")
    p.add_argument("--organism", dest="organisms", nargs="+", metavar="NAME",
                   help="only these organisms from the config's ORGANISMS section (default: all)")
    p.add_argument("--taxon-id", type=int,
                   help="NCBI taxonomy ID; without --organism, queries just this taxon using the top-level defaults")
    p.add_argument("--genome-size", type=float, help="genome size in bases, e.g. 2.7e9")
    p.add_argument("--min-coverage", type=float, help="minimum estimated coverage")
    p.add_argument("--release", help='OmicIDX release, "latest" or e.g. "v2026-11-02"')
    p.add_argument("--library-source", nargs="+", metavar="SOURCE",
                   help=f'one or more library sources, e.g. GENOMIC METAGENOMIC; "{NO_FILTER}" for no filter')
    p.add_argument("--platform", nargs="+", metavar="PLATFORM",
                   help=f'one or more platforms, e.g. ILLUMINA PACBIO_SMRT; "{NO_FILTER}" for no filter')
    p.add_argument("-o", "--output", type=Path,
                   help=f"write the full table ({', '.join(OutputFormat)}); otherwise print run IDs to stdout")
    p.add_argument("--output-uri",
                   help="batch mode: write only runs not in earlier batches to this local directory or s3://bucket/prefix")
    return p.parse_args()


def load_params(args: argparse.Namespace) -> list[Params]:
    if args.config.exists():
        config = yaml.safe_load(args.config.read_text()) or {}
    elif args.config != DEFAULT_CONFIG:
        sys.exit(f"error: config file not found: {args.config}")
    else:
        config = {}
    try:
        return organism_params(config, vars(args), only=args.organisms)
    except (ValueError, TypeError) as e:
        sys.exit(f"error: {e}")


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True, slots=True)
class Release:
    name: str          # dated release, e.g. "v2026-11-03"
    publish_date: str  # e.g. "2026-11-03"


def resolve_release(release: str) -> Release:
    """Resolve "latest" (or a dated release) to a dated release via its manifest.json."""
    req = urllib.request.Request(MANIFEST_URL.format(release=release), headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        manifest = json.load(resp)
    return Release(name=manifest["dated_path"].strip("/"), publish_date=manifest["publish_date"])


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


def _show(v: object) -> str:
    return ",".join(v) if isinstance(v, tuple) else NO_FILTER if v is None else str(v)


def main() -> None:
    args = parse_args()
    if args.output and args.output.suffix.lower() not in set(OutputFormat):
        sys.exit(f"error: unsupported output format {args.output.suffix!r} (use {', '.join(OutputFormat)})")
    all_params = load_params(args)
    for params in all_params:
        print(f"{params.label}: " + ", ".join(f"{Params.yaml_key(k)}={_show(v)}"
                                             for k, v in dataclasses.asdict(params).items() if k != "organism"),
              file=sys.stderr)

    if any(p.output_uri for p in all_params):
        if args.output:
            sys.exit("error: use either -o/--output (one-shot) or OUTPUT_URI/--output-uri (batch mode), not both")
        from batches import run_batch, store_from_uri  # imported lazily: needs boto3 only in batch mode

        failed = []
        for params in all_params:
            try:
                result = run_batch(params, store_from_uri(params.output_uri))
            except (SchemaError, SchemaErrors) as e:
                print(f"error: {params.label}: data failed schema validation:\n{e}", file=sys.stderr)
                failed.append(params.label)
                continue
            print(f"{params.label}: {result.summary()}", file=sys.stderr)
        if failed:
            sys.exit(f"error: no batch written for {', '.join(failed)}")
        return

    tables = []
    for params in all_params:
        try:
            runs = find_wgs_runs(params)
        except (SchemaError, SchemaErrors) as e:
            sys.exit(f"error: {params.label}: OmicIDX data failed schema validation:\n{e}")
        print(f"{params.label}: {runs.height:,} runs across {runs[WgsRuns.study_accession].n_unique()} studies",
              file=sys.stderr)
        if runs.is_empty():
            print(f"warning: {params.label}: no runs matched; check TAXON_ID, LIBRARY_SOURCE and PLATFORM spelling",
                  file=sys.stderr)
        tables.append(runs)
    runs = pl.concat(tables)  # each row carries its taxon_id

    if args.output:
        write_table(runs, args.output)
        print(f"wrote {args.output}", file=sys.stderr)
    else:
        sys.stdout.write("".join(f"{r}\n" for r in runs[WgsRuns.run_accession]))


if __name__ == "__main__":
    main()
