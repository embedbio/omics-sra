"""Tests for the pandera schemas in wgs_runs.py.

All tests use small in-memory or local parquet frames; none hit the network.
"""

import polars as pl
import pytest
from pandera.errors import SchemaError, SchemaErrors

import wgs_runs as w
from wgs_runs import Params, RunWithStudy, WgsRuns

INPUT_DTYPES = {
    RunWithStudy.run_accession: pl.String,
    RunWithStudy.experiment_accession: pl.String,
    RunWithStudy.sample_accession: pl.String,
    RunWithStudy.study_accession: pl.String,
    RunWithStudy.study_title: pl.String,
    RunWithStudy.bioproject: pl.String,
    RunWithStudy.taxon_id: pl.Int32,
    RunWithStudy.library_strategy: pl.String,
    RunWithStudy.library_source: pl.String,
    RunWithStudy.library_layout: pl.String,
    RunWithStudy.platform: pl.String,
    RunWithStudy.instrument_model: pl.String,
    RunWithStudy.total_spots: pl.Int64,
    RunWithStudy.total_bases: pl.Int64,
}
OUTPUT_DTYPES = INPUT_DTYPES | {WgsRuns.total_gb: pl.Float64, WgsRuns.est_coverage: pl.Float64}


def make_input_row(**overrides) -> dict:
    row = {
        RunWithStudy.run_accession: "SRR0000001",
        RunWithStudy.experiment_accession: "SRX0000001",
        RunWithStudy.sample_accession: "SRS0000001",
        RunWithStudy.study_accession: "SRP0000001",
        RunWithStudy.study_title: "Cat genomes",
        RunWithStudy.bioproject: "PRJNA1",
        RunWithStudy.taxon_id: 9685,
        RunWithStudy.library_strategy: "WGS",
        RunWithStudy.library_source: "GENOMIC",
        RunWithStudy.library_layout: "PAIRED",
        RunWithStudy.platform: "ILLUMINA",
        RunWithStudy.instrument_model: "Illumina NovaSeq 6000",
        RunWithStudy.total_spots: 100_000_000,
        RunWithStudy.total_bases: 27_000_000_000,
    }
    return row | overrides


def make_output_row(**overrides) -> dict:
    row = make_input_row()
    row[WgsRuns.total_gb] = row[WgsRuns.total_bases] / 1e9
    row[WgsRuns.est_coverage] = row[WgsRuns.total_bases] / 2.7e9
    return row | overrides


def input_frame(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame(list(rows) or [make_input_row()], schema=INPUT_DTYPES)


def output_frame(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame(list(rows) or [make_output_row()], schema=OUTPUT_DTYPES)


def failed_checks(exc: SchemaErrors) -> set[tuple[str | None, str]]:
    """(column, check) pairs from a lazy=True validation failure."""
    return {(c["column"], c["check"]) for group in exc.message.values() for cases in group.values() for c in cases}


# ---------------------------------------------------------------------------
# Column-name attributes
# ---------------------------------------------------------------------------

def test_column_attributes_are_plain_column_names():
    assert RunWithStudy.taxon_id == "taxon_id"
    assert WgsRuns.est_coverage == "est_coverage"
    assert w.columns(RunWithStudy) == list(INPUT_DTYPES)
    assert w.columns(WgsRuns) == list(OUTPUT_DTYPES)


def test_misspelled_column_attribute_raises():
    with pytest.raises(AttributeError):
        _ = WgsRuns.est_covrage


# ---------------------------------------------------------------------------
# RunWithStudy (input)
# ---------------------------------------------------------------------------

def test_input_valid_frame_passes():
    df = input_frame()
    assert RunWithStudy.validate(df).equals(df)


def test_input_allows_extra_columns():
    # The real file has more columns than the script reads.
    df = input_frame().with_columns(pl.lit("x").alias("organism"))
    RunWithStudy.validate(df)


@pytest.mark.parametrize("column", [
    RunWithStudy.study_accession, RunWithStudy.study_title, RunWithStudy.bioproject,
    RunWithStudy.library_source, RunWithStudy.platform,
    RunWithStudy.total_spots, RunWithStudy.total_bases,
])
def test_input_nullable_columns_accept_nulls(column):
    # These columns have nulls in real OmicIDX data.
    RunWithStudy.validate(input_frame(make_input_row(**{column: None})))


@pytest.mark.parametrize("column", [
    RunWithStudy.run_accession, RunWithStudy.experiment_accession,
    RunWithStudy.sample_accession, RunWithStudy.taxon_id,
])
def test_input_required_columns_reject_nulls(column):
    with pytest.raises(SchemaError, match="non-nullable|null"):
        RunWithStudy.validate(input_frame(make_input_row(**{column: None})))


def test_input_missing_column_fails():
    with pytest.raises(SchemaError, match=RunWithStudy.taxon_id):
        RunWithStudy.validate(input_frame().drop(RunWithStudy.taxon_id))


def test_input_wrong_dtype_fails():
    # e.g. upstream widening taxon_id from Int32 to Int64
    df = input_frame().with_columns(pl.col(RunWithStudy.taxon_id).cast(pl.Int64))
    with pytest.raises(SchemaError, match="Int32|dtype"):
        RunWithStudy.validate(df)


def test_input_lazyframe_checks_schema_without_collecting():
    lf = input_frame().drop(RunWithStudy.total_bases).lazy()
    with pytest.raises(SchemaError, match=RunWithStudy.total_bases):
        RunWithStudy.validate(lf)


def test_input_lazyframe_returns_lazyframe():
    assert isinstance(RunWithStudy.validate(input_frame().lazy()), pl.LazyFrame)


# ---------------------------------------------------------------------------
# WgsRuns (output)
# ---------------------------------------------------------------------------

def test_output_valid_frame_passes():
    df = output_frame(make_output_row(), make_output_row(run_accession="ERR123"), make_output_row(run_accession="DRR9"))
    assert WgsRuns.validate(df).equals(df)


@pytest.mark.parametrize("run_id", ["XRR1", "SRX0000001", "SRR", "srr123", "SRR12a", " SRR1"])
def test_output_rejects_bad_run_accession(run_id):
    with pytest.raises(SchemaError, match="str_matches"):
        WgsRuns.validate(output_frame(make_output_row(run_accession=run_id)))


def test_output_rejects_duplicate_run_accession():
    with pytest.raises(SchemaError, match="unique|duplicate"):
        WgsRuns.validate(output_frame(make_output_row(), make_output_row()))


@pytest.mark.parametrize("strategy", ["RNA-Seq", "WXS", "wgs", None])
def test_output_rejects_non_wgs_library_strategy(strategy):
    with pytest.raises(SchemaError):
        WgsRuns.validate(output_frame(make_output_row(library_strategy=strategy)))


@pytest.mark.parametrize("column", [WgsRuns.total_bases, WgsRuns.total_gb, WgsRuns.est_coverage])
def test_output_rejects_negative_values(column):
    value = -1 if column == WgsRuns.total_bases else -1.0
    with pytest.raises(SchemaError, match="greater_than_or_equal_to"):
        WgsRuns.validate(output_frame(make_output_row(**{column: value})))


def test_output_total_bases_not_nullable():
    # Nullable in the input, but runs without bases can't have a coverage.
    with pytest.raises(SchemaError, match="null"):
        WgsRuns.validate(output_frame(make_output_row(total_bases=None)))


def test_output_inherits_input_nullability():
    WgsRuns.validate(output_frame(make_output_row(study_accession=None, bioproject=None)))


def test_output_strict_rejects_extra_columns():
    df = output_frame().with_columns(pl.lit("x").alias("organism"))
    with pytest.raises(SchemaError, match="organism"):
        WgsRuns.validate(df)


def test_output_missing_derived_column_fails():
    with pytest.raises(SchemaError, match=WgsRuns.est_coverage):
        WgsRuns.validate(output_frame().drop(WgsRuns.est_coverage))


def test_output_lazy_reports_every_failure():
    df = output_frame(
        make_output_row(run_accession="BAD1"),
        make_output_row(run_accession="SRR2", library_strategy="RNA-Seq", est_coverage=-1.0),
    )
    with pytest.raises(SchemaErrors) as exc_info:
        WgsRuns.validate(df, lazy=True)
    failed_columns = {col for col, _ in failed_checks(exc_info.value)}
    assert {WgsRuns.run_accession, WgsRuns.library_strategy, WgsRuns.est_coverage} <= failed_columns


# ---------------------------------------------------------------------------
# find_wgs_runs against a local parquet file
# ---------------------------------------------------------------------------

@pytest.fixture
def local_source(tmp_path, monkeypatch):
    """Point find_wgs_runs at a local parquet file instead of OmicIDX."""
    path = tmp_path / "{release}.parquet"
    monkeypatch.setattr(w, "OMICIDX_URL", str(path))

    def write(df: pl.DataFrame, release: str = "test") -> Params:
        df.write_parquet(str(path).format(release=release))
        return Params(taxon_id=9685, genome_size=2.7e9, min_coverage=1.0, release=release)

    return write


def test_find_wgs_runs_output_passes_schema(local_source):
    params = local_source(input_frame(
        make_input_row(run_accession="SRR1"),                                   # kept, 10x
        make_input_row(run_accession="SRR2", library_strategy="RNA-Seq"),       # wrong strategy
        make_input_row(run_accession="SRR3", taxon_id=9606),                    # wrong taxon
        make_input_row(run_accession="SRR4", library_source="METAGENOMIC"),     # wrong source
        make_input_row(run_accession="SRR5", total_bases=1_000_000_000),        # < 1x coverage
        make_input_row(run_accession="SRR6", total_bases=None, total_spots=None),  # no bases
        make_input_row(run_accession="SRR7", library_strategy="\n\t\tWGS\n",    # padded values, kept
                       library_source="\n  GENOMIC\n"),
    ))
    runs = w.find_wgs_runs(params)
    assert runs[WgsRuns.run_accession].to_list() == ["SRR1", "SRR7"]
    assert runs[WgsRuns.est_coverage].to_list() == pytest.approx([10.0, 10.0])
    assert runs.columns == w.columns(WgsRuns)


def test_find_wgs_runs_rejects_upstream_schema_change(local_source):
    params = local_source(input_frame().with_columns(pl.col(RunWithStudy.taxon_id).cast(pl.Int64)))
    with pytest.raises(SchemaError):
        w.find_wgs_runs(params)


def test_find_wgs_runs_rejects_renamed_upstream_column(local_source):
    params = local_source(input_frame().rename({RunWithStudy.total_bases: "bases"}))
    with pytest.raises((SchemaError, pl.exceptions.ColumnNotFoundError)):
        w.find_wgs_runs(params)


def test_find_wgs_runs_rejects_bad_output(local_source):
    # Passes the input schema but violates the output schema.
    params = local_source(input_frame(make_input_row(run_accession="NOT_A_RUN")))
    with pytest.raises(SchemaErrors) as exc_info:
        w.find_wgs_runs(params)
    assert (WgsRuns.run_accession, r"str_matches('^[SED]RR\d+$')") in failed_checks(exc_info.value)
