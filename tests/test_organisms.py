"""Tests for the ORGANISMS section of the config (organism_params) and multi-organism CLI runs."""

import sys

import polars as pl
import pytest
import yaml

import batches as b
import notify
import wgs_runs as w
from test_schemas import make_output_row, output_frame
from wgs_runs import Params, organism_params

CONFIG = {
    "MIN_COVERAGE": 1.0,
    "LIBRARY_SOURCE": "GENOMIC",
    "ORGANISMS": {
        "cat": {"TAXON_ID": 9685, "GENOME_SIZE": "2.7e9"},
        "dog": {"TAXON_ID": 9615, "GENOME_SIZE": "2.4e9", "MIN_COVERAGE": 5, "PLATFORM": "ILLUMINA"},
    },
}


def by_name(params: list[Params]) -> dict[str, Params]:
    return {p.organism: p for p in params}


def test_one_params_per_organism_in_config_order():
    ps = organism_params(CONFIG)
    assert [(p.organism, p.taxon_id, p.genome_size) for p in ps] == [("cat", 9685, 2.7e9), ("dog", 9615, 2.4e9)]


def test_organism_entries_override_defaults():
    ps = by_name(organism_params(CONFIG))
    assert ps["cat"].min_coverage == 1.0 and ps["cat"].platform is None
    assert ps["dog"].min_coverage == 5.0 and ps["dog"].platform == ("ILLUMINA",)
    assert ps["dog"].library_source == ("GENOMIC",)  # inherited


def test_overrides_apply_to_every_organism_and_win():
    ps = organism_params(CONFIG, {"min_coverage": 10, "release": "v2026-11-02"})
    assert all(p.min_coverage == 10.0 and p.release == "v2026-11-02" for p in ps)


def test_only_selects_organisms():
    assert [p.organism for p in organism_params(CONFIG, only=["dog"])] == ["dog"]
    with pytest.raises(ValueError, match="unknown organism.*dgo.*the config has cat, dog"):
        organism_params(CONFIG, only=["dgo"])


def test_each_organism_gets_its_own_output_folder():
    ps = by_name(organism_params(CONFIG | {"OUTPUT_URI": "s3://bucket/wgs_runs/"}))
    assert ps["cat"].output_uri == "s3://bucket/wgs_runs/taxon_id=9685/"
    assert ps["dog"].output_uri == "s3://bucket/wgs_runs/taxon_id=9615/"
    # the same with no trailing slash, or from an override
    ps = by_name(organism_params(CONFIG, {"output_uri": "out"}))
    assert ps["dog"].output_uri == "out/taxon_id=9615/"


def test_no_output_uri_means_one_shot():
    assert all(p.output_uri is None for p in organism_params(CONFIG))


def test_taxon_id_override_without_organism_is_a_one_off_query():
    [p] = organism_params(CONFIG | {"OUTPUT_URI": "out"}, {"taxon_id": 9913, "genome_size": 2.7e9})
    assert (p.organism, p.taxon_id, p.min_coverage, p.output_uri) == (None, 9913, 1.0, "out/taxon_id=9913/")
    with pytest.raises(ValueError, match="GENOME_SIZE"):
        organism_params(CONFIG, {"taxon_id": 9913})


def test_taxon_id_override_with_organism_overrides_that_organism():
    [p] = organism_params(CONFIG, {"taxon_id": 9612}, only=["dog"])
    assert (p.organism, p.taxon_id, p.genome_size) == ("dog", 9612, 2.4e9)


def test_config_without_organisms_section_is_one_organism():
    [p] = organism_params({"TAXON_ID": 9685, "GENOME_SIZE": 2.7e9, "MIN_COVERAGE": 1, "OUTPUT_URI": "out"})
    assert (p.organism, p.taxon_id, p.output_uri) == (None, 9685, "out")  # OUTPUT_URI used as given
    with pytest.raises(ValueError, match="no ORGANISMS section"):
        organism_params({"TAXON_ID": 9685, "GENOME_SIZE": 2.7e9, "MIN_COVERAGE": 1}, only=["cat"])


@pytest.mark.parametrize("organisms, match", [
    ({}, "ORGANISMS must map"),
    ([{"TAXON_ID": 1}], "ORGANISMS must map"),
    ({"cow": ["TAXON_ID", 9913]}, "ORGANISMS.cow must be a mapping"),
    ({"cow": {"TAXON_ID": 9913}}, r"ORGANISMS\.cow: missing parameter\(s\): GENOME_SIZE"),
    ({"cow": None}, r"ORGANISMS\.cow: missing parameter\(s\): TAXON_ID, GENOME_SIZE"),
    ({"cow": {"TAXON_ID": 9913, "GENOME_SIZE": 2.7e9, "GENOME_SIZ": 1}}, r"ORGANISMS\.cow: unknown parameter.*GENOME_SIZ"),
    ({"cow": {"TAXON_ID": 9913, "GENOME_SIZE": 0}}, r"ORGANISMS\.cow: GENOME_SIZE must be > 0"),
    ({"cow": {"TAXON_ID": 9913, "GENOME_SIZE": 2.7e9, "OUTPUT_URI": "x"}}, "OUTPUT_URI can't be set per organism"),
    ({"cat": {"TAXON_ID": 9685, "GENOME_SIZE": 2.7e9}, "kitty": {"TAXON_ID": 9685, "GENOME_SIZE": 2.7e9}},
     "cat and kitty have the same TAXON_ID 9685"),
])
def test_bad_organisms_section_is_rejected(organisms, match):
    with pytest.raises(ValueError, match=match):
        organism_params({"MIN_COVERAGE": 1, "ORGANISMS": organisms})


def test_bundled_config_parses():
    config = yaml.safe_load(w.DEFAULT_CONFIG.read_text())
    ps = by_name(organism_params(config))
    assert (ps["cat"].taxon_id, ps["cat"].genome_size) == (9685, 2.7e9)
    assert (ps["dog"].taxon_id, ps["dog"].genome_size) == (9615, 2.4e9)


def test_label():
    assert Params(taxon_id=9615, genome_size=1, min_coverage=0, organism="dog").label == "dog (taxon 9615)"
    assert Params(taxon_id=9615, genome_size=1, min_coverage=0).label == "taxon 9615"


def test_notify_subject_names_the_organism():
    r = b.BatchResult("v2026-11-10", "2026-11-10", False, 10, 3, 7, "s3://b/p/", 9615, "dog")
    assert notify.subject(r) == "omics-sra: 3 new dog (taxon 9615) WGS runs in batch v2026-11-10"
    long = b.BatchResult("v2026-11-10", "2026-11-10", False, 10, 3, 7, "s3://b/p/", 9615, "d" * 200)
    assert len(notify.subject(long)) == notify.SNS_SUBJECT_LIMIT


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@pytest.fixture
def cli(tmp_path, monkeypatch):
    """Run wgs_runs.main() with a temp config and a fake query returning one run per taxon."""
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump(CONFIG))
    calls = []

    def fake_find(params):
        calls.append(params)
        return output_frame(make_output_row(run_accession=f"SRR{params.taxon_id}", taxon_id=params.taxon_id))

    monkeypatch.setattr(w, "find_wgs_runs", fake_find)

    def run(*args):
        monkeypatch.setattr(sys, "argv", ["wgs_runs.py", "--config", str(config), *args])
        w.main()
        return calls

    return run


def test_cli_queries_every_organism(cli, capsys):
    calls = cli()
    assert [p.organism for p in calls] == ["cat", "dog"]
    out, err = capsys.readouterr()
    assert out.split() == ["SRR9685", "SRR9615"]
    assert "cat (taxon 9685): 1 runs" in err and "dog (taxon 9615): 1 runs" in err


def test_cli_organism_flag(cli, capsys):
    assert [p.organism for p in cli("--organism", "dog", "--min-coverage", "7")] == ["dog"]
    assert capsys.readouterr().out.split() == ["SRR9615"]


def test_cli_writes_one_table_for_all_organisms(cli, tmp_path):
    cli("-o", str(tmp_path / "runs.parquet"))
    df = pl.read_parquet(tmp_path / "runs.parquet")
    assert sorted(df["taxon_id"].to_list()) == [9615, 9685]


def test_cli_unknown_organism_exits(cli):
    with pytest.raises(SystemExit, match="unknown organism"):
        cli("--organism", "dgo")
