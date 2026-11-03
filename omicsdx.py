import polars as pl

base = "https://omicidx.cancerdatasci.org/data/latest"

# 1. Filter samples by taxon ID 9685 (Felis catus)
samples_lazy = (
    pl.scan_parquet(f"{base}/sradb/sample.parquet")
    .filter(pl.col("taxon_id") == 9685)
    .select("study_accession", "organism", "taxon_id")
)

# 2. Join with studies, filter by study_type, and deduplicate study_accession
wgs_studies = (
    pl.scan_parquet(f"{base}/sradb/study.parquet")
    .filter(pl.col("study_type") == "Whole Genome Sequencing")
    .join(samples_lazy, on="study_accession")
    .select("study_accession", "study_title", "study_type", "organism", "taxon_id")
    .unique(subset=["study_accession"])  # Pass as positional argument or list
    .collect()
)

print(wgs_studies)
