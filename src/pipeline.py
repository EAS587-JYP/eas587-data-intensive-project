"""Single Databricks entry point: Notebook 04 inspection/Bronze -> Notebook 05 Silver -> Notebook 06 reports.

With the repository root on sys.path:
    from src.pipeline import run_pipeline
    result = run_pipeline(spark, dbutils)

For an isolated test, first redirect data_access.BRONZE_TABLES,
data_access.SILVER_TABLES and data_access.REPORT_ROOT to unused destinations.
Reusing existing tables alone does not test newly transformed values.
"""

from . import data_access as access
from . import data_cleaning as cleaning
from . import data_validation as validation


def run_pipeline(spark, dbutils):
    """Run Notebook 04 -> Notebook 05 -> Notebook 06 and return the table names and verified report."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Notebook 04: selected BigQuery exports -> inspection -> saved Bronze.
    frames, inspection = access.inspect_inputs(spark, dbutils)
    bronze = access.create_bronze_tables(spark, frames)

    # Notebook 05: retain the original GKG -> Events -> Mentions processing order.
    gkg = cleaning.clean_gkg(bronze["gkg"], access.EXPECTED_ROW_COUNTS["gkg"])
    access.write_silver_table(spark, "gkg", bronze["gkg"], gkg)
    events = cleaning.clean_events(bronze["events"])
    access.write_silver_table(spark, "events", bronze["events"], events)
    mentions = cleaning.clean_mentions(bronze["mentions"])
    access.write_silver_table(spark, "mentions", bronze["mentions"], mentions)

    # Notebook 06: validate saved tables, then write and verify validation artifacts.
    checks, unmatched = validation.validate_silver_tables(spark)
    report = validation.write_validation_results(
        spark, dbutils, checks, unmatched, inspection
    )
    print("Phase 2 processing complete:", report["artifacts"])
    return {"silver_tables": access.SILVER_TABLES.copy(), "validation_report": report}
