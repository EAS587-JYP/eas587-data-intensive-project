## Processed GDELT Data
## Overview
This directory documents the processed datasets produced during Phase 2 of our EAS 587 project, The Attention Span of the World.
The complete processed datasets are stored as Delta tables in Databricks rather than committed to GitHub because of their size.

## Processed Datasets
The validated Silver tables are stored in the workspace.gdelt schema.
Dataset	Databricks Table	Rows	Columns
Events	workspace.gdelt.silver_bq_events_20260917_20261018_v1	5,033,644	61
Mentions	workspace.gdelt.silver_bq_mentions_20260917_20261018_v1	12,373,559	46
GKG	workspace.gdelt.silver_bq_gkg_20260917_20261018_v1	8,331,004	56
Total: 25,738,207 records.
The datasets cover September 17, 2026, through October 18, 2026 (exclusive).

## Processing
The processing pipeline uses Databricks, PySpark, and Delta Lake.
1. Inspect the selected BigQuery CSV exports.
2. Create Bronze tables preserving the exported data.
3. Transform and validate fields to produce Silver tables.
4. Add data quality indicators without deleting original records.
5. Validate row counts, schemas, missing values, timestamps, and cross-table relationships.

All original records are preserved during Bronze-to-Silver processing.

## Accessing the Data
The full Silver datasets are available in the project's Databricks workspace to users with the appropriate permissions.

Example:
events = spark.table(
    "workspace.gdelt.silver_bq_events_20260917_20261018_v1"
)

mentions = spark.table(
    "workspace.gdelt.silver_bq_mentions_20260917_20261018_v1"
)

gkg = spark.table(
    "workspace.gdelt.silver_bq_gkg_20260917_20261018_v1"
)

## Reproducibility
The Bronze ingestion, Silver transformation, and validation notebooks document the processing workflow.

The full processed datasets are not included in this repository due to storage constraints. Reproducing the pipeline requires access to the selected BigQuery exports and the Databricks processing environment.

Small representative samples may be included separately when appropriate.
