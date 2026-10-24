# The Attention Span of the World
Semester project for EAS 587: Data Intensive Computing

## Project Overview 

This project studies how global media attention changes following major real-world events.
We use GDELT 2.0 to examine how news coverage rises, reaches a pick and declines over time.

We initally focus on:
- Technology announcements
- Scientific discoveries
- Corporate events
- Politics differ

## Research Question

**How do the attention lifespans of technology announcements, scientific discoveries, corporate events, and politics differ in global news coverage?**

# Supporting questions include:
* How concentrated is media coverage across events?
* How quickly do recorded mentions appear after event registration?
* How does source diversity relate to mention volume?
* How can we identify sustained coverage and renewed attention over time?

## Datasets 

Primary data set is **GDELT 2.0**.

Core data: 
- **Global Knowledge Graph(GKG)** additional themes and contextual information when useful

Supporting data:
- **Events** information about detected real-world events
- **Mentions** records of media mentions associated with events

## Data Collection

Our Phase 2 dataset covers September 17, 2026, through October 18, 2026 (exclusive), representing 31 days of data.

Dataset	    Records
Events	    5,033,644
Mentions	12,373,559
GKG	        8,331,004
Total	    25,738,207

We exported selected columns from GDELT's BigQuery tables to Google Drive.
The initial collection contained 23 CSV files. After identifying and excluding two duplicate file copies, we processed 21 files.
Raw data is excluded from GitHub because of its size.

## Data Pipeline
Our Phase 2 processing workflow is:
GDELT 2.0 / BigQuery
-> Export selected data to Google Drive
-> Inspect CSV files and verify coverage
-> Create Bronze Delta tables
-> Transform and validate data using PySpark
-> Create Silver Delta tables
-> Run data quality checks
-> Perform exploratory data analysis
-> Calculate initial media attention statistics

The main processing environment is Databricks Free Edition, using PySpark and Delta Lake.

We also used DuckDB and compressed Parquet files for preliminary exploration.

## Bronze Layer
Preserves the exported source data and adds provenance information.

## Silver Layer
Creates typed and validated fields, parses timestamps, and adds data quality flags while preserving all original records.

## Validation
Checks row counts, missing values, duplicate indicators, temporal coverage, and relationships between datasets.
No records were removed during the Bronze-to-Silver transformation.

## Exploratory Data Analysis
Our Phase 2 analysis covers:

# Events
* Event categories and CAMEO classifications
* Tone distributions
* Missing values and data quality
# Mentions
* Mention counts per event
* Coverage concentration
* Registration-to-mention lag
* Source diversity and observed coverage spans
# GKG
* Document volume over time
* Tone distributions
* Theme frequencies and document characteristics
# Initial Findings
* Approximately 76.47% of event IDs represented in Mentions appear in exactly one exported Mention row.
* The top 1% of represented event IDs account for approximately 29.38% of exported Mention rows.
* The median nonnegative registration-to-mention lag is 0.75 hours.
* GKG provides additional thematic and tone information for future cross-table analysis.
Registration-to-mention lag should not be interpreted as the actual lifespan of a news story.

## Phase 3 Plans
In Phase 3, we plan to:
* Build event-level media attention timelines.
* Measure observed coverage duration, peaks, decay, and renewed attention.
* Compare attention patterns across event categories.
* Investigate relationships among attention, tone, themes, and source diversity.
* Extend the observation period to better capture delayed returns and long-term coverage.

## Data Access
- Full raw datasets and processed Delta tables are not stored in this repository.

## Follow the instruction to download the data in data/raw/download_instructions.md 

## Repository Structure

```text
eas587-data-intensive-project/
│
├── README.md
├── requirements.txt
├── research_plan.md
├── phase2_report.md
|
├── colab notebooks/
|   ├── GDELT_Events_Preliminary_LocalParquet.ipynb
|   ├── GDELT_GKG_Preliminary_LocalParquet.ipynb
|   └── GDELT_Mentions_Preliminary_LocalParquet.ipynb
|
├── graph
│
├── data/
│   ├── samples/
|   |   ├── events/
|   |   ├── gkg/
|   |   └── mentions/
│   └── raw/
|       └── download_instructions.md
|
├── slide/
|   ├──Phase 1 slide.pdf
|
├── .venv/ 
│
└── src/
    ├── data_access.py
    ├── data_cleaning.py
    ├── data_validation.py
    ├── pipeline.py

# pip install -r requirements.txt