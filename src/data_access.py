""" Notebook 04/05: Phase 2 selected-input inspection, Bronze, and table persistence.
The manifest describes selected BigQuery exports, not complete GDELT schemas.
Batch constants retain the original Phase 2 expectations. For isolated testing,
redirect BRONZE_TABLES, SILVER_TABLES and REPORT_ROOT before run_pipeline().
Imports do not read or write data. Source cell numbers include Markdown cells.
"""

from functools import reduce
from pyspark.sql import functions as F

# Batch contract from Notebook 04 cells 2-4 and Notebook 06 cell 2.
BATCH_START_UTC = "2026-09-17 00:00:00"

BATCH_END_UTC = "2026-10-18 00:00:00"  # Exclusive

BATCH_ID = "20260917_20261018_v1"

CATALOG = "workspace"

SCHEMA = "gdelt"

LANDING_ROOT = "/Volumes/workspace/gdelt/landing/bigquery"

TABLE_SUFFIX = "20260917_20261018_v1"

BRONZE_TABLES = {
    "events": (f"{CATALOG}.{SCHEMA}." f"bronze_bq_events_{TABLE_SUFFIX}"),
    "mentions": (f"{CATALOG}.{SCHEMA}." f"bronze_bq_mentions_{TABLE_SUFFIX}"),
    "gkg": (f"{CATALOG}.{SCHEMA}." f"bronze_bq_gkg_{TABLE_SUFFIX}"),
}

PHYSICAL_FILE_COUNTS = {"events": 4, "mentions": 5, "gkg": 14}

SELECTED_FILE_COUNTS = {"events": 4, "mentions": 4, "gkg": 13}

EXPECTED_ROW_COUNTS = {"events": 5_033_644, "mentions": 12_373_559, "gkg": 8_331_004}

EXPECTED_DAYS = 31

EXPECTED_INTERVALS = 2_976

KNOWN_SOURCE_GAPS = {"mentions": set(), "gkg": {"2026-09-17 13:00:00"}}
# RAW_COLUMNS means selected exported fields, not the complete GDELT schema.
RAW_COLUMNS = {
    "events": [
        "GLOBALEVENTID",
        "SQLDATE",
        "DATEADDED",
        "EventCode",
        "EventRootCode",
        "QuadClass",
        "GoldsteinScale",
        "Actor1CountryCode",
        "Actor2CountryCode",
        "Actor1Type1Code",
        "Actor2Type1Code",
        "ActionGeo_CountryCode",
        "ActionGeo_Lat",
        "ActionGeo_Long",
        "NumMentions",
        "NumSources",
        "AvgTone",
        "SOURCEURL",
    ],
    "mentions": [
        "GLOBALEVENTID",
        "EventTimeDate",
        "MentionTimeDate",
        "MentionType",
        "MentionSourceName",
        "MentionIdentifier",
        "Confidence",
        "InRawText",
        "MentionDocLen",
        "MentionDocTone",
        "MentionDocTranslationInfo",
    ],
    "gkg": [
        "GKGRECORDID",
        "DATE",
        "SourceCommonName",
        "DocumentIdentifier",
        "V2Themes",
        "V2Tone",
    ],
}
FILE_MANIFEST = {
    "events": [
        f"{LANDING_ROOT}/events/" "bq-results-20261019-012723-1792373252339.csv",
        f"{LANDING_ROOT}/events/" "bq-results-20261019-012901-1792373351396.csv",
        f"{LANDING_ROOT}/events/" "bq-results-20261019-013017-1792373428806.csv",
        f"{LANDING_ROOT}/events/" "bq-results-20261019-013132-1792373507679.csv",
    ],
    "mentions": [
        f"{LANDING_ROOT}/mentions/" "bq-results-20261019-011712-1792372646940.csv",
        f"{LANDING_ROOT}/mentions/" "bq-results-20261019-011930-1792372783012.csv",
        f"{LANDING_ROOT}/mentions/" "bq-results-20261020-161906-1792513169059.csv",
        f"{LANDING_ROOT}/mentions/" "bq-results-20261019-012333-1792373031810.csv",
    ],
    "gkg": [
        f"{LANDING_ROOT}/gkg/" "bq-results-20261019-013431-1792376193423.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261019-021821-1792376333947.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261019-022250-1792378689175.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261019-150717-1792426921939.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261020-032415-1792466678889.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261020-032825-1792466929998.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261020-033200-1792467132222.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261020-034543-1792467946848.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261020-035444-1792470312428.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261020-042729-1792470477093.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261020-043043-1792470669072.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261020-043316-1792470808015.csv",
        f"{LANDING_ROOT}/gkg/" "bq-results-20261020-044435-1792471496644.csv",
    ],
}

VERIFIED_DUPLICATE_COPIES = {
    (f"{LANDING_ROOT}/mentions/" "bq-results-20261019-011930-1792372898282.csv"): (
        f"{LANDING_ROOT}/mentions/" "bq-results-20261019-011930-1792372783012.csv"
    ),
    (f"{LANDING_ROOT}/gkg/" "bq-results-20261019-150717-1792427139198.csv"): (
        f"{LANDING_ROOT}/gkg/" "bq-results-20261019-150717-1792426921939.csv"
    ),
}
CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
    "encoding": "UTF-8",
    "quote": '"',
    "escape": '"',
    "multiLine": "true",
    "mode": "FAILFAST",
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
}
COVERAGE_STATUS = {
    "events": "validated_31_of_31_event_dates",
    "mentions": "validated_2976_of_2976_event_intervals",
    "gkg": ("validated_2975_of_2976_intervals_" "known_gap_20260917T130000Z"),
}
SILVER_TABLES = {
    "events": (f"{CATALOG}.{SCHEMA}." f"silver_bq_events_{TABLE_SUFFIX}"),
    "mentions": (f"{CATALOG}.{SCHEMA}." f"silver_bq_mentions_{TABLE_SUFFIX}"),
    "gkg": (f"{CATALOG}.{SCHEMA}." f"silver_bq_gkg_{TABLE_SUFFIX}"),
}
REPORT_ROOT = "/Volumes/workspace/gdelt/landing/reports"

EXPECTED_BRONZE_COLUMNS = {"events": 21, "mentions": 14, "gkg": 9}

EXPECTED_SILVER_COLUMNS = {"events": 61, "mentions": 46, "gkg": 56}

EXPECTED_SOURCE_FILES = {"events": 4, "mentions": 4, "gkg": 13}

# Notebook 04 — Input inspection and Bronze creation.

def inspect_inputs(spark, dbutils):
    """Notebook 04 cells 5-9: inspect selected exports and return frames plus coverage.

    All original input assertions remain. File copies are excluded by the
    existing manifest; this does not perform new duplicate detection."""

    def normalize_volume_path(path):
        """Remove the optional dbfs: prefix returned by dbutils."""
        return path.removeprefix("dbfs:")

    physical_files = {}

    for dataset in ["events", "mentions", "gkg"]:
        directory = f"{LANDING_ROOT}/{dataset}"

        physical_files[dataset] = {
            normalize_volume_path(item.path)
            for item in dbutils.fs.ls(directory)
            if item.path.lower().endswith(".csv")
        }

    all_selected_paths = []

    for dataset in ["events", "mentions", "gkg"]:
        physical_set = physical_files[dataset]
        selected_paths = FILE_MANIFEST[dataset]
        selected_set = set(selected_paths)

        assert len(physical_set) == PHYSICAL_FILE_COUNTS[dataset], (
            f"{dataset}: expected "
            f"{PHYSICAL_FILE_COUNTS[dataset]} physical files, "
            f"found {len(physical_set)}"
        )

        assert len(selected_paths) == SELECTED_FILE_COUNTS[dataset], (
            f"{dataset}: expected "
            f"{SELECTED_FILE_COUNTS[dataset]} selected files, "
            f"found {len(selected_paths)}"
        )

        assert len(selected_paths) == len(
            selected_set
        ), f"{dataset}: duplicate path exists inside FILE_MANIFEST"

        missing_files = selected_set - physical_set

        assert not missing_files, (
            f"{dataset}: manifest files missing from volume: "
            f"{sorted(missing_files)}"
        )

        excluded_for_dataset = {
            duplicate_path
            for duplicate_path in VERIFIED_DUPLICATE_COPIES
            if f"/{dataset}/" in duplicate_path
        }

        assert selected_set.isdisjoint(
            excluded_for_dataset
        ), f"{dataset}: verified duplicate entered FILE_MANIFEST"

        # Confirm that the retained counterpart is selected.
        for duplicate_path in excluded_for_dataset:
            retained_path = VERIFIED_DUPLICATE_COPIES[duplicate_path]

            assert (
                duplicate_path in physical_set
            ), f"Verified duplicate file is not present: {duplicate_path}"

            assert (
                retained_path in selected_set
            ), f"Retained counterpart is not selected: {retained_path}"

        all_selected_paths.extend(selected_paths)

    assert len(all_selected_paths) == 21

    assert len(all_selected_paths) == len(set(all_selected_paths))

    def read_raw_csv(path):
        """Read a selected BigQuery CSV, retaining exported values as strings."""
        reader = spark.read

        for option_name, option_value in CSV_OPTIONS.items():
            reader = reader.option(option_name, option_value)

        return reader.csv(path)

    validated_frames = {}

    for dataset in ["events", "mentions", "gkg"]:
        frames = []

        for path in FILE_MANIFEST[dataset]:
            frame = read_raw_csv(path)

            assert frame.columns == RAW_COLUMNS[dataset], (
                f"{dataset}: unexpected columns in {path}\n"
                f"Expected: {RAW_COLUMNS[dataset]}\n"
                f"Actual:   {frame.columns}"
            )

            non_string_columns = [
                (column_name, data_type)
                for column_name, data_type in frame.dtypes
                if data_type != "string"
            ]

            assert not non_string_columns, (
                f"{dataset}: non-string raw columns in {path}: " f"{non_string_columns}"
            )

            frame_with_provenance = (
                frame.select(*RAW_COLUMNS[dataset])
                .withColumn("_source_file", F.lit(path))
                .withColumn("_batch_id", F.lit(BATCH_ID))
            )

            frames.append(frame_with_provenance)

        validated_frames[dataset] = reduce(
            lambda left, right: left.unionByName(right), frames
        )

        expected_output_columns = RAW_COLUMNS[dataset] + ["_source_file", "_batch_id"]

        assert validated_frames[dataset].columns == expected_output_columns
    events_bronze = validated_frames["events"]

    event_date = F.when(
        F.col("SQLDATE").rlike(r"^[0-9]{8}$"),
        F.try_to_timestamp(F.col("SQLDATE"), F.lit("yyyyMMdd")).cast("date"),
    )

    events_dates = events_bronze.select(event_date.alias("event_date"))

    events_summary = (
        events_dates.agg(
            F.count("*").alias("rows"),
            F.sum(F.col("event_date").isNull().cast("long")).alias("invalid_dates"),
            F.sum(
                (
                    F.col("event_date").isNotNull()
                    & (
                        (F.col("event_date") < F.lit("2026-09-17").cast("date"))
                        | (F.col("event_date") >= F.lit("2026-10-18").cast("date"))
                    )
                ).cast("long")
            ).alias("outside_window"),
        )
        .first()
        .asDict()
    )

    assert events_summary["rows"] == EXPECTED_ROW_COUNTS["events"]

    assert events_summary["invalid_dates"] == 0

    assert events_summary["outside_window"] == 0

    expected_event_dates = spark.sql("""
        SELECT explode(
            sequence(
                DATE '2026-09-17',
                DATE '2026-10-17',
                INTERVAL 1 DAY
            )
        ) AS event_date
    """)

    observed_event_dates = (
        events_dates.filter(F.col("event_date").isNotNull())
        .select("event_date")
        .distinct()
    )

    missing_event_dates = expected_event_dates.join(
        observed_event_dates, "event_date", "left_anti"
    ).orderBy("event_date")

    observed_date_count = observed_event_dates.count()

    missing_date_count = missing_event_dates.count()

    assert observed_date_count == EXPECTED_DAYS

    assert missing_date_count == 0
    expected_intervals = spark.sql("""
        SELECT explode(
            sequence(
                TIMESTAMP '2026-09-17 00:00:00',
                TIMESTAMP '2026-10-17 23:45:00',
                INTERVAL 15 MINUTES
            )
        ) AS interval_utc
    """)

    start_timestamp = F.to_timestamp(F.lit(BATCH_START_UTC))

    end_timestamp = F.to_timestamp(F.lit(BATCH_END_UTC))

    def validate_interval_coverage(dataset, raw_timestamp_column):
        source = validated_frames[dataset]

        parsed = source.select(
            F.when(
                F.col(raw_timestamp_column).rlike(r"^[0-9]{14}$"),
                F.try_to_timestamp(
                    F.col(raw_timestamp_column), F.lit("yyyyMMddHHmmss")
                ),
            ).alias("interval_utc")
        )

        summary = (
            parsed.agg(
                F.count("*").alias("rows"),
                F.sum(F.col("interval_utc").isNull().cast("long")).alias(
                    "invalid_timestamps"
                ),
                F.sum(
                    (
                        F.col("interval_utc").isNotNull()
                        & (
                            (F.col("interval_utc") < start_timestamp)
                            | (F.col("interval_utc") >= end_timestamp)
                        )
                    ).cast("long")
                ).alias("outside_window"),
                F.sum(
                    (
                        F.col("interval_utc").isNotNull()
                        & (
                            (F.minute("interval_utc") % 15 != 0)
                            | (F.second("interval_utc") != 0)
                        )
                    ).cast("long")
                ).alias("off_grid"),
            )
            .first()
            .asDict()
        )

        assert summary["rows"] == EXPECTED_ROW_COUNTS[dataset]
        assert summary["invalid_timestamps"] == 0
        assert summary["outside_window"] == 0
        assert summary["off_grid"] == 0

        observed_intervals = (
            parsed.filter(
                (F.col("interval_utc") >= start_timestamp)
                & (F.col("interval_utc") < end_timestamp)
            )
            .select("interval_utc")
            .distinct()
        )

        missing_intervals = expected_intervals.join(
            observed_intervals, "interval_utc", "left_anti"
        ).orderBy("interval_utc")

        observed_count = observed_intervals.count()
        missing_rows = missing_intervals.collect()

        missing_labels = {
            row["interval_utc"].strftime("%Y-%m-%d %H:%M:%S") for row in missing_rows
        }

        assert missing_labels == KNOWN_SOURCE_GAPS[dataset], (
            f"{dataset}: unexpected missing intervals. "
            f"Expected {sorted(KNOWN_SOURCE_GAPS[dataset])}, "
            f"found {sorted(missing_labels)}"
        )

        assert observed_count + len(missing_labels) == EXPECTED_INTERVALS

        return {
            "rows": summary["rows"],
            "observed_intervals": observed_count,
            "missing_intervals": sorted(missing_labels),
            "invalid_timestamps": summary["invalid_timestamps"],
            "outside_window": summary["outside_window"],
            "off_grid": summary["off_grid"],
        }

    mentions_coverage = validate_interval_coverage("mentions", "EventTimeDate")

    gkg_coverage = validate_interval_coverage("gkg", "DATE")
    bronze_frames = {}

    for dataset in ["events", "mentions", "gkg"]:
        bronze_frames[dataset] = validated_frames[dataset].withColumn(
            "_coverage_status", F.lit(COVERAGE_STATUS[dataset])
        )

        expected_columns = RAW_COLUMNS[dataset] + [
            "_source_file",
            "_batch_id",
            "_coverage_status",
        ]

        assert bronze_frames[dataset].columns == expected_columns

    # Pass the actual Notebook 04 results forward instead of retyping coverage in Notebook 06.
    inspection = {
        "input_manifest_summary": {
            "physical_csv_files": sum(map(len, physical_files.values())),
            "verified_duplicate_copies_excluded": len(VERIFIED_DUPLICATE_COPIES),
            "selected_files": {key: len(paths) for key, paths in FILE_MANIFEST.items()},
            "source": "Notebook 04 checks executed by inspect_inputs",
        },
        "coverage": {
            "source": "Notebook 04 coverage checks executed by inspect_inputs",
            "events": {
                "field": "SQLDATE",
                "expected_days": EXPECTED_DAYS,
                "days_with_rows": observed_date_count,
            },
            "mentions": {
                "field": "EventTimeDate",
                "expected_intervals": EXPECTED_INTERVALS,
                "intervals_with_rows": mentions_coverage["observed_intervals"],
            },
            "gkg": {
                "field": "DATE",
                "expected_intervals": EXPECTED_INTERVALS,
                "intervals_with_rows": gkg_coverage["observed_intervals"],
                "intervals_without_rows": [
                    value.replace(" ", "T") + "Z"
                    for value in gkg_coverage["missing_intervals"]
                ],
            },
            "interpretation": (
                "Interval presence does not prove that every source "
                "record was extracted. Missing coverage is not zero attention."
            ),
        },
    }
    return bronze_frames, inspection



def create_bronze_tables(spark, bronze_frames):
    """Notebook 04 cell 10 / Notebook 05 cell 2: create or reuse Bronze, check it, then read it."""
    bronze_results = {}

    for dataset in ["events", "mentions", "gkg"]:
        source = bronze_frames[dataset]
        table_name = BRONZE_TABLES[dataset]
        expected_rows = EXPECTED_ROW_COUNTS[dataset]

        if not spark.catalog.tableExists(table_name):
            (source.write.format("delta").mode("error").saveAsTable(table_name))
            action = "CREATED"
        else:
            action = "EXISTING"

        saved = spark.table(table_name)
        saved_rows = saved.count()

        assert saved_rows == expected_rows, (
            f"{dataset}: expected {expected_rows:,} saved rows, "
            f"found {saved_rows:,}"
        )

        assert (
            saved.dtypes == source.dtypes
        ), f"{dataset}: saved schema differs from source schema"

        saved_batch_ids = {
            row["_batch_id"] for row in saved.select("_batch_id").distinct().collect()
        }

        assert saved_batch_ids == {
            BATCH_ID
        }, f"{dataset}: unexpected batch IDs: {saved_batch_ids}"

        saved_source_files = {
            row["_source_file"]
            for row in saved.select("_source_file").distinct().collect()
        }

        assert saved_source_files == set(
            FILE_MANIFEST[dataset]
        ), f"{dataset}: saved source-file manifest does not match"

        detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first()

        assert (
            detail["format"] == "delta"
        ), f"{dataset}: table is not stored in Delta format"

        bronze_results[dataset] = {
            "table": table_name,
            "action": action,
            "rows": saved_rows,
            "columns": len(saved.columns),
            "source_files": len(saved_source_files),
            "format": detail["format"],
        }

    # Read saved Bronze, including reused tables, as Notebook 05 cell 2 does.
    bronze = {}

    for dataset in ["events", "mentions", "gkg"]:
        table_name = BRONZE_TABLES[dataset]

        assert spark.catalog.tableExists(
            table_name
        ), f"Missing Bronze table: {table_name}"

        frame = spark.table(table_name)
        row_count = frame.count()

        assert row_count == EXPECTED_ROW_COUNTS[dataset], (
            f"{dataset}: expected "
            f"{EXPECTED_ROW_COUNTS[dataset]:,} rows, "
            f"found {row_count:,}"
        )

        assert len(frame.columns) == EXPECTED_BRONZE_COLUMNS[dataset], (
            f"{dataset}: expected "
            f"{EXPECTED_BRONZE_COLUMNS[dataset]} columns, "
            f"found {len(frame.columns)}"
        )

        batch_ids = {
            row["_batch_id"] for row in frame.select("_batch_id").distinct().collect()
        }

        assert batch_ids == {BATCH_ID}, f"{dataset}: unexpected batch IDs: {batch_ids}"

        bronze[dataset] = frame

    return bronze

# Notebook 05 — Silver persistence and saved-table checks.

def write_silver_table(spark, dataset, bronze_frame, silver_frame):
    """Notebook 05 cells 6-9: create or reuse one Silver table and validate storage.

    Existing tables are never overwritten. A schema/count check does not
    establish full value equality with a newly evaluated transformation."""
    table_name = SILVER_TABLES[dataset]
    if not spark.catalog.tableExists(table_name):
        silver_frame.write.format("delta").mode("error").saveAsTable(table_name)

    # Check the saved table, not only the proposed transformation.
    saved = spark.table(table_name)
    assert (
        saved.count() == EXPECTED_ROW_COUNTS[dataset]
    ), f"{dataset}: unexpected saved Silver row count"
    assert (
        saved.dtypes == silver_frame.dtypes
    ), f"{dataset}: saved Silver schema differs from transformation schema"
    assert (
        saved.select(*bronze_frame.columns).dtypes == bronze_frame.dtypes
    ), f"{dataset}: original Bronze column names, order or types changed"
    source_files = {
        row["_source_file"] for row in saved.select("_source_file").distinct().collect()
    }
    assert len(source_files) == EXPECTED_SOURCE_FILES[dataset]

    # Notebook 05 checks GKG's batch here; Notebook 06 later checks all three saved Silver batches.
    if dataset == "gkg":
        batch_ids = {
            row["_batch_id"] for row in saved.select("_batch_id").distinct().collect()
        }
        assert batch_ids == {BATCH_ID}

    assert spark.catalog.tableExists(table_name)  # 05 cell 9
    assert spark.sql(f"DESCRIBE DETAIL {table_name}").first()["format"] == "delta"
    return saved
