"""Notebook 06: saved-Silver validation and reporting; no additional cleaning.

Coverage is supplied by the current run of Notebook 04 input inspection.
Phase 2 validation expectations and report literals remain unchanged.
Source: Notebook 06_bigquery_validation_final.ipynb; cells include Markdown.
"""

from datetime import datetime, timezone
import json
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType
from . import data_access as phase2


def validate_silver_tables(spark):
    """Notebook 06 cells 2-4 and 6: inspect saved tables and return validation results.

    Temporal coverage is checked by inspect_inputs, not recomputed here.
    The Events-Mentions join is diagnostic; unmatched rows remain in Silver."""
    bronze = {}

    silver = {}

    for dataset in ["events", "mentions", "gkg"]:
        bronze_table = phase2.BRONZE_TABLES[dataset]
        silver_table = phase2.SILVER_TABLES[dataset]

        assert spark.catalog.tableExists(
            bronze_table
        ), f"Missing Bronze table: {bronze_table}"
        assert spark.catalog.tableExists(
            silver_table
        ), f"Missing Silver table: {silver_table}"

        bronze_frame = spark.table(bronze_table)
        silver_frame = spark.table(silver_table)

        bronze_rows = bronze_frame.count()
        silver_rows = silver_frame.count()

        assert bronze_rows == phase2.EXPECTED_ROW_COUNTS[dataset]
        assert silver_rows == phase2.EXPECTED_ROW_COUNTS[dataset]
        assert bronze_rows == silver_rows

        assert len(bronze_frame.columns) == phase2.EXPECTED_BRONZE_COLUMNS[dataset]
        assert len(silver_frame.columns) == phase2.EXPECTED_SILVER_COLUMNS[dataset]

        # Confirm that every Bronze column remains present in Silver
        # with the same name, order, and data type.
        assert (
            silver_frame.select(*bronze_frame.columns).dtypes == bronze_frame.dtypes
        ), f"{dataset}: original Bronze schema changed in Silver"

        source_file_count = silver_frame.select("_source_file").distinct().count()

        assert source_file_count == phase2.EXPECTED_SOURCE_FILES[dataset]

        batch_ids = {
            row["_batch_id"]
            for row in (silver_frame.select("_batch_id").distinct().collect())
        }

        assert batch_ids == {phase2.BATCH_ID}

        bronze_detail = spark.sql(f"DESCRIBE DETAIL {bronze_table}").first()

        silver_detail = spark.sql(f"DESCRIBE DETAIL {silver_table}").first()

        assert bronze_detail["format"] == "delta"
        assert silver_detail["format"] == "delta"

        bronze[dataset] = bronze_frame
        silver[dataset] = silver_frame
    dataset_quality_reports = {}

    for dataset in ["events", "mentions", "gkg"]:
        frame = silver[dataset]
        total_rows = phase2.EXPECTED_ROW_COUNTS[dataset]

        missing_expressions = []
        missing_metadata = []

        for index, field in enumerate(frame.schema.fields):
            column_name = field.name
            data_type = field.dataType.simpleString()
            alias = f"missing_{index}"

            if isinstance(field.dataType, StringType):
                missing_condition = F.col(column_name).isNull() | (
                    F.trim(F.col(column_name)) == ""
                )
            else:
                missing_condition = F.col(column_name).isNull()

            missing_expressions.append(
                F.sum(F.when(missing_condition, F.lit(1)).otherwise(F.lit(0))).alias(
                    alias
                )
            )

            missing_metadata.append(
                {"alias": alias, "column_name": column_name, "data_type": data_type}
            )

        flag_columns = [
            field.name
            for field in frame.schema.fields
            if (
                isinstance(field.dataType, BooleanType) and field.name.endswith("_flag")
            )
        ]

        flag_expressions = [
            F.sum(F.coalesce(F.col(column_name), F.lit(False)).cast("long")).alias(
                f"flag_{index}"
            )
            for index, column_name in enumerate(flag_columns)
        ]

        aggregate_row = (
            frame.agg(
                F.count("*").alias("row_count"), *missing_expressions, *flag_expressions
            )
            .first()
            .asDict()
        )

        assert aggregate_row["row_count"] == total_rows

        missing_profile = []

        for item in missing_metadata:
            missing_count = int(aggregate_row[item["alias"]])

            missing_profile.append(
                {
                    "column_name": item["column_name"],
                    "data_type": item["data_type"],
                    "missing_count": missing_count,
                    "missing_percent": round(100.0 * missing_count / total_rows, 6),
                }
            )

        flag_profile = []

        for index, column_name in enumerate(flag_columns):
            true_count = int(aggregate_row[f"flag_{index}"])

            flag_profile.append(
                {
                    "flag_name": column_name,
                    "true_count": true_count,
                    "true_percent": round(100.0 * true_count / total_rows, 6),
                }
            )

        dataset_quality_reports[dataset] = {
            "row_count": total_rows,
            "schema": [
                {
                    "column_name": field.name,
                    "data_type": field.dataType.simpleString(),
                    "nullable": field.nullable,
                }
                for field in frame.schema.fields
            ],
            "missing_values": missing_profile,
            "quality_flags": flag_profile,
        }
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    events_df = silver["events"]

    mentions_df = silver["mentions"]

    events_lookup = events_df.select(
        "event_id",
        F.col("event_date").alias("events_table_event_date"),
        F.col("event_added_at_utc").alias("events_table_added_at_utc"),
        F.col("document_url").alias("event_document_url"),
    ).withColumn("event_found_flag", F.lit(True))

    event_key_check = events_lookup.agg(
        F.count("*").alias("event_rows"),
        F.countDistinct("event_id").alias("distinct_event_ids"),
    ).first()

    assert (
        event_key_check["event_rows"] == event_key_check["distinct_event_ids"]
    ), "Events event_id is not unique; the join could multiply Mention rows."

    mentions_with_events = (
        mentions_df.alias("m")
        .join(events_lookup.alias("e"), on="event_id", how="left")
        .withColumn(
            "event_found_flag", F.coalesce(F.col("event_found_flag"), F.lit(False))
        )
        .withColumn(
            "matched_event_timestamp_mismatch_flag",
            F.col("event_found_flag")
            & ~F.col("event_added_at_utc").eqNullSafe(
                F.col("events_table_added_at_utc")
            ),
        )
    )

    join_stats = (
        mentions_with_events.agg(
            F.count("*").alias("output_mentions"),
            F.sum(F.col("event_found_flag").cast("long")).alias("matched_mentions"),
            F.sum((~F.col("event_found_flag")).cast("long")).alias(
                "unmatched_mentions"
            ),
            F.countDistinct(F.when(F.col("event_found_flag"), F.col("event_id"))).alias(
                "matched_distinct_event_ids"
            ),
            F.countDistinct(
                F.when(~F.col("event_found_flag"), F.col("event_id"))
            ).alias("unmatched_distinct_event_ids"),
            F.sum(F.col("matched_event_timestamp_mismatch_flag").cast("long")).alias(
                "matched_rows_with_event_timestamp_mismatch"
            ),
        )
        .first()
        .asDict()
    )

    input_mentions = mentions_df.count()

    assert (
        join_stats["output_mentions"] == input_mentions
    ), "The Events–Mentions join changed the Mention row count."

    matched_percent = (
        100.0 * join_stats["matched_mentions"] / input_mentions
        if input_mentions
        else 0.0
    )

    events_mentions_join_report = {
        "input_mentions": input_mentions,
        "output_mentions": join_stats["output_mentions"],
        "matched_mentions": join_stats["matched_mentions"],
        "unmatched_mentions": join_stats["unmatched_mentions"],
        "match_percent": round(matched_percent, 6),
        "matched_distinct_event_ids": (join_stats["matched_distinct_event_ids"]),
        "unmatched_distinct_event_ids": (join_stats["unmatched_distinct_event_ids"]),
        "matched_rows_with_event_timestamp_mismatch": (
            join_stats["matched_rows_with_event_timestamp_mismatch"]
        ),
    }

    unmatched_mentions = mentions_with_events.filter(
        ~F.col("event_found_flag")
    ).withColumn(
        "reference_time_position",
        F.when(F.col("event_added_at_utc").isNull(), F.lit("invalid_reference_time"))
        .when(
            F.col("event_added_at_utc") < F.to_timestamp(F.lit("2026-09-17 00:00:00")),
            F.lit("before_batch_start"),
        )
        .when(
            F.col("event_added_at_utc") >= F.to_timestamp(F.lit("2026-10-18 00:00:00")),
            F.lit("at_or_after_batch_end"),
        )
        .otherwise(F.lit("within_batch_window")),
    )
    gkg_df = silver["gkg"]

    events_df = silver["events"]

    def nonempty_url_rows(df):
        # Use the existing cleaned URL without further normalization.
        return df.select(F.col("document_url").alias("url")).filter(
            F.col("url").isNotNull() & (F.length(F.trim(F.col("url"))) > 0)
        )

    gkg_url_rows = nonempty_url_rows(gkg_df)

    events_url_rows = nonempty_url_rows(events_df)

    gkg_urls = gkg_url_rows.distinct()

    events_urls = events_url_rows.distinct()

    shared_urls = gkg_urls.join(events_urls, on="url", how="inner")

    gkg_url_count = gkg_urls.count()

    events_url_count = events_urls.count()

    shared_url_count = shared_urls.count()

    gkg_matching_rows = gkg_url_rows.join(
        events_urls, on="url", how="left_semi"
    ).count()

    events_matching_rows = events_url_rows.join(
        gkg_urls, on="url", how="left_semi"
    ).count()

    gkg_events_url_overlap_report = {
        "gkg_distinct_urls": gkg_url_count,
        "events_distinct_urls": events_url_count,
        "shared_distinct_urls": shared_url_count,
        "gkg_rows_with_matching_event_url": gkg_matching_rows,
        "event_rows_with_matching_gkg_url": events_matching_rows,
        "gkg_url_match_percent": (
            round(100.0 * shared_url_count / gkg_url_count, 6)
            if gkg_url_count
            else None
        ),
        "events_url_match_percent": (
            round(100.0 * shared_url_count / events_url_count, 6)
            if events_url_count
            else None
        ),
        "interpretation": (
            "Exact match on cleaned document_url only; "
            "not confirmed research-event equivalence."
        ),
    }

    assert shared_url_count <= min(
        gkg_url_count, events_url_count
    ), "Unexpected shared URL count."

    validation = {
        "datasets": dataset_quality_reports,
        "events_mentions_join": events_mentions_join_report,
        "gkg_events_url_overlap": gkg_events_url_overlap_report,
    }
    return validation, unmatched_mentions



def write_validation_results(
    spark, dbutils, validation, unmatched_mentions, inspection
):
    """ Notebook 06 cells 7-8: save run-specific JSON/CSV and verify both readbacks.

    Coverage comes from this run of 04 logic. CSV stays distributed in
    four part files, and reports never overwrite an existing report path."""
    dataset_quality_reports = validation["datasets"]
    events_mentions_join_report = validation["events_mentions_join"]
    gkg_events_url_overlap_report = validation["gkg_events_url_overlap"]
    # Explicit inputs replace the notebook's globals() readiness checks.
    assert dataset_quality_reports is not None
    assert events_mentions_join_report is not None
    assert unmatched_mentions is not None
    assert gkg_events_url_overlap_report is not None
    assert events_mentions_join_report["input_mentions"] == 12_373_559

    assert events_mentions_join_report["output_mentions"] == 12_373_559

    assert gkg_events_url_overlap_report["gkg_distinct_urls"] == 8_331_004

    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid.uuid4().hex[:8]
    )

    report_dir = f"{phase2.REPORT_ROOT}/full_month_{phase2.BATCH_ID}_{run_id}"

    json_path = f"{report_dir}/full_month_quality_report.json"

    csv_dir = f"{report_dir}/unmatched_mentions_csv"

    unmatched_time_summary = [
        row.asDict()
        for row in (
            unmatched_mentions.groupBy("reference_time_position")
            .agg(
                F.count("*").alias("mention_rows"),
                F.countDistinct("event_id").alias("distinct_event_ids"),
            )
            .orderBy("reference_time_position")
            .collect()
        )
    ]

    full_month_quality_report = {
        "run_id": run_id,
        "batch_id": phase2.BATCH_ID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection_window": {
            "start_inclusive": "2026-09-17T00:00:00Z",
            "end_exclusive": "2026-10-18T00:00:00Z",
            "days": 31,
            "note": (
                "This is the batch selection window, not a restriction "
                "on every timestamp column. DATEADDED and MentionTimeDate "
                "can extend beyond the window."
            ),
        },
        "input_manifest_summary": inspection["input_manifest_summary"],
        "row_policy": {
            "rows_removed_from_selected_inputs": 0,
            "policy": (
                "Preserve selected export rows and values; add parsed fields "
                "and quality flags. Retain missing optional metadata, "
                "source-ID conflicts, and repeated exported values."
            ),
        },
        "silver_tables": phase2.SILVER_TABLES.copy(),
        "datasets": dataset_quality_reports,
        "coverage": inspection["coverage"],
        "events_mentions_join": events_mentions_join_report,
        "unmatched_reference_time_summary": unmatched_time_summary,
        "gkg_events_url_overlap": gkg_events_url_overlap_report,
        "limitations": [
            "The cause of unmatched Event references remains unconfirmed.",
            "GKG source IDs are not unique in this export.",
            "Repeated Mention export values are not confirmed duplicate "
            "mentions because sentence and character-offset fields are absent.",
            "Exact URL matches do not establish research-event equivalence.",
            "Research-category classification and event clustering "
            "have not been implemented.",
            "Attention lifespan estimates require an explicit observation "
            "window and treatment of events continuing beyond that window.",
        ],
        "artifacts": {
            "quality_report_json": json_path,
            "unmatched_mentions_csv_directory": csv_dir,
        },
    }

    unmatched_export = unmatched_mentions.select(
        "event_id",
        F.date_format("event_added_at_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(
            "event_added_at_utc"
        ),
        F.date_format("mention_at_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(
            "mention_at_utc"
        ),
        "reference_time_position",
        "mention_source_name",
        "mention_identifier_clean",
    )

    dbutils.fs.mkdirs(report_dir)

    (
        unmatched_export.repartition(4)
        .write.mode("error")
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(csv_dir)
    )

    csv_readback_rows = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(csv_dir)
        .count()
    )

    expected_unmatched = events_mentions_join_report["unmatched_mentions"]

    assert (
        csv_readback_rows == expected_unmatched
    ), f"CSV count mismatch: {csv_readback_rows} != {expected_unmatched}"

    full_month_quality_report["artifact_validation"] = {
        "unmatched_csv_expected_rows": expected_unmatched,
        "unmatched_csv_readback_rows": csv_readback_rows,
        "unmatched_csv_row_count_pass": True,
    }

    report_text = json.dumps(
        full_month_quality_report, indent=2, ensure_ascii=False, allow_nan=False
    )

    dbutils.fs.put(json_path, report_text, overwrite=False)

    report_readback = json.loads(
        dbutils.fs.head(json_path, len(report_text.encode("utf-8")) + 1)
    )

    assert (
        report_readback == full_month_quality_report
    ), "JSON read-back contents do not match."

    return full_month_quality_report
