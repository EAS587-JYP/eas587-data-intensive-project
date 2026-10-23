"""Notebook 05: reusable Bronze-to-Silver transformations; retain rows and flag issues.

Input frames must provide the exported fields and provenance expected by 05.
The caller sets the Spark session timezone to UTC (run_pipeline does this).
No Phase 2 paths, manifest, or batch counts are defined here.
Source: 05_bigquery_silver_final.ipynb; cell numbers include Markdown.
"""

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_integer_validation(
    frame, raw_column, parsed_column, minimum=None, maximum=None
):
    """Add missing, parsed, and invalid columns for an integer."""
    missing_flag = f"missing_{raw_column}_flag"
    invalid_flag = f"invalid_{raw_column}_flag"

    missing_expression = F.col(raw_column).isNull() | (F.trim(F.col(raw_column)) == "")

    frame = frame.withColumn(missing_flag, missing_expression).withColumn(
        parsed_column, F.expr(f"try_cast(`{raw_column}` AS BIGINT)")
    )

    invalid_expression = (~F.col(missing_flag)) & F.col(parsed_column).isNull()

    if minimum is not None:
        invalid_expression = invalid_expression | (
            (~F.col(missing_flag)) & (F.col(parsed_column) < F.lit(minimum))
        )

    if maximum is not None:
        invalid_expression = invalid_expression | (
            (~F.col(missing_flag)) & (F.col(parsed_column) > F.lit(maximum))
        )

    return frame.withColumn(invalid_flag, invalid_expression)



def add_double_validation(frame, raw_column, parsed_column, minimum=None, maximum=None):
    """Add missing, parsed, and invalid columns for a double."""
    missing_flag = f"missing_{raw_column}_flag"
    invalid_flag = f"invalid_{raw_column}_flag"

    missing_expression = F.col(raw_column).isNull() | (F.trim(F.col(raw_column)) == "")

    frame = frame.withColumn(missing_flag, missing_expression).withColumn(
        parsed_column, F.expr(f"try_cast(`{raw_column}` AS DOUBLE)")
    )

    parsed = F.col(parsed_column)

    invalid_expression = (~F.col(missing_flag)) & (parsed.isNull() | F.isnan(parsed))

    if minimum is not None:
        invalid_expression = invalid_expression | (
            (~F.col(missing_flag)) & (parsed < F.lit(minimum))
        )

    if maximum is not None:
        invalid_expression = invalid_expression | (
            (~F.col(missing_flag)) & (parsed > F.lit(maximum))
        )

    return frame.withColumn(invalid_flag, invalid_expression)



def invalid_bounded_double(column_name, minimum, maximum):
    """Return a validation flag for a required bounded double."""
    column = F.col(column_name)

    return (~F.col("missing_tone_flag")) & (
        column.isNull()
        | F.isnan(column)
        | (column < F.lit(minimum))
        | (column > F.lit(maximum))
    )



def clean_gkg(bronze_gkg, expected_rows):
    """Notebook 05 cells 3-6: return GKG Silver fields without writing or removing rows.

    The caller supplies expected_rows for the retained base, tone and theme
    row-count assertions. Raw exported
    fields remain alongside the derived representations and flags."""
    gkg_input = bronze_gkg

    required_gkg_columns = {
        "GKGRECORDID",
        "DATE",
        "SourceCommonName",
        "DocumentIdentifier",
        "V2Themes",
        "V2Tone",
        "_source_file",
        "_batch_id",
        "_coverage_status",
    }

    missing_columns = required_gkg_columns - set(gkg_input.columns)

    assert (
        not missing_columns
    ), f"GKG Bronze is missing columns: {sorted(missing_columns)}"

    valid_gkg_id = F.coalesce(
        F.col("GKGRECORDID").rlike(r"^[0-9]{14}-T?[0-9]+$"), F.lit(False)
    )

    valid_gkg_timestamp = F.coalesce(F.col("DATE").rlike(r"^[0-9]{14}$"), F.lit(False))

    gkg_base = (
        gkg_input
        # Preserve GKGRECORDID and add a validated identifier.
        .withColumn("record_id", F.when(valid_gkg_id, F.col("GKGRECORDID")))
        # Parse the archive observation timestamp.
        .withColumn(
            "observed_at_utc",
            F.when(
                valid_gkg_timestamp,
                F.try_to_timestamp(F.col("DATE"), F.lit("yyyyMMddHHmmss")),
            ),
        )
        # Parse the timestamp prefix embedded in GKGRECORDID.
        .withColumn(
            "record_id_timestamp_utc",
            F.when(
                valid_gkg_id,
                F.try_to_timestamp(
                    F.substring(F.col("GKGRECORDID"), 1, 14), F.lit("yyyyMMddHHmmss")
                ),
            ),
        )
        # Create cleaned convenience columns while preserving raw columns.
        .withColumn(
            "source_name",
            F.when(
                F.col("SourceCommonName").isNull()
                | (F.trim(F.col("SourceCommonName")) == ""),
                F.lit(None).cast("string"),
            ).otherwise(F.col("SourceCommonName")),
        )
        .withColumn(
            "document_url",
            F.when(
                F.col("DocumentIdentifier").isNull()
                | (F.trim(F.col("DocumentIdentifier")) == ""),
                F.lit(None).cast("string"),
            ).otherwise(F.col("DocumentIdentifier")),
        )
        # Validate required identifiers and timestamps.
        .withColumn("invalid_id_flag", F.col("record_id").isNull())
        .withColumn("invalid_timestamp_flag", F.col("observed_at_utc").isNull())
        .withColumn(
            "id_timestamp_mismatch_flag",
            F.coalesce(
                F.col("record_id_timestamp_utc") != F.col("observed_at_utc"),
                F.lit(False),
            ),
        )
        .withColumn("missing_source_flag", F.col("source_name").isNull())
        .withColumn("missing_document_url_flag", F.col("document_url").isNull())
        # This key distinguishes conflicting rows that reuse a source ID.
        .withColumn(
            "record_candidate_key",
            F.sha2(
                F.to_json(
                    F.struct(F.col("GKGRECORDID"), F.col("DocumentIdentifier")),
                    {"ignoreNullFields": "false"},
                ),
                256,
            ),
        )
        # This fingerprint represents all six exported GKG values.
        .withColumn(
            "record_fingerprint",
            F.sha2(
                F.to_json(
                    F.struct(
                        *[
                            F.col(column_name)
                            for column_name in [
                                "GKGRECORDID",
                                "DATE",
                                "SourceCommonName",
                                "DocumentIdentifier",
                                "V2Themes",
                                "V2Tone",
                            ]
                        ]
                    ),
                    {"ignoreNullFields": "false"},
                ),
                256,
            ),
        )
    )

    gkg_source_id_stats = (
        gkg_base.filter(F.col("record_id").isNotNull())
        .groupBy("record_id")
        .agg(
            F.count("*").alias("source_id_group_size"),
            F.countDistinct("record_fingerprint").alias("source_id_distinct_versions"),
        )
    )

    gkg_base = (
        gkg_base.join(gkg_source_id_stats, on="record_id", how="left")
        .withColumn(
            "source_id_group_size", F.coalesce(F.col("source_id_group_size"), F.lit(0))
        )
        .withColumn(
            "source_id_distinct_versions",
            F.coalesce(F.col("source_id_distinct_versions"), F.lit(0)),
        )
        .withColumn("source_id_duplicate_flag", F.col("source_id_group_size") > 1)
        .withColumn("source_id_conflict_flag", F.col("source_id_distinct_versions") > 1)
    )

    gkg_base_summary = (
        gkg_base.agg(
            F.count("*").alias("output_rows"),
            F.sum(F.col("invalid_id_flag").cast("long")).alias("invalid_ids"),
            F.sum(F.col("invalid_timestamp_flag").cast("long")).alias(
                "invalid_timestamps"
            ),
            F.sum(F.col("id_timestamp_mismatch_flag").cast("long")).alias(
                "id_timestamp_mismatches"
            ),
            F.sum(F.col("missing_source_flag").cast("long")).alias("missing_sources"),
            F.sum(F.col("missing_document_url_flag").cast("long")).alias(
                "missing_document_urls"
            ),
            F.sum(F.col("source_id_duplicate_flag").cast("long")).alias(
                "rows_with_reused_source_ids"
            ),
            F.sum(F.col("source_id_conflict_flag").cast("long")).alias(
                "rows_with_source_id_conflicts"
            ),
            F.countDistinct("record_id").alias("distinct_valid_source_ids"),
            F.countDistinct("record_candidate_key").alias("distinct_candidate_keys"),
            F.countDistinct("record_fingerprint").alias("distinct_record_fingerprints"),
        )
        .first()
        .asDict()
    )

    assert (
        gkg_base_summary["output_rows"] == expected_rows
    ), "GKG row count changed during base transformation"

    # Notebook 05 cell 4: preserve the seven-component parsing expressions and flags.
    gkg_tone = (
        gkg_base
        # Distinguish a missing tone value from a malformed value.
        .withColumn(
            "missing_tone_flag",
            F.col("V2Tone").isNull() | (F.trim(F.col("V2Tone")) == ""),
        )
        # Preserve empty fields by using a negative split limit.
        .withColumn(
            "_tone_parts",
            F.when(~F.col("missing_tone_flag"), F.split(F.col("V2Tone"), ",", -1)),
        ).withColumn(
            "invalid_tone_structure_flag",
            (~F.col("missing_tone_flag")) & (F.size(F.col("_tone_parts")) != 7),
        )
        # V2Tone component 1: overall tone.
        .withColumn(
            "tone_parsed", F.expr("try_cast(element_at(_tone_parts, 1) AS DOUBLE)")
        )
        # V2Tone component 2: positive score.
        .withColumn(
            "positive_score", F.expr("try_cast(element_at(_tone_parts, 2) AS DOUBLE)")
        )
        # V2Tone component 3: negative score.
        .withColumn(
            "negative_score", F.expr("try_cast(element_at(_tone_parts, 3) AS DOUBLE)")
        )
        # V2Tone component 4: polarity.
        .withColumn(
            "polarity", F.expr("try_cast(element_at(_tone_parts, 4) AS DOUBLE)")
        )
        # V2Tone component 5: activity-reference density.
        .withColumn(
            "activity_reference_density",
            F.expr("try_cast(element_at(_tone_parts, 5) AS DOUBLE)"),
        )
        # V2Tone component 6: self/group-reference density.
        .withColumn(
            "self_group_reference_density",
            F.expr("try_cast(element_at(_tone_parts, 6) AS DOUBLE)"),
        )
        # V2Tone component 7: article word count.
        .withColumn(
            "word_count_parsed",
            F.expr("try_cast(element_at(_tone_parts, 7) AS BIGINT)"),
        )
    )

    gkg_tone = (
        gkg_tone.withColumn(
            "invalid_tone_parsed_flag",
            invalid_bounded_double("tone_parsed", -100.0, 100.0),
        )
        .withColumn(
            "invalid_positive_score_flag",
            invalid_bounded_double("positive_score", 0.0, 100.0),
        )
        .withColumn(
            "invalid_negative_score_flag",
            invalid_bounded_double("negative_score", 0.0, 100.0),
        )
        .withColumn(
            "invalid_polarity_flag", invalid_bounded_double("polarity", 0.0, 100.0)
        )
        .withColumn(
            "invalid_activity_reference_density_flag",
            invalid_bounded_double("activity_reference_density", 0.0, 100.0),
        )
        .withColumn(
            "invalid_self_group_reference_density_flag",
            invalid_bounded_double("self_group_reference_density", 0.0, 100.0),
        )
        .withColumn(
            "invalid_word_count_parsed_flag",
            (~F.col("missing_tone_flag"))
            & (F.col("word_count_parsed").isNull() | (F.col("word_count_parsed") < 0)),
        )
        .withColumn(
            "invalid_tone_flag",
            F.col("invalid_tone_structure_flag")
            | F.col("invalid_tone_parsed_flag")
            | F.col("invalid_positive_score_flag")
            | F.col("invalid_negative_score_flag")
            | F.col("invalid_polarity_flag")
            | F.col("invalid_activity_reference_density_flag")
            | F.col("invalid_self_group_reference_density_flag")
            | F.col("invalid_word_count_parsed_flag"),
        )
    )

    tone_flag_columns = [
        "missing_tone_flag",
        "invalid_tone_structure_flag",
        "invalid_tone_parsed_flag",
        "invalid_positive_score_flag",
        "invalid_negative_score_flag",
        "invalid_polarity_flag",
        "invalid_activity_reference_density_flag",
        "invalid_self_group_reference_density_flag",
        "invalid_word_count_parsed_flag",
        "invalid_tone_flag",
    ]

    tone_summary = (
        gkg_tone.agg(
            F.count("*").alias("output_rows"),
            *[
                F.sum(F.col(column_name).cast("long")).alias(column_name)
                for column_name in tone_flag_columns
            ],
        )
        .first()
        .asDict()
    )

    assert (
        tone_summary["output_rows"] == expected_rows
    ), "GKG row count changed during tone transformation"

    # Notebook 05 cell 5: keep occurrences, distinct names and invalid-entry evidence.
    empty_string_array = F.expr("CAST(array() AS ARRAY<STRING>)")

    gkg_themes = (
        gkg_tone.withColumn(
            "missing_themes_flag",
            F.col("V2Themes").isNull() | (F.trim(F.col("V2Themes")) == ""),
        )
        # Remove trailing semicolons before splitting.
        # Missing theme values become an empty array.
        .withColumn(
            "_theme_entries_raw",
            F.when(F.col("missing_themes_flag"), empty_string_array).otherwise(
                F.split(F.regexp_replace(F.col("V2Themes"), r";+$", ""), ";", -1)
            ),
        )
        # Parse each entry as THEME,CHARACTER_OFFSET.
        .withColumn(
            "_theme_entries_parsed",
            F.expr("""
                transform(
                    _theme_entries_raw,
                    x -> named_struct(
                        'raw_entry',
                        x,

                        'theme',
                        CASE
                            WHEN x RLIKE '^([^,;]+),([0-9]+)$'
                                 AND trim(
                                     regexp_extract(
                                         x,
                                         '^([^,;]+),([0-9]+)$',
                                         1
                                     )
                                 ) <> ''
                            THEN trim(
                                regexp_extract(
                                    x,
                                    '^([^,;]+),([0-9]+)$',
                                    1
                                )
                            )
                            ELSE CAST(NULL AS STRING)
                        END,

                        'character_offset',
                        CASE
                            WHEN x RLIKE '^([^,;]+),([0-9]+)$'
                            THEN try_cast(
                                regexp_extract(
                                    x,
                                    '^([^,;]+),([0-9]+)$',
                                    2
                                )
                                AS BIGINT
                            )
                            ELSE CAST(NULL AS BIGINT)
                        END,

                        'valid',
                        (
                            x RLIKE '^([^,;]+),([0-9]+)$'
                            AND trim(
                                regexp_extract(
                                    x,
                                    '^([^,;]+),([0-9]+)$',
                                    1
                                )
                            ) <> ''
                            AND try_cast(
                                regexp_extract(
                                    x,
                                    '^([^,;]+),([0-9]+)$',
                                    2
                                )
                                AS BIGINT
                            ) >= 0
                        )
                    )
                )
                """),
        )
        # Preserve invalid entries for later diagnosis.
        .withColumn(
            "invalid_theme_entries",
            F.expr("""
                filter(
                    _theme_entries_parsed,
                    x -> NOT x.valid
                )
                """),
        )
        # Preserve repeated valid theme mentions and their offsets.
        .withColumn(
            "theme_mentions",
            F.expr("""
                transform(
                    filter(
                        _theme_entries_parsed,
                        x -> x.valid
                    ),
                    x -> named_struct(
                        'theme', x.theme,
                        'character_offset', x.character_offset
                    )
                )
                """),
        )
        # Produce a sorted, unique list for category exploration.
        .withColumn(
            "themes",
            F.array_sort(
                F.array_distinct(
                    F.transform(F.col("theme_mentions"), lambda item: item["theme"])
                )
            ),
        )
        .withColumn("theme_mention_count", F.size(F.col("theme_mentions")))
        .withColumn("distinct_theme_count", F.size(F.col("themes")))
        .withColumn("invalid_theme_entry_count", F.size(F.col("invalid_theme_entries")))
        .withColumn("invalid_themes_flag", F.col("invalid_theme_entry_count") > 0)
        # Remove temporary working arrays.
        .drop("_theme_entries_raw", "_theme_entries_parsed")
    )

    theme_summary = (
        gkg_themes.agg(
            F.count("*").alias("output_rows"),
            F.sum(F.col("missing_themes_flag").cast("long")).alias(
                "rows_missing_themes"
            ),
            F.sum(F.col("invalid_themes_flag").cast("long")).alias(
                "rows_with_invalid_theme_entries"
            ),
            F.sum(F.col("invalid_theme_entry_count")).alias(
                "invalid_theme_entries_total"
            ),
            F.sum((F.col("theme_mention_count") > 0).cast("long")).alias(
                "rows_with_valid_themes"
            ),
            F.max(F.col("theme_mention_count")).alias("max_theme_mentions_per_row"),
            F.max(F.col("distinct_theme_count")).alias("max_distinct_themes_per_row"),
        )
        .first()
        .asDict()
    )

    assert (
        theme_summary["output_rows"] == expected_rows
    ), "GKG row count changed during theme transformation"

    # Notebook 05 cell 6: final flags; missing optional themes stay separate.
    candidate_key_window = Window.partitionBy("record_candidate_key")

    fingerprint_window = Window.partitionBy("record_fingerprint")

    gkg_silver = (
        gkg_themes
        # Remove the remaining temporary tone array.
        .drop("_tone_parts")
        # Flag duplicate derived keys without deleting rows.
        .withColumn("candidate_key_group_size", F.count("*").over(candidate_key_window))
        .withColumn("duplicate_key_flag", F.col("candidate_key_group_size") > 1)
        # Flag exact repetition of all six exported raw values.
        .withColumn(
            "record_fingerprint_group_size", F.count("*").over(fingerprint_window)
        )
        .withColumn("exact_duplicate_flag", F.col("record_fingerprint_group_size") > 1)
        # Parsing issues indicate malformed required values.
        .withColumn(
            "parsing_issue_flag",
            F.col("invalid_id_flag")
            | F.col("invalid_timestamp_flag")
            | F.col("id_timestamp_mismatch_flag")
            | F.col("missing_tone_flag")
            | F.col("invalid_tone_flag")
            | F.col("invalid_themes_flag"),
        )
        # The overall flag includes source-ID conflicts and missing
        # identifiers, while optional missing themes remain separate.
        .withColumn(
            "quality_issue_flag",
            F.col("parsing_issue_flag")
            | F.col("missing_source_flag")
            | F.col("missing_document_url_flag")
            | F.col("source_id_conflict_flag")
            | F.col("duplicate_key_flag")
            | F.col("exact_duplicate_flag"),
        )
    )

    return gkg_silver



def clean_events(bronze_events):
    """Notebook 05 cell 7: return Events Silver fields; retain rows and exported values."""
    events_input = bronze_events

    required_event_columns = {
        "GLOBALEVENTID",
        "SQLDATE",
        "DATEADDED",
        "EventCode",
        "EventRootCode",
        "QuadClass",
        "GoldsteinScale",
        "NumMentions",
        "NumSources",
        "AvgTone",
        "ActionGeo_Lat",
        "ActionGeo_Long",
        "SOURCEURL",
        "_source_file",
        "_batch_id",
        "_coverage_status",
    }

    missing_event_columns = required_event_columns - set(events_input.columns)

    assert not missing_event_columns, (
        f"Events Bronze is missing columns: " f"{sorted(missing_event_columns)}"
    )

    valid_event_id = F.coalesce(
        F.col("GLOBALEVENTID").rlike(r"^[1-9][0-9]*$"), F.lit(False)
    )

    valid_event_date = F.coalesce(F.col("SQLDATE").rlike(r"^[0-9]{8}$"), F.lit(False))

    valid_added_timestamp = F.coalesce(
        F.col("DATEADDED").rlike(r"^[0-9]{14}$"), F.lit(False)
    )

    event_id_window = Window.partitionBy("event_id")

    events_silver = (
        events_input
        # Validated event identifier.
        .withColumn("event_id", F.when(valid_event_id, F.col("GLOBALEVENTID")))
        # Event date from SQLDATE.
        .withColumn(
            "event_date",
            F.when(
                valid_event_date,
                F.try_to_timestamp(F.col("SQLDATE"), F.lit("yyyyMMdd")).cast("date"),
            ),
        )
        # GDELT insertion or update timestamp.
        .withColumn(
            "event_added_at_utc",
            F.when(
                valid_added_timestamp,
                F.try_to_timestamp(F.col("DATEADDED"), F.lit("yyyyMMddHHmmss")),
            ),
        )
        # Convenience columns preserve the original raw values.
        .withColumn(
            "event_code",
            F.when(
                F.col("EventCode").isNull() | (F.trim(F.col("EventCode")) == ""),
                F.lit(None).cast("string"),
            ).otherwise(F.col("EventCode")),
        )
        .withColumn(
            "event_root_code",
            F.when(
                F.col("EventRootCode").isNull()
                | (F.trim(F.col("EventRootCode")) == ""),
                F.lit(None).cast("string"),
            ).otherwise(F.col("EventRootCode")),
        )
        .withColumn(
            "document_url",
            F.when(
                F.col("SOURCEURL").isNull() | (F.trim(F.col("SOURCEURL")) == ""),
                F.lit(None).cast("string"),
            ).otherwise(F.col("SOURCEURL")),
        )
        # Required-field validation.
        .withColumn("invalid_id_flag", F.col("event_id").isNull())
        .withColumn("invalid_event_date_flag", F.col("event_date").isNull())
        .withColumn("invalid_timestamp_flag", F.col("event_added_at_utc").isNull())
        .withColumn("missing_event_code_flag", F.col("event_code").isNull())
        .withColumn("missing_event_root_code_flag", F.col("event_root_code").isNull())
        .withColumn("missing_document_url_flag", F.col("document_url").isNull())
        # Event and insertion dates may legitimately differ.
        .withColumn(
            "event_date_differs_from_added_date_flag",
            F.coalesce(
                F.col("event_date") != F.to_date(F.col("event_added_at_utc")),
                F.lit(False),
            ),
        )
        # Preserve all rows while flagging repeated event IDs.
        .withColumn("event_id_group_size", F.count("*").over(event_id_window))
        .withColumn(
            "duplicate_key_flag",
            F.col("event_id").isNotNull() & (F.col("event_id_group_size") > 1),
        )
    )

    events_silver = add_integer_validation(
        events_silver, "QuadClass", "QuadClass_num", minimum=1, maximum=4
    )

    events_silver = add_integer_validation(
        events_silver, "NumMentions", "NumMentions_num", minimum=0
    )

    events_silver = add_integer_validation(
        events_silver, "NumSources", "NumSources_num", minimum=0
    )

    events_silver = add_double_validation(
        events_silver,
        "GoldsteinScale",
        "GoldsteinScale_num",
        minimum=-10.0,
        maximum=10.0,
    )

    events_silver = add_double_validation(
        events_silver, "AvgTone", "AvgTone_num", minimum=-100.0, maximum=100.0
    )

    events_silver = add_double_validation(
        events_silver, "ActionGeo_Lat", "ActionGeo_Lat_num", minimum=-90.0, maximum=90.0
    )

    events_silver = add_double_validation(
        events_silver,
        "ActionGeo_Long",
        "ActionGeo_Long_num",
        minimum=-180.0,
        maximum=180.0,
    )

    required_numeric_missing_flags = [
        "missing_QuadClass_flag",
        "missing_NumMentions_flag",
        "missing_NumSources_flag",
        "missing_GoldsteinScale_flag",
        "missing_AvgTone_flag",
    ]

    numeric_invalid_flags = [
        "invalid_QuadClass_flag",
        "invalid_NumMentions_flag",
        "invalid_NumSources_flag",
        "invalid_GoldsteinScale_flag",
        "invalid_AvgTone_flag",
        "invalid_ActionGeo_Lat_flag",
        "invalid_ActionGeo_Long_flag",
    ]

    events_silver = (
        events_silver
        # Only one coordinate being absent is an incomplete coordinate pair.
        .withColumn(
            "incomplete_coordinates_flag",
            F.col("missing_ActionGeo_Lat_flag") != F.col("missing_ActionGeo_Long_flag"),
        )
        .withColumn(
            "missing_required_numeric_flag",
            reduce(
                lambda left, right: left | right,
                [F.col(column_name) for column_name in required_numeric_missing_flags],
            ),
        )
        .withColumn(
            "invalid_numeric_flag",
            reduce(
                lambda left, right: left | right,
                [F.col(column_name) for column_name in numeric_invalid_flags],
            ),
        )
        .withColumn(
            "quality_issue_flag",
            F.col("invalid_id_flag")
            | F.col("invalid_event_date_flag")
            | F.col("invalid_timestamp_flag")
            | F.col("missing_event_code_flag")
            | F.col("missing_event_root_code_flag")
            | F.col("missing_document_url_flag")
            | F.col("duplicate_key_flag")
            | F.col("missing_required_numeric_flag")
            | F.col("invalid_numeric_flag")
            | F.col("incomplete_coordinates_flag"),
        )
    )

    return events_silver



def clean_mentions(bronze_mentions):
    """Notebook 05 cell 8: return Mentions Silver fields; retain rows and exported values."""
    mentions_input = bronze_mentions

    required_mention_columns = {
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
        "_source_file",
        "_batch_id",
        "_coverage_status",
    }

    missing_mention_columns = required_mention_columns - set(mentions_input.columns)

    assert not missing_mention_columns, (
        f"Mentions Bronze is missing columns: " f"{sorted(missing_mention_columns)}"
    )

    valid_mention_event_id = F.coalesce(
        F.col("GLOBALEVENTID").rlike(r"^[1-9][0-9]*$"), F.lit(False)
    )

    valid_event_timestamp = F.coalesce(
        F.col("EventTimeDate").rlike(r"^[0-9]{14}$"), F.lit(False)
    )

    valid_mention_timestamp = F.coalesce(
        F.col("MentionTimeDate").rlike(r"^[0-9]{14}$"), F.lit(False)
    )

    mentions_silver = (
        mentions_input
        # Validated event identifier used to join with Events.
        .withColumn("event_id", F.when(valid_mention_event_id, F.col("GLOBALEVENTID")))
        # Event registration timestamp referenced by this mention.
        .withColumn(
            "event_added_at_utc",
            F.when(
                valid_event_timestamp,
                F.try_to_timestamp(F.col("EventTimeDate"), F.lit("yyyyMMddHHmmss")),
            ),
        )
        # Timestamp when the mention was observed.
        .withColumn(
            "mention_at_utc",
            F.when(
                valid_mention_timestamp,
                F.try_to_timestamp(F.col("MentionTimeDate"), F.lit("yyyyMMddHHmmss")),
            ),
        )
        # Convenience columns preserve original values.
        .withColumn(
            "mention_source_name",
            F.when(
                F.col("MentionSourceName").isNull()
                | (F.trim(F.col("MentionSourceName")) == ""),
                F.lit(None).cast("string"),
            ).otherwise(F.col("MentionSourceName")),
        )
        .withColumn(
            "mention_identifier_clean",
            F.when(
                F.col("MentionIdentifier").isNull()
                | (F.trim(F.col("MentionIdentifier")) == ""),
                F.lit(None).cast("string"),
            ).otherwise(F.col("MentionIdentifier")),
        )
        # Required identifier and timestamp validation.
        .withColumn("invalid_id_flag", F.col("event_id").isNull())
        .withColumn(
            "invalid_event_timestamp_flag", F.col("event_added_at_utc").isNull()
        )
        .withColumn("invalid_mention_timestamp_flag", F.col("mention_at_utc").isNull())
        .withColumn(
            "mention_before_event_flag",
            F.coalesce(
                F.col("mention_at_utc") < F.col("event_added_at_utc"), F.lit(False)
            ),
        )
        .withColumn(
            "missing_mention_source_flag", F.col("mention_source_name").isNull()
        )
        .withColumn(
            "missing_mention_identifier_flag",
            F.col("mention_identifier_clean").isNull(),
        )
        # Fingerprint all 11 exported Mentions values.
        # Repetition is retained because the export does not include
        # SentenceID or character-offset fields.
        .withColumn(
            "export_row_fingerprint",
            F.sha2(
                F.to_json(
                    F.struct(
                        *[
                            F.col(column_name)
                            for column_name in [
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
                            ]
                        ]
                    ),
                    {"ignoreNullFields": "false"},
                ),
                256,
            ),
        )
    )

    assert callable(add_integer_validation)

    assert callable(add_double_validation)

    mentions_silver = add_integer_validation(
        mentions_silver, "MentionType", "MentionType_num", minimum=1, maximum=6
    )

    mentions_silver = add_integer_validation(
        mentions_silver, "Confidence", "Confidence_num", minimum=10, maximum=100
    )

    mentions_silver = add_integer_validation(
        mentions_silver, "InRawText", "InRawText_num", minimum=0, maximum=1
    )

    mentions_silver = add_integer_validation(
        mentions_silver, "MentionDocLen", "MentionDocLen_num", minimum=0
    )

    mentions_silver = add_double_validation(
        mentions_silver,
        "MentionDocTone",
        "MentionDocTone_num",
        minimum=-100.0,
        maximum=100.0,
    )

    mention_required_missing_flags = [
        "missing_MentionType_flag",
        "missing_Confidence_flag",
        "missing_InRawText_flag",
        "missing_MentionDocLen_flag",
        "missing_MentionDocTone_flag",
    ]

    mention_invalid_numeric_flags = [
        "invalid_MentionType_flag",
        "invalid_Confidence_flag",
        "invalid_InRawText_flag",
        "invalid_MentionDocLen_flag",
        "invalid_MentionDocTone_flag",
    ]

    fingerprint_window = Window.partitionBy("export_row_fingerprint")

    mentions_silver = (
        mentions_silver.withColumn(
            "export_value_group_size", F.count("*").over(fingerprint_window)
        )
        .withColumn("repeated_export_values_flag", F.col("export_value_group_size") > 1)
        .withColumn(
            "missing_required_numeric_flag",
            reduce(
                lambda left, right: left | right,
                [F.col(column_name) for column_name in mention_required_missing_flags],
            ),
        )
        .withColumn(
            "invalid_numeric_flag",
            reduce(
                lambda left, right: left | right,
                [F.col(column_name) for column_name in mention_invalid_numeric_flags],
            ),
        )
        # Repeated exported values remain separate from quality_issue_flag
        # because omitted SentenceID/offset fields prevent us from proving
        # that they represent duplicate real-world mentions.
        .withColumn(
            "quality_issue_flag",
            F.col("invalid_id_flag")
            | F.col("invalid_event_timestamp_flag")
            | F.col("invalid_mention_timestamp_flag")
            | F.col("mention_before_event_flag")
            | F.col("missing_mention_source_flag")
            | F.col("missing_mention_identifier_flag")
            | F.col("missing_required_numeric_flag")
            | F.col("invalid_numeric_flag"),
        )
    )

    return mentions_silver
