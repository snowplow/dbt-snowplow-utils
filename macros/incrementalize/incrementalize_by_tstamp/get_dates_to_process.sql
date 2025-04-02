{% macro get_dates_to_process(this_run_table, manifest_table) %}

{% set run_type = var('snowplow__run_type', 'incremental') %}
{% set late_event_lookback_days = var('snowplow__late_event_lookback_days', 0) %}
{% set snowplow__backfill_limit_days = var('snowplow__backfill_limit_days', 0) %}
{% set min_late_events_to_process = var('snowplow__min_late_events_to_process', 1000) %}
{% set snowplow__start_date = var('snowplow__start_date', '2025-01-01') %}
{% if late_event_lookback_days > snowplow__backfill_limit_days %}
    {% set limit_days = late_event_lookback_days %}
{% else %}
    {% set limit_days = snowplow__backfill_limit_days %}
{% endif %}


{% set is_not_full_refresh = flags.FULL_REFRESH == false %}
-- can we find the schema for the manifest table based on the compiled data? ideally from dbt node metadata
{% set manifest_schema = target.schema~'_snowplow_manifest' %}

WITH source_data AS (
    SELECT 
        date(derived_tstamp) AS event_date,
        count(*) as event_count,
        CASE 
            WHEN date(derived_tstamp) IN (
                SELECT DISTINCT date(derived_tstamp)
                FROM {{ ref(this_run_table) }}
                WHERE date(derived_tstamp) >= '{{ snowplow__start_date }}'
                EXCEPT
                SELECT DISTINCT date(load_tstamp)
                FROM {{ ref(this_run_table) }}
                WHERE date(derived_tstamp) >= '{{ snowplow__start_date }}'
            ) THEN 1
            ELSE 0
        END AS is_delayed
    FROM {{ ref(this_run_table) }}
    WHERE date(derived_tstamp) >= '{{ snowplow__start_date }}'
    GROUP BY date(derived_tstamp)
),
{% if is_not_full_refresh %}
manifest_data AS (
    SELECT 
        event_date,
        skipped_events as manifest_skipped_events
    -- create the scheme ref not native dbt way but {}.{}.{} way
    FROM {{manifest_schema}}.{{manifest_table}}
),
combined_data AS (
    SELECT 
        s.event_date,
        s.event_count,
        s.is_delayed,
        COALESCE(m.manifest_skipped_events, 0) + CASE WHEN s.is_delayed = 1 THEN s.event_count ELSE 0 END as skipped_events,
        s.event_count + COALESCE(m.manifest_skipped_events, 0) as total_events
    FROM source_data s
    LEFT JOIN manifest_data m ON s.event_date = m.event_date
    WHERE (skipped_events >= {{ min_late_events_to_process }} OR NOT is_delayed)
    ORDER BY event_date DESC
    LIMIT {{ limit_days }}

)
{% else %}
combined_data AS (
    SELECT 
        event_date,
        event_count,
        is_delayed,
        0 as skipped_events,
        event_count as total_events
    FROM source_data
    ORDER BY event_date DESC
)
{% endif %}

SELECT 
    event_date,
    event_count,
    skipped_events,
    total_events,
    is_delayed,
    current_timestamp() as processed_at
FROM combined_data

{% endmacro %}
