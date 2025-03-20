{% macro get_dates_to_process(this_run_table, manifest_table) %}

{% set run_type = var('snowplow__run_type', 'incremental') %}
{% set late_event_lookback_days = var('snowplow__late_event_lookback_days', 0) %}
{% set min_late_events_to_process = var('snowplow__min_late_events_to_process', 1000) %}
{% set snowplow__start_date = var('snowplow__start_date', '2025-01-01') %}

{%- set manifest_relation = adapter.get_relation(
    database=target.database,
    schema=target.schema,
    identifier=manifest_table) -%}

{% set manifest_exists = manifest_relation is not none %}

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
{% if manifest_exists %}
manifest_data AS (
    SELECT 
        event_date,
        skipped_events
    -- create the scheme ref not native dbt way but {}.{}.{} way
    FROM {{target.database}}.{{target.schema}}.{{manifest_table}}
),
combined_data AS (
    SELECT 
        s.event_date,
        s.event_count,
        s.is_delayed,
        COALESCE(m.skipped_events, 0) as skipped_events,
        s.event_count + COALESCE(m.skipped_events, 0) as total_events,
        case when COALESCE(m.skipped_events, 0) >= {{ min_late_events_to_process }} then 1 else 0 end as is_delayed
    FROM source_data s
    LEFT JOIN manifest_data m ON s.event_date = m.event_date
    HAVING skipped_events >= {{ min_late_events_to_process }}
    ORDER BY event_date DESC
    LIMIT {{ late_event_lookback_days }}
)
{% else %}
combined_data AS (
    SELECT 
        event_date,
        event_count,
        is_delayed,
        0 as skipped_events,
        event_count as total_events,
    FROM source_data
    ORDER BY event_date DESC
    LIMIT {{ late_event_lookback_days }}
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
