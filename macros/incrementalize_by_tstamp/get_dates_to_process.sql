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

WITH base AS (
  SELECT
    DATE(derived_tstamp) AS event_date,
    load_tstamp
  FROM {{ ref(this_run_table) }}
  WHERE DATE(derived_tstamp) >= '{{ snowplow__start_date }}'
),

source_data AS (
  SELECT 
    event_date,
    COUNT(*) AS event_count,

    {% if target.type == 'snowflake' %}
      CASE 
        WHEN event_date IN (
          SELECT DISTINCT DATE(derived_tstamp)
          FROM {{ ref(this_run_table) }}
          WHERE DATE(derived_tstamp) >= '{{ snowplow__start_date }}'
          EXCEPT
          SELECT DISTINCT DATE(load_tstamp)
          FROM {{ ref(this_run_table) }}
          WHERE DATE(derived_tstamp) >= '{{ snowplow__start_date }}'
        ) THEN 1
        ELSE 0
      END AS is_delayed
    {% elif target.type == 'bigquery' %}
      IF(
        NOT EXISTS (
          SELECT 1
          FROM {{ ref(this_run_table) }} d
          WHERE DATE(d.derived_tstamp) = base.event_date
            AND DATE(d.load_tstamp) = base.event_date
        ), 1, 0
      ) AS is_delayed
    {% endif %}

  FROM base
  GROUP BY event_date
)

{% if is_not_full_refresh %}
  ,  manifest_data as (
        select 
            event_date,
            skipped_events as manifest_skipped_events
        -- create the scheme ref not native dbt way but {}.{}.{} way
        from {{manifest_schema}}.{{manifest_table}}
    ),
    combined_data as (
        select 
            s.event_date,
            s.event_count,
            coalesce(m.manifest_skipped_events, cast(0 as {{ dbt.type_int() }})) + case when s.is_delayed = 1 then s.event_count else 0 end as skipped_events,
            s.event_count + coalesce(m.manifest_skipped_events, cast(0 as {{ dbt.type_int() }})) as total_events,
            cast(s.is_delayed as {{ dbt.type_boolean() }}) as is_delayed,
            {{ snowplow_utils.current_timestamp_in_utc() }} as processed_at
        from source_data s
        left join manifest_data m on s.event_date = m.event_date
    )

    select * 
    from combined_data
    where (skipped_events >= {{ min_late_events_to_process }} or not is_delayed)
    order by event_date desc
    limit {{ limit_days }}
    
{% else %}

    select 
        event_date,
        event_count,
        cast(0 as {{ dbt.type_int() }}) as skipped_events,
        event_count as total_events,
        is_delayed,
        {{ snowplow_utils.current_timestamp_in_utc() }} as processed_at
      from source_data
      order by event_date desc
  
{% endif %}
{% endmacro %}
