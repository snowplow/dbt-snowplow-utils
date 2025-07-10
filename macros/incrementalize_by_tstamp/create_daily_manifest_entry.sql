{% macro create_daily_manifest_entry(source_model, prefix) %}

    {% set start_date = var('snowplow__start_date', '2025-01-01') %}

    -- Get the distinct dates being processed in this run
    WITH dates_to_process AS (
        SELECT DISTINCT 
            event_date
        FROM {{ref(prefix ~ "_daily_aggregates_this_run")}}
    ),

    -- Calculate event metrics per day
    daily_event_metrics AS (
        SELECT 
            DATE(derived_tstamp) AS event_date,
            MAX(load_tstamp) AS max_load_tstamp,
            COUNT(*) AS event_count
        FROM {{ ref(prefix ~ "_filtered_events") }}
        WHERE DATE(derived_tstamp) IN (SELECT event_date FROM dates_to_process)
        GROUP BY 1
    ),

    -- Calculate events that were skipped in this run
    daily_skipped_events AS (
        SELECT 
            events.event_date,
            SUM(CASE WHEN d.event_date IS NULL THEN 1 ELSE 0 END) AS skipped_events
        FROM {{ ref(prefix ~ "_filtered_events_this_run") }} events
        LEFT JOIN dates_to_process d 
            ON events.event_date = d.event_date
        GROUP BY 1
    ),

    -- Get previous manifest data for incremental processing
    {% if is_incremental() %}
        previous_manifest_data AS (
            SELECT 
                event_date,
                skipped_events AS previous_skipped_events,
                processed_events AS previous_processed_events,
                max_load_tstamp AS previous_max_load_tstamp
                {# avg_delay_hours AS previous_avg_delay_hours,
                median_delay_hours AS previous_median_delay_hours,
                p95_delay_hours AS previous_p95_delay_hours,
                max_delay_hours AS previous_max_delay_hours #}
            FROM {{ this }}
        )
    {% else %}
        previous_manifest_data AS (
            SELECT
                date(null) AS event_date,
                cast(null as {{ dbt.type_int() }}) AS previous_skipped_events,
                cast(null as {{ dbt.type_int() }}) AS previous_processed_events,
                cast(null as {{ dbt.type_timestamp() }}) AS previous_max_load_tstamp
                {# NULL AS previous_avg_delay_hours,
                NULL AS previous_median_delay_hours,
                NULL AS previous_p95_delay_hours,
                NULL AS previous_max_delay_hours #}
        )
    {% endif %}

    -- Combine all metrics and calculate final manifest
    SELECT 
        COALESCE(skipped.event_date, metrics.event_date) AS event_date,
        cast('{{ source_model }}' as {{ dbt.type_string() }}) AS source_model,
        COALESCE(metrics.event_count, prev.previous_processed_events) AS processed_events,
        {# COALESCE(metrics.avg_delay_hours, prev.previous_avg_delay_hours) AS avg_delay_hours,
        COALESCE(metrics.median_delay_hours, prev.previous_median_delay_hours) AS median_delay_hours,
        COALESCE(metrics.p95_delay_hours, prev.previous_p95_delay_hours) AS p95_delay_hours,
        COALESCE(metrics.max_delay_hours, prev.previous_max_delay_hours) AS max_delay_hours, #}
        cast(COALESCE(prev.previous_skipped_events, 0) + COALESCE(skipped.skipped_events, 0) as {{ dbt.type_int() }}) AS skipped_events,
        COALESCE(metrics.max_load_tstamp, prev.previous_max_load_tstamp) AS max_load_tstamp,
        {{ snowplow_utils.current_timestamp_in_utc() }} AS last_updated_at
    FROM daily_event_metrics metrics
    FULL OUTER JOIN daily_skipped_events skipped
        ON skipped.event_date = metrics.event_date
    LEFT JOIN previous_manifest_data prev
        ON COALESCE(skipped.event_date, metrics.event_date) = prev.event_date
    WHERE COALESCE(skipped.event_date, metrics.event_date) >= '{{ start_date }}'

{% endmacro %}
