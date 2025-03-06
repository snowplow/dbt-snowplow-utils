{% macro create_daily_manifest(source_model) %}

    {% set start_date = var('snowplow__start_date', '2025-01-01') %}

    -- Get the distinct dates being processed in this run
    WITH dates_to_process AS (
        SELECT DISTINCT 
            event_date
        FROM {{ ref("snowplow_autogen_daily_aggregates_this_run") }}
    ),

    -- Calculate event metrics per day
    daily_event_metrics AS (
        SELECT 
            DATE(derived_tstamp) AS event_date,
            MAX(load_tstamp)::TIMESTAMP_NTZ AS max_load_tstamp,
            AVG(DATEDIFF('second', derived_tstamp, load_tstamp) / 3600.0)::FLOAT AS avg_delay_hours,
            PERCENTILE_CONT(0.50) WITHIN GROUP (
                ORDER BY DATEDIFF('second', derived_tstamp, load_tstamp) / 3600.0
            )::FLOAT AS median_delay_hours,
            PERCENTILE_CONT(0.95) WITHIN GROUP (
                ORDER BY DATEDIFF('second', derived_tstamp, load_tstamp) / 3600.0
            )::FLOAT AS p95_delay_hours,
            MAX(DATEDIFF('second', derived_tstamp, load_tstamp) / 3600.0)::FLOAT AS max_delay_hours,
            COUNT(*) AS event_count
        FROM {{ ref("snowplow_autogen_filtered_events") }}
        WHERE DATE(derived_tstamp) IN (SELECT event_date FROM dates_to_process)
        GROUP BY 1
    ),

    -- Calculate events that were skipped in this run
    daily_skipped_events AS (
        SELECT 
            events.event_date,
            SUM(CASE WHEN d.event_date IS NULL THEN 1 ELSE 0 END) AS skipped_events
        FROM {{ ref("snowplow_autogen_filtered_events_this_run") }} events
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
                number_of_rows AS previous_number_of_rows,
                max_load_tstamp AS previous_max_load_tstamp,
                avg_delay_hours AS previous_avg_delay_hours,
                median_delay_hours AS previous_median_delay_hours,
                p95_delay_hours AS previous_p95_delay_hours,
                max_delay_hours AS previous_max_delay_hours
            FROM {{ this }}
        )
    {% else %}
        previous_manifest_data AS (
            SELECT
                NULL AS event_date,
                NULL AS previous_skipped_events,
                NULL AS previous_number_of_rows,
                NULL AS previous_max_load_tstamp,
                NULL AS previous_avg_delay_hours,
                NULL AS previous_median_delay_hours,
                NULL AS previous_p95_delay_hours,
                NULL AS previous_max_delay_hours
        )
    {% endif %}

    -- Combine all metrics and calculate final manifest
    SELECT 
        COALESCE(skipped.event_date, metrics.event_date) AS event_date,
        '{{ source_model }}'::VARCHAR AS source_model,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS last_updated_at,
        COALESCE(metrics.event_count, prev.previous_number_of_rows) AS number_of_rows,
        COALESCE(metrics.max_load_tstamp, prev.previous_max_load_tstamp) AS max_load_tstamp,
        COALESCE(metrics.avg_delay_hours, prev.previous_avg_delay_hours) AS avg_delay_hours,
        COALESCE(metrics.median_delay_hours, prev.previous_median_delay_hours) AS median_delay_hours,
        COALESCE(metrics.p95_delay_hours, prev.previous_p95_delay_hours) AS p95_delay_hours,
        COALESCE(metrics.max_delay_hours, prev.previous_max_delay_hours) AS max_delay_hours,
        COALESCE(prev.previous_skipped_events, 0) + COALESCE(skipped.skipped_events, 0)::NUMBER AS skipped_events
    FROM daily_event_metrics metrics
    FULL OUTER JOIN daily_skipped_events skipped
        ON skipped.event_date = metrics.event_date
    LEFT JOIN previous_manifest_data prev
        ON COALESCE(skipped.event_date, metrics.event_date) = prev.event_date
    WHERE COALESCE(skipped.event_date, metrics.event_date) >= '{{ start_date }}'

{% endmacro %}
