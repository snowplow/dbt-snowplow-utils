{% macro create_daily_aggregation_manifest(package_name) %}
    {% set daily_agg_models = snowplow_utils.get_daily_agg_models() %}

    {% if execute and daily_agg_models|length > 0 %}
        {% for model in daily_agg_models %}
            {{ log("Processing model: " ~ model, info=True) }}
            ({{ snowplow_utils.create_daily_manifest_entry(model, package_name) }})
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    {% else %}
        select
            null::date as event_date,
            null::varchar as source_model,
            0::number as processed_events,
            0::number as skipped_events,
            null::timestamp_ntz as max_load_tstamp,
            null::timestamp_ntz as last_updated_at
            {# 0.0::float as avg_delay_hours,
            0.0::float as median_delay_hours,
            0.0::float as p95_delay_hours,
            0.0::float as max_delay_hours #}
        where false
    {% endif %}
{% endmacro %} 
