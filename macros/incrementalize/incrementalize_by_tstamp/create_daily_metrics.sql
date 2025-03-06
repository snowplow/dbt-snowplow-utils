{% macro create_daily_metrics(source_model) %}
    {% set metrics = get_daily_semantic_metrics(source_model) %}

    select
        date(derived_tstamp) as event_date,
        {% for metric in metrics %}
        {{ metric.agg }} as {{ metric.name }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref(source_model) }}
    {% if is_incremental() %}
        where date(derived_tstamp) in ({{ snowplow_utils.get_dates_to_process(source_model) }})
    {% endif %}
    group by 1
{% endmacro %}