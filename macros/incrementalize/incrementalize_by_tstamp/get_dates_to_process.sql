{% macro get_dates_to_process(source_model, date_column) %}
    {% set run_type = var('snowplow__run_type', 'incremental') %}
    {% set late_event_lookback_days = var('snowplow__late_event_lookback_days', 0) %}
    {% set min_late_events_to_process = var('snowplow__min_late_events_to_process', 1000) %}
    {% set snowplow__start_date = var('snowplow__start_date', '2025-01-01') %}
    -- Log if run_type is incremental

    {% if date_column == 'derived_tstamp' %}
        {% set node_name = 'model.snowplow_autogen.' ~ this_run_model %}
        {% if (not execute) or (graph.nodes.get(node_name) is none) %}
            {% do log("Model not found, returning NULL for dates to process", info=True) %}
            {{ return("NULL") }}
        {% endif %}
        
        {% set query %}
            SELECT date(derived_tstamp) AS event_date, count(*) as count_rows,
                MAX(CASE WHEN date(derived_tstamp) != date(load_tstamp) THEN 1 ELSE 0 END) AS is_delayed
            FROM {{ ref(source_model) }}
            WHERE date(derived_tstamp) >= '{{ snowplow__start_date }}'
            GROUP BY date(derived_tstamp)
            HAVING count_rows >= {{ min_late_events_to_process }}
            ORDER BY event_date DESC
            LIMIT {{ late_event_lookback_days }}
        {% endset %}
        
        {% set results = run_query(query) %}
        
        {% if execute %}
            {% set dates = results.columns[0].values() %}
            {% set count_rows = results.columns[1].values() %}
            {% set is_delayed = results.columns[2].values() %}
            {% set date_list = [] %}
            
            {% do log("\n================================================\nProcessing data\n================================================\n", info=True) %}
            {% for i in range(dates|length) %}
                {% set date = dates[i] %}
                {% set count_row = count_rows[i] %}
                
                {% if is_delayed[i] == 1 %}
                    {% do log("Refreshing delayed data for event date: " ~ date ~ " with " ~ count_row ~ " rows", info=True) %}
                {% else %}
                    {% do log("Refreshing data for event date: " ~ date ~ " with " ~ count_row ~ " rows", info=True) %}
                {% endif %}
                
                {% do date_list.append("'" ~ date ~ "'") %}
            {% endfor %}

            {% if date_list|length > 0 %}
                {{ return(date_list|join(',')) }}
            {% else %}
                {{ return("NULL") }}
            {% endif %}
        {% else %}
            {{ return("NULL") }}
        {% endif %}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid run_type: " ~ run_type) }}
    {% endif %}
{% endmacro %} 