{% macro print_dates_to_process() %}
    {%- set query %}
        SELECT 
            event_date,
            event_count,
            skipped_events,
            total_events,
            is_delayed
        FROM {{ this }}
        ORDER BY event_date DESC
    {% endset -%}

    {%- if execute -%}
        {%- set results = run_query(query) -%}
        {%- set dates = results.columns[0].values() -%}
        {%- set counts = results.columns[1].values() -%}
        {%- set skipped_events = results.columns[2].values() -%}
        {%- set total_events = results.columns[3].values() -%}
        {%- set delayed = results.columns[4].values() -%}

        {% do log("================================================", info=True) %}
        {% do log("Dates to Process Summary", info=True) %}
        {% do log("================================================", info=True) %}
        {% do log("Total dates to process: " ~ dates|length, info=True) %}
        {% do log("Total events to process: " ~ (total_events|sum), info=True) %}
        {% do log("================================================", info=True) %}

        {%- if dates|length > 20 -%}
            {% do log("⚠️  Large number of dates detected. Showing only the latest 20 days.", info=True) %}
        {%- endif -%}

        {%- for i in range(dates|length if dates|length <= 20 else 20) -%}
            {% do log(
                "📅 " ~ dates[i] ~ 
                " | " ~ ("⌛ Delayed" if delayed[i] == 1 else "✅ Regular") ~ " events" ~
                " | 📊 " ~ total_events[i] ~ " events",

                info=True
            ) %}
        {%- endfor -%}

        {% do log("================================================\n", info=True) %}
    {%- endif -%}
{% endmacro %} 