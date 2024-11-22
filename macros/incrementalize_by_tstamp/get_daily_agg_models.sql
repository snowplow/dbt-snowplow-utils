{% macro get_daily_agg_models() %}
    {% set daily_agg_models = [] %}

    {% if execute %}
        {{ log("Starting model discovery...", info=True) }}

        {% for node in graph.nodes.values() %}
            {% if node.resource_type == 'model' 
                and 'daily_aggregates' in node.path 
                and '_this_run' in node.name
                and node.depends_on.nodes
            %}
                {% set source_model_ref = node.depends_on.nodes[0] %}
                {% set source_model = source_model_ref.split('.')[-1] %}
                {% do daily_agg_models.append(source_model) %}
                {{ log("Added model: " ~ source_model, info=True) }}
            {% endif %}
        {% endfor %}

        {{ log("Found models: " ~ daily_agg_models | length, info=True) }}
    {% endif %}
    {{ log( daily_agg_models) }}
    {{ return(daily_agg_models) }}
{% endmacro %} 