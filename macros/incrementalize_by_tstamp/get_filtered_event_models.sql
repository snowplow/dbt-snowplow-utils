{% macro get_filtered_event_models() %}
    {% set filtered_event_models = [] %}

    {% for node in graph.nodes.values() %}
        {%- if node.resource_type == 'model' and node.path.startswith('filtered_events/') -%}
            {%- if not node.name.endswith('_this_run') -%}
                {% do filtered_event_models.append(node.name) %}
            {%- endif -%}
        {%- endif -%}
    {% endfor %}

    {{ return(filtered_event_models) }}
{% endmacro %} 