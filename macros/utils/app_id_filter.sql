{% macro app_id_filter(app_ids) %}

  {%- if app_ids|length -%}

    app_id in ('{{ app_ids|join("','") }}') --filter on app_id if provided

  {%- else -%}

    true

  {%- endif -%}

{% endmacro %}
