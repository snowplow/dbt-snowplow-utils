{% macro get_cluster_by(bigquery_cols=none, snowflake_cols=none) %}

  {%- do exceptions.warn("Warning: the `get_cluster_by` macro is deprecated and will be removed in a future version of the package, please use `get_value_by_target_type` instead.") -%}


  {% if target.type == 'bigquery' %}
    {{ return(bigquery_cols) }}
  {% elif target.type == 'snowflake' %}
    {{ return(snowflake_cols) }}
  {% endif %}

{% endmacro %}
