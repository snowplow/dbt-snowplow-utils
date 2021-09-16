{% macro get_cluster_by(bigquery_cols=none, snowflake_cols=none) %}

  {% if target.type == 'bigquery' %}
    {{ return(bigquery_cols) }}
  {% elif target.type == 'snowflake' %}
    {{ return(snowflake_cols) }}
  {% endif %}
  
{% endmacro %}
