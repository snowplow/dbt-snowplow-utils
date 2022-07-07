{%- macro get_partition_by(bigquery_partition_by=none, databricks_partition_by=none) -%}

  {% if target.type == 'bigquery' %}
    {{ return(bigquery_partition_by) }}
  {% elif target.type in ['databricks', 'spark'] %}
    {{ return(databricks_partition_by) }}
  {% endif %}

{%- endmacro -%}
