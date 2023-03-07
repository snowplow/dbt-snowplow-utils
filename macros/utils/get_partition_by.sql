{%- macro get_partition_by(bigquery_partition_by=none, databricks_partition_by=none) -%}

  {%- do exceptions.warn("Warning: the `get_partition_by` macro is deprecated and will be removed in a future version of the package, please use `get_value_by_target_type` instead.") -%}

  {% if target.type == 'bigquery' %}
    {{ return(bigquery_partition_by) }}
  {% elif target.type in ['databricks', 'spark'] %}
    {{ return(databricks_partition_by) }}
  {% endif %}

{%- endmacro -%}
