{%- macro get_value_by_target_type(bigquery_val=none, snowflake_val=none, redshift_val=none, postgres_val=none, databricks_val=none) -%}

  {% if target.type == 'bigquery' %}
    {{ return(bigquery_val) }}
  {% elif target.type == 'snowflake' %}
    {{ return(snowflake_val) }}
  {% elif target.type == 'redshift' %}
    {{ return(redshift_val) }}
  {% elif target.type == 'postgres' %}
    {{ return(postgres_val) }}
  {% elif target.type in ['databricks', 'spark'] %}
    {{ return(databricks_val) }}
  {% else %}
    {{ exceptions.raise_compiler_error("Snowplow: Unexpected target type "~target.type) }}
  {% endif %}

{%- endmacro -%}
