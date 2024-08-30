{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Destructive macro. Use with care! #}

{% macro post_ci_cleanup(schema_pattern=target.schema) %}
  {{ return(adapter.dispatch('post_ci_cleanup', 'snowplow_utils')(schema_pattern)) }}
{% endmacro %}


{% macro default__post_ci_cleanup(schema_pattern=target.schema) %}

  {# Get all schemas with the target.schema prefix #}
  {% set schemas = snowplow_utils.get_schemas_by_pattern(schema_pattern~'%') %}

  {% if schemas|length %}

    {%- if target.type in ['databricks'] -%}
      {# Generate sql to drop all identified schemas #}
      {% for schema in schemas -%}
        {%- set drop_schema_sql -%}
          DROP SCHEMA IF EXISTS {{schema}} CASCADE;
        {%- endset -%}

        {% do run_query(drop_schema_sql) %}

      {% endfor %}

    {%- else -%}
      {# Generate sql to drop all identified schemas #}
      {% set drop_schema_sql -%}

        {% for schema in schemas -%}
          DROP SCHEMA IF EXISTS {{schema}} CASCADE;
        {% endfor %}

      {%- endset %}

      {# Drop schemas #}
      {% do run_query(drop_schema_sql) %}

    {%- endif -%}

  {% endif %}

{% endmacro %}


{% macro databricks__post_ci_cleanup(schema_pattern=target.schema) %}

  {# Get all schemas with the target.schema prefix #}
  {% set schemas = snowplow_utils.get_schemas_by_pattern(schema_pattern~'%') %}

  {% if schemas|length %}

    {%- if target.type in ['databricks'] -%}
      {# Generate sql to drop all identified schemas #}
      {% for schema in schemas -%}
        {%- set drop_schema_sql -%}
          DROP SCHEMA IF EXISTS {{schema}} CASCADE;
        {%- endset -%}

        {% do run_query(drop_schema_sql) %}

      {% endfor %}

    {%- else -%}
      {# Generate sql to drop all identified schemas #}
      {% set drop_schema_sql -%}

        {% for schema in schemas -%}
          DROP SCHEMA IF EXISTS {{schema}} CASCADE;
        {% endfor %}

      {%- endset %}

      {# Drop schemas #}
      {% do run_query(drop_schema_sql) %}

    {%- endif -%}

  {% endif %}

{% endmacro %}

{#
  Spark-specific implementation for post CI cleanup.
#}

{% macro spark__post_ci_cleanup(schema_pattern=target.schema) %}
  {# Retrieve all schemas matching the pattern #}
  {% set schemas = snowplow_utils.get_schemas_by_pattern(schema_pattern ~ "%") %}

  {% if schemas | length > 0 %}
    {% for schema in schemas %}
      {{ log("Processing schema: " ~ schema, info=True) }}
      
      {# Step 1: List all tables in the current schema #}
      {% set tables_query = "SHOW TABLES IN " ~ schema %}
      {% set tables_result = run_query(tables_query) %}
      
      {# Initialize an empty list for tables #}
      {% set table_list = [] %}
      
      {% if tables_result and tables_result.rows %}
        {% for row in tables_result.rows %}
          {% set table = row[1] %}
          {% do table_list.append(table) %}
        {% endfor %}
        
        {# Step 2: Drop each table individually #}
        {% for table in table_list %}
          {% set drop_table_sql = "DROP TABLE IF EXISTS " ~ schema ~ "." ~ table ~ ";" %}
          {% do adapter.execute(drop_table_sql) %}
        {% endfor %}
      {% else %}
      {% endif %}
      
      {# For spark we shouldn't delete the schema as this has the role of the database #}
    {% endfor %}
  {% else %}
    {{ log("No schemas found matching pattern: " ~ schema_pattern, info=True) }}
  {% endif %}
{% endmacro %}
