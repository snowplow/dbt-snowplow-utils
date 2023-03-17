{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{# Destructive macro. Use with care! #}

{% macro post_ci_cleanup(schema_pattern=target.schema) %}

  {# Get all schemas with the target.schema prefix #}
  {% set schemas = snowplow_utils.get_schemas_by_pattern(schema_pattern~'%') %}

  {% if schemas|length %}

    {%- if target.type in ['databricks', 'spark'] -%}
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
