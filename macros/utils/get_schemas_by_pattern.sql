{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro get_schemas_by_pattern(schema_pattern=target.schema) %}
    {{ return(adapter.dispatch('get_schemas_by_pattern', 'snowplow_utils')
        (schema_pattern)) }}
{% endmacro %}

{% macro default__get_schemas_by_pattern(schema_pattern) %}

    {% set get_tables_sql = dbt_utils.get_tables_by_pattern_sql(schema_pattern, table_pattern='%') %}
    {% set results = [] if get_tables_sql.isspace() else run_query(get_tables_sql) %}
    {% set schemas = results|map(attribute='table_schema')|unique|list %}
    {{ return(schemas) }}

{% endmacro %}

{% macro spark__get_schemas_by_pattern(schema_pattern) %}
    {# 
      Databricks/Spark uses a regex on SHOW SCHEMAS and doesn't have an information schema in hive_metastore.
      Replace '%' with '*' for Spark's pattern matching.
    #}
    {%- set adjusted_schema_pattern = schema_pattern | replace("%", "*") -%}

    {# Construct the SHOW SCHEMAS LIKE query #}
    {%- set get_schemas_sql = "SHOW SCHEMAS LIKE '" ~ adjusted_schema_pattern ~ "'" -%}

    {# Execute the query and fetch results #}
    {% set results = run_query(get_schemas_sql) %}

    {# Extract schema names from the results #}
    {% set schemas = results.columns[0].values() | unique | list %}

    {{ return(schemas) }}

{% endmacro %}
