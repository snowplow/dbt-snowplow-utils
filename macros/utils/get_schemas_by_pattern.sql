{% macro get_schemas_by_pattern(schema_pattern) %}
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
    {# databricks/spark uses a regex on SHOW SCHEMAS and doesn't have an information schema in hive_metastore #}
    {%- set schema_pattern= dbt.replace(schema_pattern, "%", "*") -%}

    {# Get all schemas with the target.schema prefix #}
    {%- set get_schemas_sql -%}
        SHOW SCHEMAS LIKE '{{schema_pattern}}';
    {%- endset -%}

    {% set results = run_query(get_schemas_sql) %}
    {% set schemas = results|map(attribute='databaseName')|unique|list %}

    {{ return(schemas) }}

{% endmacro %}
