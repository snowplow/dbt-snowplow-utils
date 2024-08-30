{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro get_field(column_name, field_name, table_alias = none, type = none, array_index = none, relation = none) %}
    {{ return(adapter.dispatch('get_field', 'snowplow_utils')(column_name, field_name, table_alias, type, array_index, relation)) }}
{% endmacro %}

{% macro bigquery__get_field(column_name, field_name, table_alias = none, type = none, array_index = none, relation = none) %}

{% if '*' in column_name %}
    {{ snowplow_utils.get_optional_fields(
        enabled=true,
        fields=[{'field': field_name, 'dtype': type or 'string' }],
        col_prefix=column_name|replace('_*', ''),
        relation=relation,
        relation_alias=table_alias,
        include_field_alias=false
    ) }}

{% else %}
{%- if type -%}cast({%- endif -%}{%- if table_alias -%}{{table_alias}}.{%- endif -%}{{column_name}}{%- if array_index is not none -%}[SAFE_OFFSET({{array_index}})]{%- endif -%}.{{field_name}}{%- if type %} as {{type}}){%- endif -%}
{% endif %}
{% endmacro %}

{% macro spark__get_field(column_name, field_name, table_alias = none, type = none, array_index = none, relation = none) %}
{% if '*' in column_name %}
    {% do exceptions.raise_compiler_error('Wildcard schema versions are only supported for Bigquery, they are not supported for ' ~ target.type) %}
{% else %}
    {%- if type is none -%}
        {%- if table_alias -%}{{table_alias}}.{%- endif -%}{{column_name}}{%- if array_index is not none -%}[{{array_index}}]{%- endif -%}.{{field_name}}
    {%- else -%}
        CAST(
            {%- if table_alias -%}{{table_alias}}.{%- endif -%}{{column_name}}{%- if array_index is not none -%}[{{array_index}}]{%- endif -%}.{{field_name}} AS {{type}}
        )
    {%- endif -%}
{% endif %}
{% endmacro %}

{% macro snowflake__get_field(column_name, field_name, table_alias = none, type = none, array_index = none, relation = none) %}
{% if '*' in column_name %}
    {% do exceptions.raise_compiler_error('Wildcard schema versions are only supported for Bigquery, they are not supported for ' ~ target.type) %}
{% else %}
    {%- if type is none and execute -%}
        {% do exceptions.warn("Warning: macro snowplow_utils.get_field is being used without a type provided, Snowflake will return a variant column in this case which is unlikely to be what you want.") %}
    {%- endif -%}
    {%- if table_alias -%}{{table_alias}}.{%- endif -%}{{column_name}}{%- if array_index is not none -%}[{{array_index}}]{%- endif -%}:{{field_name}}{%- if type -%}::{{type}}{%- endif -%}
{% endif %}
{% endmacro %}


{% macro default__get_field(column_name, field_name, table_alias = none, type = none, array_index = none, relation = none) %}

{% if execute %}
    {% do exceptions.raise_compiler_error('Macro get_field only supports Bigquery, Snowflake, Spark, and Databricks, it is not supported for ' ~ target.type) %}
{% endif %}

{% endmacro %}
