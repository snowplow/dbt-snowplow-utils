{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro get_field(column_name, field_name, table_alias = none, type = none, array_index = none) %}
    {{ return(adapter.dispatch('get_field', 'snowplow_utils')(column_name, field_name, table_alias, type, array_index)) }}
{% endmacro %}

{% macro bigquery__get_field(column_name, field_name, table_alias = none, type = none, array_index = none) %}
{%- if type -%}cast({%- endif -%}{%- if table_alias -%}{{table_alias}}.{%- endif -%}{{column_name}}{%- if array_index is not none -%}[SAFE_OFFSET({{array_index}})]{%- endif -%}.{{field_name}}{%- if type %} as {{type}}){%- endif -%}
{% endmacro %}

{% macro spark__get_field(column_name, field_name, table_alias = none, type = none, array_index = none) %}
{%- if table_alias -%}{{table_alias}}.{%- endif -%}{{column_name}}{%- if array_index is not none -%}[{{array_index}}]{%- endif -%}.{{field_name}}{%- if type -%}::{{type}}{%- endif -%}
{% endmacro %}

{% macro snowflake__get_field(column_name, field_name, table_alias = none, type = none, array_index = none) %}
{%- if type is none and execute -%}
{% do exceptions.warn("Warning: macro snowplow_utils.get_field is being used without a type provided, Snowflake will return a variant column in this case which is unlikely to be what you want.") %}
{%- endif -%}
{%- if table_alias -%}{{table_alias}}.{%- endif -%}{{column_name}}{%- if array_index is not none -%}[{{array_index}}]{%- endif -%}:{{field_name}}{%- if type -%}::{{type}}{%- endif -%}
{% endmacro %}


{% macro default__get_field(column_name, field_name, table_alias = none, type = none, array_index = none) %}

{% if execute %}
    {% do exceptions.raise_compiler_error('Macro get_field only supports Bigquery, Snowflake, Spark, and Databricks, it is not supported for ' ~ target.type) %}
{% endif %}

{% endmacro %}
