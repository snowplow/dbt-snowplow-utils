{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# string  -------------------------------------------------     #}

{%- macro type_max_string() -%}
        {{ return(adapter.dispatch('type_max_string', 'snowplow_utils')()) }}
{%- endmacro -%}

{% macro default__type_max_string() %}
    {{ dbt.type_string() }}
{% endmacro %}
{# Redshift is the only warehouse that does not have a max length string generated by default (256 chars instead) #}
{% macro redshift__type_max_string() %}
    varchar(max)
{% endmacro %}
