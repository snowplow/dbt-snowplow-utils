{#
 Takes care of harmonising cross-db list_agg, string_agg type functions.
 #}

{%- macro get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) -%}

  {{ return(adapter.dispatch('get_string_agg', 'snowplow_utils')(string_column, column_prefix, separator, order_by_column, sort_numeric)) }}

{%- endmacro -%}

{% macro default__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    listagg({{column_prefix}}.{{string_column}}::varchar, '{{separator}}') within group (order by to_numeric({{column_prefix}}.{{order_by_column}}))

  {%- else %}
    listagg({{column_prefix}}.{{string_column}}, '{{separator}}') within group (order by {{column_prefix}}.{{order_by_column}})

  {%- endif -%}

{% endmacro %}

{% macro bigquery__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    string_agg(cast({{column_prefix}}.{{string_column}} as string), '{{separator}}' order by cast({{column_prefix}}.{{order_by_column}} as numeric))

  {%- else %}
    string_agg(cast({{column_prefix}}.{{string_column}} as string), '{{separator}}' order by {{column_prefix}}.{{order_by_column}})

  {%- endif -%}

{% endmacro %}

{% macro databricks__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    array_join(array_sort(collect_list(cast({{column_prefix}}.{{string_column}} as numeric))), '{{separator}}')

  {%- else %}
    array_join(array_sort(collect_list({{column_prefix}}.{{string_column}})), '{{separator}}')

  {%- endif -%}

{% endmacro %}

{% macro postgres__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    string_agg({{column_prefix}}.{{string_column}}::varchar, '{{separator}}' order by {{column_prefix}}.{{order_by_column}}::decimal)

  {%- else %}
    string_agg({{column_prefix}}.{{string_column}}::varchar, '{{separator}}' order by {{column_prefix}}.{{order_by_column}})

  {%- endif -%}

{% endmacro %}

{% macro redshift__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    listagg({{column_prefix}}.{{string_column}}::varchar, '{{separator}}') within group (order by text_to_numeric_alt({{column_prefix}}.{{order_by_column}}))

  {%- else %}
    listagg({{column_prefix}}.{{string_column}}::varchar, '{{separator}}') within group (order by {{column_prefix}}.{{order_by_column}})

  {%- endif -%}

{% endmacro %}

{% macro spark__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    array_join(array_sort(collect_list(cast({{column_prefix}}.{{string_column}} as numeric))), '{{separator}}')

  {%- else %}
    array_join(array_sort(collect_list({{column_prefix}}.{{string_column}})), '{{separator}}')

  {%- endif -%}

{% endmacro %}
