{% macro create_table_as_sql(dest_table, as_query, replace=false) -%}

  {{ return(adapter.dispatch('create_table_as_sql', ['snowplow_utils'])(dest_table, as_query, replace)) }}

{% endmacro %}

{% macro default__create_table_as_sql(dest_table, as_query, replace) -%}

create {% if replace %} or replace {% endif %} table {{ dest_table }} as {{ as_query }};

{%- endmacro %}

{% macro redshift__create_table_as_sql(dest_table, as_query, replace) -%}
{% if replace %}
drop table if exists {{ dest_table }};
{% endif %}
create table {{ dest_table }} as ( {{ as_query }} );

{%- endmacro %}


