{% macro create_table_as_sql(dest_table, as_query, replace=false) -%}

  {{ return(adapter.dispatch('create_table_as_sql', ['snowplow_utils'])(dest_table, as_query, replace)) }}

{% endmacro %}

{% macro default__create_table_as_sql(dest_table, as_query, replace) -%}

create {% if replace %} or replace {% endif %} table {{ dest_table }} as {{ as_query }};

{%- endmacro %}


{% macro postgres__create_table_as_sql(dest_table, as_query, replace) -%}
{% if replace %}
drop table if exists {{ dest_table }};
{% endif %}
create table {{ dest_table }} as ( {{ as_query }} );

{%- endmacro %}


{# Given a set of required columns, returns the missing columns in relation
   TODO: Add warning if column order of required columns will not be preserved #}
{% macro get_columns_to_create(relation, required_columns) -%}

  {%- set columns_to_create = [] -%}

  {# map to lower to cater for snowflake returning column names as upper case #}
  {%- set existing_columns = adapter.get_columns_in_relation(relation)|map(attribute='column')|map('lower')|list -%}

  {%- for required_column in required_columns -%}
    {%- if required_column[0] not in existing_columns -%}
      {%- do columns_to_create.append(required_column) -%}
    {%- endif -%}
  {%- endfor -%}

  {{ return(columns_to_create) }}

{%- endmacro %}


{# Adds and missing required columns to relation. SQL only, doesn't run query. 
   Missing columns in required_columns will be added but order might not be preserved #}
{% macro alter_table_sql(relation, required_columns) -%}

  {{ return(adapter.dispatch('alter_table_sql', ['snowplow_utils'])(relation, required_columns)) }}

{% endmacro %}

{% macro default__alter_table_sql(relation, required_columns) -%}

  {%- set columns_to_create = snowplow_utils.get_columns_to_create(relation, required_columns) -%}

  {% if columns_to_create|length %}

    {%- for column in columns_to_create -%}
      alter table {{ relation }}
      add column {{ column[0] }} {{ column[1] }} default null;
    {% endfor -%}

  {% endif %}

{%- endmacro %}

{% macro bigquery__alter_table_sql(relation, required_columns) -%}

  {%- set columns_to_create = snowplow_utils.get_columns_to_create(relation, required_columns) -%}

  {% if columns_to_create|length %}

    alter table {{ relation }}

    {%- for column in columns_to_create -%}
      add column {{ column[0] }} {{ column[1] }} {%- if not loop.last %}, {% else %} ;{% endif %}
    {% endfor -%}

  {% endif %}

{%- endmacro %}


{# Create table SQL command. SQL only, doesn't run query. #}
{% macro create_table_sql(relation, required_columns, if_not_exists=true) -%}

  create table {% if if_not_exists %} if not exists {% endif %} {{ relation }}
  (
  {% for column in required_columns %}
      {{ column[0] }} {{ column[1] }}{% if not loop.last %},{% endif %}
  {% endfor %}
  );

{% endmacro %}


