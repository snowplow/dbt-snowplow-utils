{#
 Takes care of harmonising cross-db unnesting.
 #}

{%- macro unnest(id_column, unnest_column, field_alias, source_table) -%}
    {{ return(adapter.dispatch('unnest', 'snowplow_utils')(id_column, unnest_column, field_alias, source_table)) }}
{%- endmacro -%}

{% macro default__unnest(id_column, unnest_column, field_alias, source_table) %}
    select {{ id_column }}, explode({{ unnest_column }}) as {{ field_alias }}
    from {{ source_table }}
{% endmacro %}

{% macro bigquery__unnest(id_column, unnest_column, field_alias, source_table) %}
    select {{ id_column }}, r as {{ field_alias }}
    from {{ source_table }} t, unnest(t.{{ unnest_column }}) r
{% endmacro %}

{% macro snowflake__unnest(id_column, unnest_column, field_alias, source_table) %}
    select t.{{ id_column }}, r.value as {{ field_alias }}
    from {{ source_table }} t, table(flatten(t.{{ unnest_column }})) r
{% endmacro %}

{% macro postgres__unnest(id_column, unnest_column, field_alias, source_table) %}
    select {{ id_column }}, cast(trim(unnest({{ unnest_column }})) as {{ dbt_utils.type_int() }}) as {{ field_alias }}
    from {{ source_table }}
{% endmacro %}

{% macro redshift__unnest(id_column, unnest_column, field_alias, source_table) %}
    select {{ id_column }}, {{ field_alias }}
    from {{ source_table }} p, p.{{ unnest_column }} as {{ field_alias }}
{% endmacro %}
