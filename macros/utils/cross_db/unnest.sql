{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
    Takes care of harmonising cross-db unnesting.
#}

{%- macro unnest(id_column, unnest_column, field_alias, source_table, with_index=false) -%}
    {{ return(adapter.dispatch('unnest', 'snowplow_utils')(id_column, unnest_column, field_alias, source_table, with_index)) }}
{%- endmacro -%}

{% macro default__unnest(id_column, unnest_column, field_alias, source_table, with_index=false) %}
    {% if with_index %}
        select {{ id_column }}, posexplode({{ unnest_column }}) as (source_index, {{ field_alias }})
    {% else %}
        select {{ id_column }}, explode({{ unnest_column }}) as {{ field_alias }}
    {% endif %}
        from {{ source_table }}
{% endmacro %}

{% macro bigquery__unnest(id_column, unnest_column, field_alias, source_table, with_index=false) %}
    select {{ id_column }}, r as {{ field_alias }} {% if with_index %}, source_index {% endif %}
    from {{ source_table }} t, unnest(t.{{ unnest_column }}) r {% if with_index %} WITH OFFSET AS source_index {% endif %}
{% endmacro %}

{% macro snowflake__unnest(id_column, unnest_column, field_alias, source_table, with_index=false) %}
    select t.{{ id_column }}, replace(r.value, '"', '') as {{ field_alias }}
    {% if with_index %}, r.index as source_index {% endif %}
    from {{ source_table }} t, table(flatten(t.{{ unnest_column }})) r
{% endmacro %}

{% macro postgres__unnest(id_column, unnest_column, field_alias, source_table, with_index=false) %}
    select {{ id_column }}, trim(unnest({{ unnest_column }})) as {{ field_alias }}
    from {{ source_table }}
{% endmacro %}

{% macro redshift__unnest(id_column, unnest_column, field_alias, source_table, with_index=false) %}
    select {{ id_column }}, {{ field_alias }} {% if with_index %} , index as source_index {% endif %} 
    from {{ source_table }} p, p.{{ unnest_column }} as {{ field_alias }}{% if with_index %}  at index {% endif %}
{% endmacro %}
