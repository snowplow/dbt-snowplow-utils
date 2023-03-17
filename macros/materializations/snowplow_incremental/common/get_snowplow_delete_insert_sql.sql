{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro get_snowplow_delete_insert_sql(target, source, unique_key, dest_cols_csv, predicates) -%}
  {{ adapter.dispatch('get_snowplow_delete_insert_sql', 'snowplow_utils')(target, source, unique_key, dest_cols_csv, predicates) }}
{%- endmacro %}

{% macro default__get_snowplow_delete_insert_sql(target, source, unique_key, dest_cols_csv, predicates) -%}

    delete from {{ target }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ source }}
    )
    {% if predicates %} and {{ predicates | join(' and ') }} {% endif %};

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    );

{%- endmacro %}

{# dbt v0.21 enabled autocommit for Snowflake. Wrap in transaction. #}
{% macro snowflake__get_snowplow_delete_insert_sql(target, source, unique_key, dest_cols_csv, predicates) -%}

    begin;
    {{ snowplow_utils.default__get_snowplow_delete_insert_sql(target, source, unique_key, dest_cols_csv, predicates) }}
    commit;

{%- endmacro %}
