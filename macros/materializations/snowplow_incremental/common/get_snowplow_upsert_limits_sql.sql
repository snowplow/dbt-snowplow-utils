{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) -%}
  {{ adapter.dispatch('get_snowplow_upsert_limits_sql', 'snowplow_utils')(tmp_relation, upsert_date_key, disable_upsert_lookback) }}
{%- endmacro %}


{% macro default__get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) -%}

  {% set upsert_limits_sql -%}

    {% if disable_upsert_lookback %}
      with vars as (
        select min({{ upsert_date_key }}) as lower_limit,
               max({{ upsert_date_key }}) as upper_limit
        from {{ tmp_relation }}
      )
    {% else %}
      with vars as (
        select
              {{ dateadd('day',
                                   -var("snowplow__upsert_lookback_days", 30),
                                   'min('~upsert_date_key~')') }} as lower_limit,
                   max({{ upsert_date_key }}) as upper_limit
        from {{ tmp_relation }}
      )
    {% endif %}

  {%- endset %}

  {{ return(upsert_limits_sql) }}

{%- endmacro %}


{% macro bigquery__get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) -%}

  {# partition_by supplied as upsert_date_key for BigQuery. Rename for clarity #}
  {%- set partition_by = upsert_date_key -%}

  {% set upsert_limits_sql -%}

    {% if disable_upsert_lookback %}
      set (dbt_partition_lower_limit, dbt_partition_upper_limit) = (
            select as struct
                   min({{ partition_by.field }}) as lower_limit,
                   max({{ partition_by.field }}) as upper_limit
            from {{ tmp_relation }}
      );
    {% else %}
      set (dbt_partition_lower_limit, dbt_partition_upper_limit) = (
            select as struct
                   cast({{ dateadd('day',
                                             -var("snowplow__upsert_lookback_days", 30),
                                             'min('~partition_by.field~')') }} as {{ partition_by.data_type }}) as lower_limit,
                   max({{ partition_by.field }}) as upper_limit
            from {{ tmp_relation }}
      );
    {% endif %}

  {%- endset %}

  {{ return(upsert_limits_sql) }}

{%- endmacro %}


{% macro snowflake__get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) -%}

  {% set upsert_limits_sql -%}

    {% if disable_upsert_lookback %}
      set (dbt_partition_lower_limit, dbt_partition_upper_limit) = (
            select
              min({{ upsert_date_key }}) as lower_limit,
              max({{ upsert_date_key }}) as upper_limit
            from {{ tmp_relation }}
      );
    {% else %}
      set (dbt_partition_lower_limit, dbt_partition_upper_limit) = (
            select
              {{ dateadd('day',
                                   -var("snowplow__upsert_lookback_days", 30),
                                   'min('~upsert_date_key~')') }} as lower_limit,
                   max({{ upsert_date_key }}) as upper_limit
            from {{ tmp_relation }}
      );
    {% endif %}

  {%- endset %}

  {{ return(upsert_limits_sql) }}

{%- endmacro %}

{% macro databricks__get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) -%}

  {% set upsert_limits_sql -%}

    {% if disable_upsert_lookback %}

      select
        min({{ upsert_date_key }}) as lower_limit,
        max({{ upsert_date_key }}) as upper_limit
      from {{ tmp_relation }}
      ;
    {% else %}
      select
        {{ dateadd('day',
                              -var("snowplow__upsert_lookback_days", 30),
                              'min('~upsert_date_key~')') }} as lower_limit,
        max({{ upsert_date_key }}) as upper_limit
      from {{ tmp_relation }}
      ;
    {% endif %}

  {%- endset %}

  {{ return(upsert_limits_sql) }}

{%- endmacro %}

{%- macro spark__get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) -%}
    {{ return(snowplow_utils.databricks__get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback)) }}
{%- endmacro %}
