{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro default__get_merge_sql(target_tb, source, unique_key, dest_columns, incremental_predicates = none) -%}
    {# Set default predicates to pass on #}
    {%- set predicate_override = "" -%}
    {%- set orig_predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}

    {%- set optimize = config.get('snowplow_optimize') -%}
    {% if optimize %}

        -- run some queries to dynamically determine the min + max of this 'upsert_date_key' in the new data
        {%- set date_column = config.require('upsert_date_key') -%}
        {%- set disable_upsert_lookback = config.get('disable_upsert_lookback') -%} {# We do this for late arriving data possibly e.g. shifting a session start earlier #}

        {# We need the type of the column to get the correct cast of the default value #}
        {%- set columns = adapter.get_columns_in_relation(this) -%}
        {%- set date_type = [] %} {# Because you can't assign values in a loop otherwise #}
        {%- for col in columns -%}
            {%- if col.column|lower == date_column|lower -%}
                {% do date_type.append(col.dtype) %}
            {%- endif %}
        {%- endfor %}

        {% set get_limits_query %}

            {% if disable_upsert_lookback %}
                select
                    coalesce(min({{ date_column }}), cast({{ dbt.current_timestamp() }} as {{ date_type[0] }}) ) as lower_limit,
                    coalesce(max({{ date_column }}), cast({{ dbt.current_timestamp() }} as {{ date_type[0] }})) as upper_limit
                from {{ source }}
            {% else %}
                select
                    coalesce(cast({{ dateadd('day', -var("snowplow__upsert_lookback_days", 30), 'min('~date_column~')') }} as {{ date_type[0] }}), cast({{ dbt.current_timestamp() }} as {{ date_type[0] }})) as lower_limit,
                    coalesce(max({{ date_column }}), cast({{ dbt.current_timestamp() }} as {{ date_type[0] }})) as upper_limit
                from {{ source }}
            {% endif %}
        {% endset %}

        {% set limits = run_query(get_limits_query)[0] %}
        {% set lower_limit, upper_limit = limits[0], limits[1] %}

        -- use those calculated min + max values to limit 'target' scan, to only the days with new data
        {% set predicate_override %}
            DBT_INTERNAL_DEST.{{ date_column }} between '{{ lower_limit }}' and '{{ upper_limit }}'
        {% endset %}
    {% endif %}

    {# Combine predicates with user provided ones #}
    {% set predicates = [predicate_override] + orig_predicates if predicate_override else orig_predicates %}
    -- standard merge from here
    {% if target.type in ['databricks', 'spark'] -%}
        {% set merge_sql = spark__get_merge_sql(target_tb, source, unique_key, dest_columns, predicates) %}
    {% else %}
        {% set merge_sql = dbt.default__get_merge_sql(target_tb, source, unique_key, dest_columns, predicates) %}
    {% endif %}

    {{ return(merge_sql) }}

{% endmacro %}

{% macro default__get_delete_insert_merge_sql(target_tb, source, unique_key, dest_columns, incremental_predicates) -%}
    {# Set default predicates to pass on #}
    {%- set predicate_override = "" -%}
    {%- set orig_predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set optimize = config.get('snowplow_optimize') -%}
    {% if optimize %}
        -- run some queries to dynamically determine the min + max of this 'upsert_date_key' in the new data
        {%- set date_column = config.require('upsert_date_key') -%}
        {%- set disable_upsert_lookback = config.get('disable_upsert_lookback') -%}

        {# We need the type of the column to get the correct cast of the default value is needed #}
        {%- set columns = adapter.get_columns_in_relation(this) -%}
        {%- set date_type = [] %} {# Because you can't assign values in a loop otherwise #}
        {%- for col in columns -%}
            {%- if col.column|lower == date_column|lower -%}
                {% do date_type.append(col.dtype) %}
            {%- endif %}
        {%- endfor %}

        {% set get_limits_query %}
            {% if disable_upsert_lookback %}
                select
                    coalesce(min({{ date_column }}), cast({{ dbt.current_timestamp() }} as {{ date_type[0] }}) ) as lower_limit,
                    coalesce(max({{ date_column }}), cast({{ dbt.current_timestamp() }} as {{ date_type[0] }}))as upper_limit
                from {{ source }}
            {% else %}
                select
                    coalesce(cast({{ dateadd('day', -var("snowplow__upsert_lookback_days", 30), 'min('~date_column~')') }} as {{ date_type[0] }}), cast({{ dbt.current_timestamp() }} as {{ date_type[0] }})) as lower_limit,
                    coalesce(max({{ date_column }}), cast({{ dbt.current_timestamp() }} as {{ date_type[0] }})) as upper_limit
                from {{ source }}
            {% endif %}
        {% endset %}

        {% set limits = run_query(get_limits_query)[0] %}
        {% set lower_limit, upper_limit = limits[0], limits[1] %}
        -- use those calculated min + max values to limit 'target' scan, to only the days with new data
        {% set predicate_override %}
            {{ date_column }} between '{{ lower_limit }}' and '{{ upper_limit }}'
        {% endset %}
    {% endif %}
    {# Combine predicates with user provided ones #}
    {% set predicates = [predicate_override] + orig_predicates if predicate_override else orig_predicates %}
    -- standard merge from here
    {% if target.type in ['databricks', 'spark'] -%}
        {% set merge_sql = spark__get_delete_insert_merge_sql(target_tb, source, unique_key, dest_columns, predicates) %}
    {% else %}
        {% set merge_sql = dbt.default__get_delete_insert_merge_sql(target_tb, source, unique_key, dest_columns, predicates) %}
    {% endif %}

    {{ return(merge_sql) }}

{% endmacro %}
