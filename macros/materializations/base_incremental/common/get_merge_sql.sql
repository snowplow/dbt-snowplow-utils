{% macro default__get_merge_sql(target_tb, source, unique_key, dest_columns, incremental_predicates = none) -%}
    {# Set default predicates to pass on #}
    {%- set predicate_override = "" -%}
    {%- set orig_predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}

    {%- set optimise = config.get('snowplow_optimise') -%}
    {% if optimise %}
        -- run some queries to dynamically determine the min + max of this 'upsert_date_key' in the new data
        {%- set date_column = config.require('upsert_date_key') -%}
        {%- set disable_upsert_lookback = config.get('disable_upsert_lookback') -%} {# We do this for late arriving data possibly e.g. shifting a session start earlier #}
        {% set get_limits_query %}
            {% if disable_upsert_lookback %}
                select
                    min({{ date_column }}) as lower_limit,
                    max({{ date_column }}) as upper_limit
                from {{ source }}
            {% else %}
                select
                    {{ dateadd('day', -var("snowplow__upsert_lookback_days", 30), 'min('~date_column~')') }} as lower_limit,
                    max({{ date_column }}) as upper_limit
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
    {%- set optimise = config.get('snowplow_optimise') -%}
    {% if optimise %}
        -- run some queries to dynamically determine the min + max of this 'upsert_date_key' in the new data
        {%- set date_column = config.require('upsert_date_key') -%}
        {%- set disable_upsert_lookback = config.get('disable_upsert_lookback') -%}
        {% set get_limits_query %}
            {% if disable_upsert_lookback %}
                select
                    min({{ date_column }}) as lower_limit,
                    max({{ date_column }}) as upper_limit
                from {{ source }}
            {% else %}
                select
                    {{ dateadd('day', -var("snowplow__upsert_lookback_days", 30), 'min('~date_column~')') }} as lower_limit,
                    max({{ date_column }}) as upper_limit
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
