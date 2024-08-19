{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro get_sde_or_context(schema, identifier, lower_limit, upper_limit, prefix = none, single_entity = true, database = target.database) %}
    {{ return(adapter.dispatch('get_sde_or_context', 'snowplow_utils')(schema, identifier, lower_limit, upper_limit, prefix, single_entity, database)) }}
{% endmacro %}

{% macro default__get_sde_or_context(schema, identifier, lower_limit, upper_limit, prefix = none, single_entity = true, database = target.database) %}
    {% if execute %}
        {% do exceptions.raise_compiler_error('Macro get_sde_or_context is only for Postgres or Redshift, it is not supported for' ~ target.type) %}
    {% endif %}
{% endmacro %}


{% macro postgres__get_sde_or_context(schema, identifier, lower_limit, upper_limit, prefix = none, single_entity = true, database = target.database) %}
    {# Create a relation from the inputs then get all columns in that context/sde table #}
    {% set relation = api.Relation.create(database = database, schema = schema, identifier = identifier) %}
    {# Get the schema name to be able to alias the timestamp and id #}
    {% set schema_get_query %}
        select schema_name from {{ relation }}
        limit 1
    {% endset %}
    {%- set schema_name = dbt_utils.get_single_value(schema_get_query) -%}
    {# Get the columns to loop over #}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}

    {% set sql %}
        {{'dd_' ~ identifier }} as (
            select
            {# Get all columns that aren't related to the schema itself #}
            {%- for col in columns -%}
                {%- if col.name not in ['schema_vendor', 'schema_name', 'schema_format', 'schema_version', 'ref_root', 'ref_tree', 'ref_parent'] %}
                    {{ col.quoted }},
                {%- endif -%}
            {% endfor %}
            {% if single_entity %}
                row_number() over (partition by root_id order by root_tstamp) as dedupe_index -- keep the first event for that root_id
            {% else %}
                row_number() over (partition by {% for item in columns | map(attribute='quoted') %}{{item}}{%- if not loop.last %},{% endif %}{% endfor -%} ) as dedupe_index -- get the index across all columns for the entity
            {% endif %}
            from
                {{ relation }}
            {% if upper_limit and lower_limit -%}
                where
                    root_tstamp >= {{ lower_limit }}
                    and root_tstamp <= {{ upper_limit }}
            {% endif %}
        ),

        {{identifier}} as (
            select
            {%- for col in columns -%}
                {%- if col.name | lower not in ['schema_vendor', 'schema_name', 'schema_format', 'schema_version', 'ref_root', 'ref_tree', 'ref_parent', 'root_tstamp', 'root_id'] %}
                    {{ col.quoted }}{% if prefix %} as {{ adapter.quote(prefix ~ '_' ~ col.name) }}{% endif -%},
                {%- endif -%}
            {% endfor -%}
            {# Rename columns that we know exist in every schema based table #}
            {% if not single_entity %}
                dedupe_index as {% if prefix %}{{ adapter.quote(prefix ~ '__index') }}{% else %}{{ adapter.quote(schema_name ~ '__index') }}{% endif %}, -- keep track of this for the join
            {% endif %}
                root_tstamp as {% if prefix %}{{ adapter.quote(prefix ~ '__tstamp') }}{% else %}{{ adapter.quote(schema_name ~ '__tstamp') }}{% endif %},
                root_id as {% if prefix %}{{ adapter.quote(prefix ~ '__id') }}{% else %}{{ adapter.quote(schema_name ~ '__id') }}{% endif %}
            from
                {{'dd_' ~ identifier }}
            {% if single_entity %}
            where
                dedupe_index = 1
            {% endif %}
        )

    {% endset %}
    {{ return(sql) }}
{% endmacro %}
