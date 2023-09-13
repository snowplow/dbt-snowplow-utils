{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}


{% macro base_create_snowplow_events_this_run(sessions_this_run_table='snowplow_base_sessions_this_run', session_identifiers=[{"table" : "events", "field" : "domain_sessionid"}], session_sql=none, session_timestamp='load_tstamp', derived_tstamp_partitioned=true, days_late_allowed=3, max_session_days=3, app_ids=[], snowplow_events_database=none, snowplow_events_schema='atomic', snowplow_events_table='events', entities_or_sdes=none, custom_sql=none) %}
    {{ return(adapter.dispatch('base_create_snowplow_events_this_run', 'snowplow_utils')(sessions_this_run_table, session_identifiers, session_sql, session_timestamp, derived_tstamp_partitioned, days_late_allowed, max_session_days, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, entities_or_sdes, custom_sql)) }}
{% endmacro %}

{% macro default__base_create_snowplow_events_this_run(sessions_this_run_table, session_identifiers, session_sql, session_timestamp, derived_tstamp_partitioned, days_late_allowed, max_session_days, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, entities_or_sdes, custom_sql) %}
    {%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref(sessions_this_run_table),
                                                                          'start_tstamp',
                                                                          'end_tstamp') %}
    {% set sessions_this_run = ref(sessions_this_run_table) %}
    {% set snowplow_events = api.Relation.create(database=snowplow_events_database, schema=snowplow_events_schema, identifier=snowplow_events_table) %}

    {% set events_this_run_query %}
        with identified_events AS (
            select
                {% if session_sql %}
                    {{ session_sql }} as session_identifier,
                {% else -%}
                    COALESCE(
                        {% for identifier in session_identifiers %}
                            {%- if identifier['schema']|lower != 'atomic' -%}
                                {{ snowplow_utils.get_field(identifier['schema'], identifier['field'], 'e', dbt.type_string(), 0) }}
                            {%- else -%}
                                e.{{identifier['field']}}
                            {%- endif -%}
                            ,
                        {%- endfor -%}
                        NULL
                    ) as session_identifier,
                {%- endif %}
                e.*
                {% if custom_sql %}
                    , {{ custom_sql }}
                {% endif %}

            from {{ snowplow_events }} e

        )

        select
            a.*,
            b.user_identifier -- take user_identifier from manifest. This ensures only 1 domain_userid per session.

        from identified_events as a
        inner join {{ sessions_this_run }} as b
        on a.session_identifier = b.session_identifier

        where a.{{ session_timestamp }} <= {{ snowplow_utils.timestamp_add('day', max_session_days, 'b.start_tstamp') }}
        and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'a.dvce_created_tstamp') }}
        and a.{{ session_timestamp }} >= {{ lower_limit }}
        and a.{{ session_timestamp }} <= {{ upper_limit }}

        {% if derived_tstamp_partitioned and target.type == 'bigquery' | as_bool() %}
            and a.derived_tstamp >= {{ snowplow_utils.timestamp_add('hour', -1, lower_limit) }}
            and a.derived_tstamp <= {{ upper_limit }}
        {% endif %}

        and {{ snowplow_utils.app_id_filter(app_ids) }}

        qualify row_number() over (partition by a.event_id order by a.{{ session_timestamp }}, a.dvce_created_tstamp) = 1
    {% endset %}

    {{ return(events_this_run_query) }}

{% endmacro %}

{% macro postgres__base_create_snowplow_events_this_run(sessions_this_run_table, session_identifiers, session_sql, session_timestamp, derived_tstamp_partitioned, days_late_allowed, max_session_days, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, entities_or_sdes, custom_sql) %}
    {%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref(sessions_this_run_table),
                                                                          'start_tstamp',
                                                                          'end_tstamp') %}

    {% if entities_or_sdes %}
        -- check uniqueness of entity/sde names provided
        {% set ent_sde_names = [] %}
        {% for ent_or_sde in entities_or_sdes %}
            {% do ent_sde_names.append(ent_or_sde['schema']) %}
        {% endfor %}
        {% if ent_sde_names | unique | list | length != entities_or_sdes | length %}
            {% do exceptions.raise_compiler_error("There are duplicate schema names in your provided `entities_or_sdes` list. Please correct this before proceeding.")%}
        {% endif %}
    {% endif %}

    {% set sessions_this_run = ref(sessions_this_run_table) %}
    {% set snowplow_events = api.Relation.create(database=snowplow_events_database, schema=snowplow_events_schema, identifier=snowplow_events_table) %}

    {% set events_this_run_query %}
        with

        {% if session_identifiers -%}
            {% for identifier in session_identifiers %}
                {% if identifier['schema']|lower != 'atomic' %}
                    {{ snowplow_utils.get_sde_or_context(snowplow_events_schema, identifier['schema'], lower_limit, upper_limit, identifier['prefix']) }},
                {%- endif -%}
            {% endfor %}
        {% endif %}

        {%- if entities_or_sdes -%}
            {%- for ent_or_sde in entities_or_sdes -%}
                {%- set name = none -%}
                {%- set prefix = none -%}
                {%- set single_entity = true -%}
                {%- if ent_or_sde['schema'] -%}
                    {%- set name = ent_or_sde['schema'] -%}
                {%- else -%}
                    {%- do exceptions.raise_compiler_error("Need to specify the schema name of your Entity or SDE using the {'schema'} attribute in a key-value map.") -%}
                {%- endif -%}
                {%- if ent_or_sde['prefix'] -%}
                    {%- set prefix = ent_or_sde['prefix'] -%}
                {%- else -%}
                    {%- set prefix = name -%}
                {%- endif -%}
                {%- if ent_or_sde['single_entity'] and ent_or_sde['single_entity'] is boolean -%}
                    {%- set single_entity = ent_or_sde['single_entity'] -%}
                {%- endif %}
                {{ snowplow_utils.get_sde_or_context(snowplow_events_schema, name, lower_limit, upper_limit, prefix, single_entity) }},
            {% endfor -%}
        {%- endif %}

        identified_events AS (
            select
                {% if session_sql -%}
                    {{ session_sql }} as session_identifier,
                {% else -%}
                    COALESCE(
                            {% for identifier in session_identifiers %}
                                {%- if identifier['schema']|lower != 'atomic' %}
                                    {% if identifier['alias'] %}{{identifier['alias']}}{% else %}{{identifier['schema']}}{% endif %}.{% if identifier['prefix'] %}{{ identifier['prefix'] }}{% else %}{{ identifier['schema']}}{% endif %}_{{identifier['field']}}
                                {%- else %}
                                    e.{{identifier['field']}}
                                {%- endif -%}
                                ,
                            {%- endfor -%}
                            NULL
                        ) as session_identifier,
                {%- endif %}
                    e.*
                    {% if custom_sql %}
                    , {{ custom_sql }}
                    {%- endif %}

            from {{ snowplow_events }} e
            {% if session_identifiers|length > 0 %}
                {% for identifier in session_identifiers %}
                    {%- if identifier['schema']|lower != 'atomic' -%}
                    left join {{ identifier['schema'] }} {% if identifier['alias'] %}as {{ identifier['alias'] }}{% endif %} on e.event_id = {% if identifier['alias'] %}{{ identifier['alias']}}{% else %}{{ identifier['schema'] }}{% endif %}.{{identifier['prefix']}}__id and e.collector_tstamp = {% if identifier['alias'] %}{{ identifier['alias']}}{% else %}{{ identifier['schema'] }}{% endif %}.{{ identifier['prefix'] }}__tstamp
                    {% endif -%}
                {% endfor %}
            {% endif %}

        ), events_this_run as (

            select
                a.*,
                b.user_identifier, -- take user_identifier from manifest. This ensures only 1 domain_userid per session.
                row_number() over (partition by a.event_id order by a.{{ session_timestamp }}, a.dvce_created_tstamp ) as event_id_dedupe_index,
                count(*) over (partition by a.event_id) as event_id_dedupe_count

            from identified_events as a
            inner join {{ sessions_this_run }} as b
            on a.session_identifier = b.session_identifier

            where a.{{ session_timestamp }} <= {{ snowplow_utils.timestamp_add('day', max_session_days, 'b.start_tstamp') }}
            and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'a.dvce_created_tstamp') }}
            and a.{{ session_timestamp }} >= {{ lower_limit }}
            and a.{{ session_timestamp }} <= {{ upper_limit }}
            and {{ snowplow_utils.app_id_filter(app_ids) }}

        )

        select *

        from events_this_run as e
        {%- if entities_or_sdes -%}
            {% for ent_or_sde in entities_or_sdes -%}
                {%- set name = none -%}
                {%- set prefix = none -%}
                {%- set single_entity = true -%}
                {%- set alias = none -%}
                {%- if ent_or_sde['schema'] -%}
                    {%- set name = ent_or_sde['schema'] -%}
                {%- else -%}
                    {%- do exceptions.raise_compiler_error("Need to specify the schema name of your Entity or SDE using the {'schema'} attribute in a key-value map.") -%}
                {%- endif -%}
                {%- if ent_or_sde['prefix'] -%}
                    {%- set prefix = ent_or_sde['prefix'] -%}
                {%- else -%}
                    {%- set prefix = name -%}
                {%- endif -%}
                {%- if ent_or_sde['single_entity'] and ent_or_sde['single_entity'] is boolean -%}
                    {%- set single_entity = ent_or_sde['single_entity'] -%}
                {%- endif -%}
                {%- if ent_or_sde['alias'] -%}
                    {%- set alias = ent_or_sde['alias'] -%}
                {%- endif %}
                left join {{name}} {% if alias -%} as {{ alias }} {%- endif %} on e.event_id = {% if alias -%} {{ alias }} {%- else -%}{{name}}{%- endif %}.{{prefix}}__id
                and e.collector_tstamp = {% if alias -%} {{ alias }} {%- else -%}{{name}}{%- endif %}.{{prefix}}__tstamp
                {% if not single_entity -%} and mod({% if alias -%} {{ alias }} {%- else -%}{{name}}{%- endif %}.{{prefix}}__index, e.event_id_dedupe_count) = 0{%- endif -%}
            {% endfor %}
        {% endif %}
        where event_id_dedupe_index = 1

    {% endset %}

    {{ return(events_this_run_query) }}

{% endmacro %}
