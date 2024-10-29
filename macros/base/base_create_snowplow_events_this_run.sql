{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro base_create_snowplow_events_this_run(sessions_this_run_table='snowplow_base_sessions_this_run', session_identifiers=[{"schema" : "atomic", "field" : "domain_sessionid"}], session_sql=none, session_timestamp='load_tstamp', derived_tstamp_partitioned=true, days_late_allowed=3, max_session_days=3, app_ids=[], snowplow_events_database=none, snowplow_events_schema='atomic', snowplow_events_table='events', entities_or_sdes=none, custom_sql=none, allow_null_dvce_tstamps=false) %}
    {{ return(adapter.dispatch('base_create_snowplow_events_this_run', 'snowplow_utils')(sessions_this_run_table, session_identifiers, session_sql, session_timestamp, derived_tstamp_partitioned, days_late_allowed, max_session_days, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, entities_or_sdes, custom_sql, allow_null_dvce_tstamps)) }}
{% endmacro %}

{% macro default__base_create_snowplow_events_this_run(sessions_this_run_table, session_identifiers, session_sql, session_timestamp, derived_tstamp_partitioned, days_late_allowed, max_session_days, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, entities_or_sdes, custom_sql, allow_null_dvce_tstamps) %}
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
                                {{ snowplow_utils.get_field(identifier['schema'], identifier['field'], 'e', dbt.type_string(), 0, snowplow_events) }}
                            {%- else -%}
                                e.{{identifier['field']}}
                            {%- endif -%}
                            ,
                        {%- endfor -%}
                        NULL
                    ) as session_identifier,
                {%- endif %}
                e.*

            from {{ snowplow_events }} e

        )

        select
            a.*
            ,b.user_identifier -- take user_identifier from manifest. This ensures only 1 domain_userid per session.
            {% if custom_sql %}
                , {{ custom_sql }}
            {% endif %}

        from identified_events as a
        inner join {{ sessions_this_run }} as b
        on a.session_identifier = b.session_identifier

        where a.{{ session_timestamp }} <= {{ snowplow_utils.timestamp_add('day', max_session_days, 'b.start_tstamp') }}
        
        {% if allow_null_dvce_tstamps %}
            and coalesce(a.dvce_sent_tstamp, a.collector_tstamp) <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'coalesce(a.dvce_created_tstamp, a.collector_tstamp)') }}
        {% else %}
            and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'a.dvce_created_tstamp') }}
        {% endif %}
        
        and a.{{ session_timestamp }} >= {{ lower_limit }}
        and a.{{ session_timestamp }} <= {{ upper_limit }}
        and a.{{ session_timestamp }} >= b.start_tstamp -- deal with late loading events

        {% if derived_tstamp_partitioned and target.type == 'bigquery' | as_bool() %}
            and a.derived_tstamp >= {{ snowplow_utils.timestamp_add('hour', -1, lower_limit) }}
            and a.derived_tstamp <= {{ upper_limit }}
        {% endif %}

        and {{ snowplow_utils.app_id_filter(app_ids) }}

        qualify row_number() over (partition by a.event_id order by a.{{ session_timestamp }}, a.dvce_created_tstamp) = 1
    {% endset %}

    {{ return(events_this_run_query) }}

{% endmacro %}

{% macro postgres__base_create_snowplow_events_this_run(sessions_this_run_table, session_identifiers, session_sql, session_timestamp, derived_tstamp_partitioned, days_late_allowed, max_session_days, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, entities_or_sdes, custom_sql, allow_null_dvce_tstamps) %}
    {%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref(sessions_this_run_table),
                                                                            'start_tstamp',
                                                                            'end_tstamp') %}


    {# Get all the session and user contexts extracted and ready to join later #}
    {% set unique_session_identifiers = dict() %} {# need to avoid duplicate contexts when values come from the same one, so just use the first of that context #}

    {% if session_identifiers %}
        {% for identifier in session_identifiers %}
            {% if identifier['schema']|lower != 'atomic' and identifier['schema'] not in unique_session_identifiers %}
                {% do unique_session_identifiers.update({identifier['schema']: identifier}) %}
            {%- endif -%}
            {% if identifier['schema'] in unique_session_identifiers.keys() %}
                {% if identifier['alias'] != unique_session_identifiers[identifier['schema']]['alias'] or identifier['prefix'] != unique_session_identifiers[identifier['schema']]['prefix']  %}
                    {% do exceptions.warn("Snowplow Warning: Duplicate context ( " ~ identifier['schema'] ~" ) detected for session identifiers, using first alias and prefix provided ( " ~ unique_session_identifiers[identifier['schema']] ~ " ) in base events this run.") %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {# check uniqueness of entity/sde names provided, warn those also in session identifiers #}
    {% if entities_or_sdes %}
        {% set ent_sde_names = [] %}
        {% for ent_or_sde in entities_or_sdes %}
            {% do ent_sde_names.append(ent_or_sde['schema']) %}
            {% if ent_or_sde['schema'] in unique_session_identifiers.keys() %}
                {% if ent_or_sde['alias'] != unique_session_identifiers[ent_or_sde['schema']]['alias'] or ent_or_sde['prefix'] != unique_session_identifiers[ent_or_sde['schema']]['prefix']  %}
                    {% do exceptions.warn("Snowplow Warning: Context or SDE ( " ~ ent_or_sde['schema'] ~ " ) used for session_identifier is being included, using alias and prefix from session_identifier ( " ~ unique_session_identifiers[ent_or_sde['schema']] ~ " ).") %}
                {% endif %}
            {% endif %}
        {% endfor %}
        {% if ent_sde_names | unique | list | length != entities_or_sdes | length %}
            {% do exceptions.raise_compiler_error("There are duplicate schema names in your provided `entities_or_sdes` list. Please correct this before proceeding.")%}
        {% endif %}
    {% endif %}

    {% set sessions_this_run = ref(sessions_this_run_table) %}
    {% set snowplow_events = api.Relation.create(database=snowplow_events_database, schema=snowplow_events_schema, identifier=snowplow_events_table) %}

    {% set events_this_run_query %}
        with

        {# Extract the session identifier contexts into CTEs #}
        {% if unique_session_identifiers -%}
            {% for identifier in unique_session_identifiers.values() %}
                {% if identifier['schema']|lower != 'atomic' %}
                    {{ snowplow_utils.get_sde_or_context(snowplow_events_schema, identifier['schema'], lower_limit, upper_limit, identifier['prefix'], database=snowplow_events_database) }},
                {%- endif -%}
            {% endfor %}
        {% endif %}

        {# Extract the entitity/sde contexts into CTEs UNLESS they are in the session already #}
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
                {% if ent_or_sde['schema'] not in unique_session_identifiers.keys() %} {# Exclude any that we have already made above #}
                    {{ snowplow_utils.get_sde_or_context(snowplow_events_schema, name, lower_limit, upper_limit, prefix, single_entity, database=snowplow_events_database) }},
                {% endif %}
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
                                    {# Use the parsed version of the context to ensure we have the right alias and prefix #}
                                    {% set uniq_iden = unique_session_identifiers[identifier['schema']] %}
                                    {% if uniq_iden['alias'] %}{{uniq_iden['alias']}}{% else %}{{uniq_iden['schema']}}{% endif %}.{% if uniq_iden['prefix'] %}{{ uniq_iden['prefix'] }}{% else %}{{ uniq_iden['schema']}}{% endif %}_{{identifier['field']}}
                                {%- else %}
                                    e.{{identifier['field']}}
                                {%- endif -%}
                                ,
                            {%- endfor -%}
                            NULL
                        ) as session_identifier,
                {%- endif %}
                    e.*

            from {{ snowplow_events }} e
            {% if unique_session_identifiers|length > 0 %}
                {% for identifier in unique_session_identifiers.values() %}
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
            {% if allow_null_dvce_tstamps %}
                and coalesce(a.dvce_sent_tstamp, a.collector_tstamp) <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'coalesce(a.dvce_created_tstamp, a.collector_tstamp)') }}
            {% else %}
                and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'a.dvce_created_tstamp') }}
            {% endif %}
            and a.{{ session_timestamp }} >= {{ lower_limit }}
            and a.{{ session_timestamp }} <= {{ upper_limit }}
            and a.{{ session_timestamp }} >= b.start_tstamp -- deal with late loading events
            and {{ snowplow_utils.app_id_filter(app_ids) }}

        )

        select 
            *
            {% if custom_sql %}
            , {{ custom_sql }}
            {%- endif %}

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
                {%- if ent_or_sde['prefix'] and name not in unique_session_identifiers.keys() -%}
                    {%- set prefix = ent_or_sde['prefix'] -%}
                {%- elif name in unique_session_identifiers.keys() and unique_session_identifiers.get(name, {}).get('prefix') -%}
                    {%- set prefix = unique_session_identifiers[name]['prefix'] -%}
                {%- else -%}
                    {%- set prefix = name -%}
                {%- endif -%}
                {%- if ent_or_sde['single_entity'] and ent_or_sde['single_entity'] is boolean -%}
                    {%- set single_entity = ent_or_sde['single_entity'] -%}
                {%- endif -%}
                {%- if ent_or_sde['alias'] and name not in unique_session_identifiers.keys() -%}
                    {%- set alias = ent_or_sde['alias'] -%}
                {%- elif name in unique_session_identifiers.keys() and unique_session_identifiers.get(name, {}).get('alias') -%}
                    {%- set alias = unique_session_identifiers[name] -%}
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


{% macro spark__base_create_snowplow_events_this_run(sessions_this_run_table, session_identifiers, session_sql, session_timestamp, derived_tstamp_partitioned, days_late_allowed, max_session_days, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, entities_or_sdes, custom_sql, allow_null_dvce_tstamps) %}
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
                                {{ snowplow_utils.get_field(identifier['schema'], identifier['field'], 'e', dbt.type_string(), 0, snowplow_events) }}
                            {%- else -%}
                                e.{{identifier['field']}}
                            {%- endif -%}
                            ,
                        {%- endfor -%}
                        NULL
                    ) as session_identifier,
                {%- endif %}
                e.*
            from {{ snowplow_events }} e
            WHERE e.{{ session_timestamp }} >= {{ lower_limit }}
            and e.{{ session_timestamp }} <= {{ upper_limit }}

        ),
        main_logic as (
        select
            a.*
            ,b.user_identifier -- take user_identifier from manifest. This ensures only 1 domain_userid per session.
            {% if custom_sql %}
                , {{ custom_sql }}
            {% endif %}
            ,row_number() over (partition by event_id order by {{ session_timestamp }}, dvce_created_tstamp) as event_id_dedupe_index

        from identified_events as a
        inner join {{ sessions_this_run }} as b
        on a.session_identifier = b.session_identifier

        where a.{{ session_timestamp }} <= {{ snowplow_utils.timestamp_add('day', max_session_days, 'b.start_tstamp') }}
        {% if allow_null_dvce_tstamps %}
            and coalesce(a.dvce_sent_tstamp, a.collector_tstamp) <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'coalesce(a.dvce_created_tstamp, a.collector_tstamp)') }}
        {% else %}
            and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'a.dvce_created_tstamp') }}
        {% endif %}
        and a.{{ session_timestamp }} >= {{ lower_limit }}
        and a.{{ session_timestamp }} <= {{ upper_limit }}
        and a.{{ session_timestamp }} >= b.start_tstamp -- deal with late loading events

        {% if derived_tstamp_partitioned and target.type == 'bigquery' | as_bool() %}
            and a.derived_tstamp >= {{ snowplow_utils.timestamp_add('hour', -1, lower_limit) }}
            and a.derived_tstamp <= {{ upper_limit }}
        {% endif %}

        and {{ snowplow_utils.app_id_filter(app_ids) }}
    )
    SELECT * 
    FROM main_logic
    WHERE event_id_dedupe_index = 1
    {% endset %}

    {{ return(events_this_run_query) }}

{% endmacro %}