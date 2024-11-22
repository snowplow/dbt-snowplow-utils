{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Add quarantined_sessions reference properly #}
{% macro base_create_snowplow_sessions_lifecycle_manifest(session_identifiers=[{"schema": "atomic", "field" : "domain_sessionid"}], session_sql=none, session_timestamp='load_tstamp', user_identifiers=[{"schema": "atomic", "field" : "domain_userid"}], user_sql=none, quarantined_sessions=none, derived_tstamp_partitioned=true, days_late_allowed=3, max_session_days=3, app_ids=[], snowplow_events_database=none, snowplow_events_schema='atomic', snowplow_events_table='events', event_limits_table='snowplow_base_new_event_limits', incremental_manifest_table='snowplow_incremental_manifest', package_name='snowplow', allow_null_dvce_tstamps=false) %}
    {{ return(adapter.dispatch('base_create_snowplow_sessions_lifecycle_manifest', 'snowplow_utils')(session_identifiers, session_sql, session_timestamp, user_identifiers, user_sql, quarantined_sessions, derived_tstamp_partitioned, days_late_allowed, max_session_days, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, event_limits_table, incremental_manifest_table, package_name, allow_null_dvce_tstamps)) }}
{% endmacro %}

{% macro default__base_create_snowplow_sessions_lifecycle_manifest(session_identifiers, session_sql, session_timestamp, user_identifiers, user_sql, quarantined_sessions, derived_tstamp_partitioned, days_late_allowed, max_session_days, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, event_limits_table, incremental_manifest_table, package_name, allow_null_dvce_tstamps) %}
    {% set base_event_limits = ref(event_limits_table) %}
    {% set lower_limit, upper_limit, _ = snowplow_utils.return_base_new_event_limits(base_event_limits) %}
    {% set session_lookback_limit = snowplow_utils.get_session_lookback_limit(lower_limit) %}
    {% set is_run_with_new_events = snowplow_utils.is_run_with_new_events(package_name, event_limits_table, incremental_manifest_table) %}
    {% set snowplow_events = api.Relation.create(database=snowplow_events_database, schema=snowplow_events_schema, identifier=snowplow_events_table) %}

    {% set sessions_lifecycle_manifest_query %}

        with new_events_session_ids_init as (
            select
            {% if session_sql %}
                {{ session_sql }} as session_identifier,
            {% elif session_identifiers|length > 0 %}
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
            {%- else -%}
                {% do exceptions.raise_compiler_error("Need to specify either session identifiers or custom session SQL") %}
            {%- endif %}
            {%- if user_sql -%}
                max({{ user_sql }}) as user_identifier,
            {%- elif user_identifiers|length > 0 %}
                max(
                    COALESCE(
                        {% for identifier in user_identifiers %}
                            {%- if identifier['schema']|lower != 'atomic' -%}
                                {{ snowplow_utils.get_field(identifier['schema'], identifier['field'], 'e', dbt.type_string(), 0, snowplow_events) }}
                            {%- else -%}
                                e.{{identifier['field']}}
                            {%- endif -%}
                            ,
                        {%- endfor -%}
                        NULL
                    )
                ) as user_identifier, -- Edge case 1: Arbitary selection to avoid window function like first_value.
            {% else %}
                {% do exceptions.raise_compiler_error("Need to specify either user identifiers or custom user SQL") %}
            {%- endif %}
                min({{ session_timestamp }}) as start_tstamp,
                max({{ session_timestamp }}) as end_tstamp

            from {{ snowplow_events }} e

            where
            {% if allow_null_dvce_tstamps %}
                coalesce(dvce_sent_tstamp, collector_tstamp) <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'coalesce(dvce_created_tstamp, collector_tstamp)') }}
            {% else %}
                dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'dvce_created_tstamp') }}
            {% endif %} -- don't process data that's too late
                and {{ session_timestamp }} >= {{ lower_limit }}
                and {{ session_timestamp }} <= {{ upper_limit }}
                and {{ snowplow_utils.app_id_filter(app_ids) }}
                and {{ is_run_with_new_events }} --don't reprocess sessions that have already been processed.
                {% if derived_tstamp_partitioned and target.type == 'bigquery' | as_bool() %} -- BQ only
                and derived_tstamp >= {{ lower_limit }}
                and derived_tstamp <= {{ upper_limit }}
                {% endif %}

            group by 1
        ), new_events_session_ids as (
            select *
            from new_events_session_ids_init e
            {% if quarantined_sessions %}
                where session_identifier is not null
                and not exists (select 1 from {{ ref(quarantined_sessions) }} as a where a.session_identifier = e.session_identifier) -- don't continue processing v.long sessions
            {%- endif %}

        )
        {% if is_incremental() %}

        , previous_sessions as (
        select *

        from {{ this }}

        where start_tstamp >= {{ session_lookback_limit }}
        and {{ is_run_with_new_events }} --don't reprocess sessions that have already been processed.
        )

        , session_lifecycle as (
        select
            ns.session_identifier,
            coalesce(self.user_identifier, ns.user_identifier) as user_identifier, -- Edge case 1: Take previous value to keep domain_userid consistent. Not deterministic but performant
            least(ns.start_tstamp, coalesce(self.start_tstamp, ns.start_tstamp)) as start_tstamp,
            greatest(ns.end_tstamp, coalesce(self.end_tstamp, ns.end_tstamp)) as end_tstamp -- BQ 1 NULL will return null hence coalesce

        from new_events_session_ids ns
        left join previous_sessions as self
            on ns.session_identifier = self.session_identifier

        where
            self.session_identifier is null -- process all new sessions
            or self.end_tstamp < {{ snowplow_utils.timestamp_add('day', max_session_days, 'self.start_tstamp') }} --stop updating sessions exceeding 3 days
        )

        {% else %}

        , session_lifecycle as (

        select * from new_events_session_ids

        )

        {% endif %}

        select
        sl.session_identifier,
        sl.user_identifier,
        sl.start_tstamp,
        least({{ snowplow_utils.timestamp_add('day', max_session_days, 'sl.start_tstamp') }}, sl.end_tstamp) as end_tstamp -- limit session length to max_session_days
        {% if target.type in ['databricks', 'spark'] -%}
        , DATE(start_tstamp) as start_tstamp_date
        {%- endif %}

        from session_lifecycle sl
    {% endset %}

    {{ return(sessions_lifecycle_manifest_query) }}

{% endmacro %}

{% macro postgres__base_create_snowplow_sessions_lifecycle_manifest(session_identifiers, session_sql, session_timestamp, user_identifiers, user_sql, quarantined_sessions, derived_tstamp_partitioned, days_late_allowed, max_session_days, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, event_limits_table, incremental_manifest_table, package_name, allow_null_dvce_tstamps) %}
    {% set base_event_limits = ref(event_limits_table) %}
    {% set lower_limit, upper_limit, _ = snowplow_utils.return_base_new_event_limits(base_event_limits) %}
    {% set session_lookback_limit = snowplow_utils.get_session_lookback_limit(lower_limit) %}
    {% set is_run_with_new_events = snowplow_utils.is_run_with_new_events(package_name, event_limits_table, incremental_manifest_table) %}
    {% set snowplow_events = api.Relation.create(database=snowplow_events_database, schema=snowplow_events_schema, identifier=snowplow_events_table) %}

    {% set sessions_lifecycle_manifest_query %}

        with
        {# Get all the session and user contexts extracted and ready to join later #}
        {% set unique_identifiers = dict() %} {# need to avoid duplicate contexts when values come from the same one, so just use the first of that context #}

        {% if session_identifiers %}
            {% for identifier in session_identifiers %}
                {% if identifier['schema']|lower != 'atomic' and identifier['schema'] not in unique_identifiers %}
                    {{ snowplow_utils.get_sde_or_context(snowplow_events_schema, identifier['schema'], lower_limit, upper_limit, identifier['prefix'], database=snowplow_events_database) }},
                    {% do unique_identifiers.update({identifier['schema']: identifier}) %}
                {%- endif -%}
            {% endfor %}
        {% endif %}

        {% if user_identifiers%}
            {% for identifier in user_identifiers %}
                {% if identifier['schema']|lower != 'atomic' and identifier['schema'] not in unique_identifiers %}
                    {{ snowplow_utils.get_sde_or_context(snowplow_events_schema, identifier['schema'], lower_limit, upper_limit, identifier['prefix'], database=snowplow_events_database) }},
                    {% do unique_identifiers.update({identifier['schema']: identifier}) %}
                {%- endif -%}
            {% endfor %}
        {% endif %}

        {# Produce the core session and single user identifier for sessions with new events #}
        new_events_session_ids_init as (
            select
            {% if session_sql %}
                {{ session_sql }} as session_identifier,
            {% elif session_identifiers|length > 0 %}
                COALESCE(
                    {% for identifier in session_identifiers %}
                        {%- if identifier['schema']|lower != 'atomic' -%}
                            {# Use the parsed version of the context to ensure we have the right alias and prefix #}
                            {% set uniq_iden = unique_identifiers[identifier['schema']] %}
                            {% if uniq_iden['alias'] %}{{uniq_iden['alias']}}{% else %}{{uniq_iden['schema']}}{% endif %}.{% if uniq_iden['prefix'] %}{{ uniq_iden['prefix']  ~ '_' }}{% endif %}{{identifier['field']}}
                        {%- else -%}
                            e.{{identifier['field']}}
                        {%- endif -%}
                        ,
                    {%- endfor -%}
                    NULL
                ) as session_identifier,
            {% else %}
                {% do exceptions.raise_compiler_error("Need to specify either session identifiers or custom session SQL") %}
            {% endif %}
            {% if user_sql %}
                max({{ user_sql }}) as user_identifier,
            {% elif user_identifiers|length > 0 %}
                max(
                    COALESCE(
                        {% for identifier in user_identifiers %}
                            {%- if identifier['schema']|lower != 'atomic' %}
                            {# Use the parsed version of the context to ensure we have the right alias and prefix #}
                                {% set uniq_iden = unique_identifiers[identifier['schema']] %}
                                {% if uniq_iden['alias'] %}{{uniq_iden['alias']}}{% else %}{{uniq_iden['schema']}}{% endif %}.{% if uniq_iden['prefix'] %}{{ uniq_iden['prefix'] ~ '_' }}{% endif %}{{identifier['field']}}
                            {%- else %}
                                e.{{identifier['field']}}
                            {%- endif -%}
                            ,
                        {%- endfor -%}
                        NULL
                    )
                ) as user_identifier, -- Edge case 1: Arbitary selection to avoid window function like first_value.
            {% else %}
                {% do exceptions.raise_compiler_error("Need to specify either user identifiers or custom user SQL") %}
            {% endif %}
                min({{ session_timestamp }}) as start_tstamp,
                max({{ session_timestamp }}) as end_tstamp

            from {{ snowplow_events }} e
            {% if unique_identifiers|length > 0 %}
                {% for identifier in unique_identifiers.values() %}
                    {%- if identifier['schema']|lower != 'atomic' -%}
                    left join {{ identifier['schema'] }} {% if identifier['alias'] %}as {{ identifier['alias'] }}{% endif %} on e.event_id = {% if identifier['alias'] %}{{ identifier['alias']}}{% else %}{{ identifier['schema'] }}{% endif %}.{% if identifier['prefix'] %}{{ identifier['prefix'] }}{% else %}{{ identifier['schema']}}{% endif %}__id and e.collector_tstamp = {% if identifier['alias'] %}{{ identifier['alias']}}{% else %}{{ identifier['schema'] }}{% endif %}.{% if identifier['prefix'] %}{{ identifier['prefix'] }}{% else %}{{ identifier['schema']}}{% endif %}__tstamp
                    {% endif -%}
                {% endfor %}
            {% endif %}
            where
            {% if allow_null_dvce_tstamps %}
                coalesce(dvce_sent_tstamp, collector_tstamp) <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'coalesce(dvce_created_tstamp, collector_tstamp)') }}
            {% else %}
                dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', days_late_allowed, 'dvce_created_tstamp') }}
            {% endif %}-- don't process data that's too late
                and {{ session_timestamp }} >= {{ lower_limit }}
                and {{ session_timestamp }} <= {{ upper_limit }}
                and {{ snowplow_utils.app_id_filter(app_ids) }}
                and {{ is_run_with_new_events }} --don't reprocess sessions that have already been processed.
                {% if derived_tstamp_partitioned and target.type == 'bigquery' | as_bool() %} -- BQ only
                and derived_tstamp >= {{ lower_limit }}
                and derived_tstamp <= {{ upper_limit }}
                {% endif %}

            group by 1

        {# Exclude quarantined sessions #}
        ), new_events_session_ids as (
            select *
            from new_events_session_ids_init e
            {% if quarantined_sessions %}
                where session_identifier is not null
                and not exists (select 1 from {{ ref(quarantined_sessions) }} as a where a.session_identifier = e.session_identifier) -- don't continue processing v.long sessions
            {%- endif %}
        )

        {% if is_incremental() %}

        , previous_sessions as (
        select *

        from {{ this }}

        where start_tstamp >= {{ session_lookback_limit }}
        and {{ is_run_with_new_events }} --don't reprocess sessions that have already been processed.
        )

        , session_lifecycle as (
        select
            ns.session_identifier,
            coalesce(self.user_identifier, ns.user_identifier) as user_identifier, -- Edge case 1: Take previous value to keep domain_userid consistent. Not deterministic but performant
            least(ns.start_tstamp, coalesce(self.start_tstamp, ns.start_tstamp)) as start_tstamp,
            greatest(ns.end_tstamp, coalesce(self.end_tstamp, ns.end_tstamp)) as end_tstamp -- BQ 1 NULL will return null hence coalesce

        from new_events_session_ids ns
        left join previous_sessions as self
            on ns.session_identifier = self.session_identifier

        where
            self.session_identifier is null -- process all new sessions
            or self.end_tstamp < {{ snowplow_utils.timestamp_add('day', max_session_days, 'self.start_tstamp') }} --stop updating sessions exceeding 3 days
        )

        {% else %}

        , session_lifecycle as (

        select * from new_events_session_ids

        )

        {% endif %}

        select
        sl.session_identifier,
        sl.user_identifier,
        sl.start_tstamp,
        least({{ snowplow_utils.timestamp_add('day', max_session_days, 'sl.start_tstamp') }}, sl.end_tstamp) as end_tstamp -- limit session length to max_session_days

        from session_lifecycle sl
    {% endset %}

    {{ return(sessions_lifecycle_manifest_query) }}

{% endmacro %}
