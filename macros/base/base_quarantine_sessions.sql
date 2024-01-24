{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro base_quarantine_sessions(max_session_length, quarantined_sessions='snowplow_base_quarantined_sessions', src_relation=this) %}

  {{ return(adapter.dispatch('base_quarantine_sessions', 'snowplow_utils')(max_session_length, quarantined_sessions, src_relation)) }}

{% endmacro %}

{% macro default__base_quarantine_sessions(max_session_length, quarantined_sessions_str, src_relation) %}
  {% set quarantined_sessions = ref(quarantined_sessions_str) %}

  {% set sessions_to_quarantine_sql = snowplow_utils.base_get_quarantine_sql(src_relation, max_session_length) %}

  {% set quarantine_query %}

  merge into {{ quarantined_sessions }} trg
  using ({{ sessions_to_quarantine_sql }}) src
  on trg.session_identifier = src.session_identifier
  when not matched then insert (session_identifier) values(session_identifier);

  {% endset %}

  {{ return(quarantine_query) }}

{% endmacro %}

{% macro postgres__base_quarantine_sessions(max_session_length, quarantined_sessions_str, src_relation) %}

  {% set quarantined_sessions = ref(quarantined_sessions_str) %}
  {% set sessions_to_quarantine_tmp = 'sessions_to_quarantine_tmp' %}

  begin;

    create temporary table {{ sessions_to_quarantine_tmp }} as (
      {{ snowplow_utils.base_get_quarantine_sql(src_relation, max_session_length) }}
    );

    delete from {{ quarantined_sessions }}
    where session_identifier in (select session_identifier from {{ sessions_to_quarantine_tmp }});

    insert into {{ quarantined_sessions }} (
      select session_identifier from {{ sessions_to_quarantine_tmp }});

    drop table {{ sessions_to_quarantine_tmp }};

  commit;

{% endmacro %}

{% macro base_get_quarantine_sql(relation, max_session_length) %}

  {# Find sessions exceeding max_session_days #}
  {% set quarantine_sql -%}

    select
      session_identifier

    from {{ relation }}
    -- '=' since end_tstamp is restricted to start_tstamp + max_session_days
    where end_tstamp = {{ snowplow_utils.timestamp_add(
                              'day',
                              max_session_length,
                              'start_tstamp'
                              ) }}

  {%- endset %}

  {{ return(quarantine_sql) }}

{% endmacro %}
