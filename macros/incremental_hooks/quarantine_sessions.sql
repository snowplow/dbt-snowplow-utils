{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro quarantine_sessions(package_name, max_session_length, src_relation=this) %}

  {{ return(adapter.dispatch('quarantine_sessions', 'snowplow_utils')(package_name, max_session_length, src_relation=this)) }}

{% endmacro %}

{% macro default__quarantine_sessions(package_name, max_session_length, src_relation=this) %}

  {% set quarantined_sessions = ref(package_name~'_base_quarantined_sessions') %}

  {% set sessions_to_quarantine_sql = snowplow_utils.get_quarantine_sql(src_relation, max_session_length) %}

  merge into {{ quarantined_sessions }} trg
  using ({{ sessions_to_quarantine_sql }}) src
  on trg.session_id = src.session_id
  when not matched then insert (session_id) values(session_id);

{% endmacro %}

{% macro postgres__quarantine_sessions(package_name, max_session_length, src_relation=this) %}

  {% set quarantined_sessions = ref(package_name~'_base_quarantined_sessions') %}
  {% set sessions_to_quarantine_tmp = 'sessions_to_quarantine_tmp' %}

  begin;

    create temporary table {{ sessions_to_quarantine_tmp }} as (
      {{ snowplow_utils.get_quarantine_sql(src_relation, max_session_length) }}
    );

    delete from {{ quarantined_sessions }}
    where session_id in (select session_id from {{ sessions_to_quarantine_tmp }});

    insert into {{ quarantined_sessions }} (
      select session_id from {{ sessions_to_quarantine_tmp }});

    drop table {{ sessions_to_quarantine_tmp }};

  commit;

{% endmacro %}

{% macro get_quarantine_sql(relation, max_session_length) %}

  {# Find sessions exceeding max_session_days #}
  {% set quarantine_sql -%}

    select
      session_id

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
