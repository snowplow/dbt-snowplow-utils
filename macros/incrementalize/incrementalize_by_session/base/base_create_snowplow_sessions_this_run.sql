{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro base_create_snowplow_sessions_this_run(lifecycle_manifest_table='snowplow_base_sessions_lifecycle_manifest', new_event_limits_table='snowplow_base_new_event_limits') %}
    {% set lifecycle_manifest = ref(lifecycle_manifest_table) %}
    {% set new_event_limits = ref(new_event_limits_table) %}
    {%- set lower_limit,
        upper_limit,
        session_start_limit = snowplow_utils.return_base_new_event_limits(new_event_limits) %}

    {% set sessions_sql %}


        select
        s.session_identifier,
        s.user_identifier,
        s.start_tstamp,
        -- end_tstamp used in next step to limit events. When backfilling, set end_tstamp to upper_limit if end_tstamp > upper_limit.
        -- This ensures we don't accidentally process events after upper_limit
        case when s.end_tstamp > {{ upper_limit }} then {{ upper_limit }} else s.end_tstamp end as end_tstamp

        from {{ lifecycle_manifest }} s

        where
        -- General window of start_tstamps to limit table scans. Logic complicated by backfills.
        -- To be within the run, session start_tstamp must be >= lower_limit - max_session_days as we limit end_tstamp in manifest to start_tstamp + max_session_days
        s.start_tstamp >= {{ session_start_limit }}
        and s.start_tstamp <= {{ upper_limit }}
        -- Select sessions within window that either; start or finish between lower & upper limit, start and finish outside of lower and upper limits
        and not (s.start_tstamp > {{ upper_limit }} or s.end_tstamp < {{ lower_limit }})
    {% endset %}

    {{ return(sessions_sql) }}
{% endmacro%}
