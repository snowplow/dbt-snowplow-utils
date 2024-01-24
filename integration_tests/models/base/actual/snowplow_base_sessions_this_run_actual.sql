{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{
    config(
        tags=['base_macro'],
        post_hook=["{{ snowplow_utils.base_quarantine_sessions(var('snowplow__max_session_days', 3), 'snowplow_base_quarantined_sessions_actual') }}"]
    )
}}


{% set sessions_query = snowplow_utils.base_create_snowplow_sessions_this_run(lifecycle_manifest_table='snowplow_base_sessions_lifecycle_manifest_actual', new_event_limits_table='snowplow_base_new_event_limits_actual') %}

{{ sessions_query }}
