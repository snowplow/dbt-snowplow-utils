{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
Static stand-in for the incremental manifest is_run_with_new_events() checks against for
test_lifecycle_manifest_dedupe_actual. last_success is fixed well before both test runs' event
limits, so run 2 is always detected as having new events to process regardless of run order.
#}
{{
    config(
        materialized='table',
        tags=['lifecycle_manifest_dedupe_test', 'requires_script']
    )
}}

select
    'test_lifecycle_manifest_dedupe_actual' as model,
    cast('2023-01-01 00:00:00' as timestamp) as last_success
