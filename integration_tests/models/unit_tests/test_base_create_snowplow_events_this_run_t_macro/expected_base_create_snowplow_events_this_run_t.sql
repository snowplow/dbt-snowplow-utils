{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# Expected output: only the event inside the window and the event exactly at upper_limit.
   The event before lower_limit and the event exactly at lower_limit are excluded. #}

select event_id, load_tstamp
from {{ ref('data_base_events_boundary_t') }}
where event_id in ('evt-inside-window', 'evt-at-upper')
