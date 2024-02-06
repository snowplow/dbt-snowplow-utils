{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

with data as (
select * from {{ ref('data_parse_agg_dict') }}
)

select
  {{ snowplow_utils.parse_agg_dict({'type': 'countd', 'field': 'event_name', 'alias': 'distinct_event_names'})}},
  {{ snowplow_utils.parse_agg_dict({'type': 'count', 'field': 'event_name', 'alias': 'count_event_names'})}},
  {{ snowplow_utils.parse_agg_dict({'type': 'sum', 'field': 'event_value', 'alias': 'sum_event_value'})}},
  {{ snowplow_utils.parse_agg_dict({'type': 'avg', 'field': 'event_value', 'alias': 'avg_event_value'})}},
  {{ snowplow_utils.parse_agg_dict({'type': 'min', 'field': 'event_value', 'alias': 'min_event_value'})}},
  {{ snowplow_utils.parse_agg_dict({'type': 'max', 'field': 'event_value', 'alias': 'max_event_value'})}}
from data
