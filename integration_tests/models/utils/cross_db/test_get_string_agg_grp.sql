{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
with data as (
select * from {{ ref('data_get_string_agg_grp') }}
)

select{{ snowplow_utils.get_string_agg('string_col', 'd') }} as result, group_col as grp from data d
group by group_col
