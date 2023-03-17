{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}


with data as (
select * from {{ ref('data_app_id_filter') }}
)

select
  *
from data
where app_id in ('a','b')

union all

select
  *
from data
where true

union all

select
  *
from data
where app_id in ('c')
