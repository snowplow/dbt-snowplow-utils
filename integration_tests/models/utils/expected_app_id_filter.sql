{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
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

union all

select
  *
from data
where (app_id in ('a') or app_id is null)

union all

select
  *
from data
where app_id is null

union all

select
  *
from data
where app_id is null

union all

select
  *
from data
where 1=1
