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
where {{ snowplow_utils.app_id_filter(["a","b"]) }}

union all

select
  *
from data
where {{ snowplow_utils.app_id_filter([]) }}

union all

select
  *
from data
where {{ snowplow_utils.app_id_filter("c") }}

union all

select
  *
from data
where {{ snowplow_utils.app_id_filter(["a",null]) }}

union all

select
  *
from data
where {{ snowplow_utils.app_id_filter([null]) }}

union all

select
  *
from data
where {{ snowplow_utils.app_id_filter(null) }}

union all

select
  *
from data
where {{ snowplow_utils.app_id_filter() }}
