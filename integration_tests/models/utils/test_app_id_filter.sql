
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
