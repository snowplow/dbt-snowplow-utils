
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
