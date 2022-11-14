with data as (
select * from {{ ref('data_get_string_agg_int_string') }}
)

select
  {{ snowplow_utils.get_string_agg('int_col', 'd', ';', 'order_by_col', sort_numeric=true) }} as result
from data d
