with data as (
select * from {{ ref('data_get_string_agg') }}
)

select
  {{ snowplow_utils.get_string_agg('string_col', 'd', ';') }} as result

from data d

union all

select
  {{ snowplow_utils.get_string_agg('string_col', 'd', ';', 'order_by_col') }} as result

from data d
