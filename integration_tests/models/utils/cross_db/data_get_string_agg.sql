{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Everything says int but I've made some of them decimals to test that as well #}

{{
  config(
    materialized = 'table',
    )
}}

with data as (

  select
    'a' as string_col,
    1 as int_col,
    '1' as int_as_string_col,
    'i' as order_by_col,
    10 as order_by_int_col,
    '10' as order_by_int_as_string_col

  union all

  select
    'b' as string_col,
    2 as int_col,
    '2.5' as int_as_string_col,
    'k' as order_by_col,
    11.6 as order_by_int_col,
    '11.6' as order_by_int_as_string_col

  union all

  select
    'c' as string_col,
    3 as int_col,
    '3' as int_as_string_col,
    'j' as order_by_col,
    12 as order_by_int_col,
    '12' as order_by_int_as_string_col

  union all
  -- for distinct test
  select
    'c' as string_col,
    3 as int_col,
    '3' as int_as_string_col,
    'm' as order_by_col,
    1000 as order_by_int_col,
    '1000' as order_by_int_as_string_col

  union all

  select
    'd' as string_col,
    4 as int_col,
    '4' as int_as_string_col,
    'l' as order_by_col,
    100 as order_by_int_col,
    '100' as order_by_int_as_string_col

)

select * from data
