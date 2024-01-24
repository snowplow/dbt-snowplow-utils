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
    test_type,
    {{ snowplow_utils.get_split_to_array('result', 'g', ';') }} as test_array
    
  from {{ ref('expected_get_string_agg')}} g
  
  where test_type in ('string_def_colon_false_false', 'string_string_colon_false_true', 'int_def_colon_false_true')
)

select * from data
