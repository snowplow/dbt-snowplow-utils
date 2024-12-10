{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

with data as (
  
  select * from {{ ref('data_indexed_unnest')}}
)

, expected as (

{{ snowplow_utils.unnest('test_type', 'test_array', 'element', 'data', with_index=true) }}

)

select test_type, element from expected 
