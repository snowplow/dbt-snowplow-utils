{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

with prep as (
  select
  'string_def_colon_false_false' as test_type,
  'a' as element,
  0 as source_index
  
  union all
  
  select
  'string_def_colon_false_false' as test_type,
  'b' as element,
  1 as source_index
  
  union all
  
  select
  'string_def_colon_false_false' as test_type,
  'c' as element,
  2 as source_index
  
  union all
  
  select
  'string_def_colon_false_false' as test_type,
  'c' as element,
  3 as source_index
  
  union all
  
  select
  'string_def_colon_false_false' as test_type,
  'd' as element,
  4 as source_index
  
  union all
  
  select
  'string_string_colon_false_true' as test_type,
  'c' as element,
  0 as source_index
  
  union all
  
  select
  'string_string_colon_false_true' as test_type,
  'd' as element,
  1 as source_index
  
  union all
  
  select
  'string_string_colon_false_true' as test_type,
  'b' as element,
  2 as source_index
  
  union all
  
  select
  'string_string_colon_false_true' as test_type,
  'c' as element,
  3 as source_index
  
  union all
  
  select
  'string_string_colon_false_true' as test_type,
  'a' as element,
  4 as source_index
  
  union all 
  
  select
  'int_def_colon_false_true' as test_type,
  '4' as element,
  0 as source_index
  
  union all
  
  select
  'int_def_colon_false_true' as test_type,
  '3' as element,
  1 as source_index
  
  union all
  
  select
  'int_def_colon_false_true' as test_type,
  '3' as element,
  2 as source_index
  
  union all
  
  select
  'int_def_colon_false_true' as test_type,
  '2' as element,
  3 as source_index
  
  union all
  
  select
  'int_def_colon_false_true' as test_type,
  '1' as element,
  4 as source_index
  
)

select test_type, element
from prep
