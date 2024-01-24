{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{
  config(
    materialized = 'table',
    )
}}

with data as (
select * from {{ ref('data_get_string_agg') }}
)

/*
All combinations below refers to the options of inputs:
1 - seperator default (', ') or ';'
2 - Is distinct default (false) or true
3 - order desc default (false) or true

2^3 = 8 options per other inputs
However distinct is only supported when you are ordering by the same column (not cast) so in many cases there will only be 4 for non-databricks
*/




-- All combinations with a string col and default order by
          select 'string_def_comma_false_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd') }} as result from data d
union all select 'string_def_colon_false_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', separator = ';') }} as result from data d
union all select 'string_def_comma_false_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_desc = true) }} as result from data d
union all select 'string_def_colon_false_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', separator = ';', order_desc = true) }} as result from data d
union all select 'string_def_comma_true_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', is_distinct = true) }} as result from data d
union all select 'string_def_colon_true_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', separator = ';', is_distinct = true) }} as result from data d
union all select 'string_def_comma_true_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', is_distinct = true, order_desc = true) }} as result from data d
union all select 'string_def_colon_true_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', separator = ';', is_distinct = true, order_desc = true) }} as result from data d



-- All combinations with a string col and a string order by
union all select 'string_string_comma_false_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd') }} as result from data d
union all select 'string_string_colon_false_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', separator = ';') }} as result from data d
union all select 'string_string_comma_false_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', order_desc = true) }} as result from data d
union all select 'string_string_colon_false_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', separator = ';', order_desc = true) }} as result from data d
{% if target.type in ['databricks', 'spark'] %}
  union all select 'string_string_comma_true_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', is_distinct = true) }} as result from data d
  union all select 'string_string_colon_true_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', separator = ';', is_distinct = true) }} as result from data d
  union all select 'string_string_comma_true_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', is_distinct = true, order_desc = true) }} as result from data d
  union all select 'string_string_colon_true_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', separator = ';', is_distinct = true, order_desc = true) }} as result from data d
{% endif %}

-- All combinations with a string col and an int order by
union all select 'string_int_comma_false_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd') }} as result from data d
union all select 'string_int_colon_false_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';') }} as result from data d
union all select 'string_int_comma_false_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', order_desc = true) }} as result from data d
union all select 'string_int_colon_false_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', order_desc = true) }} as result from data d
{% if target.type in ['databricks', 'spark'] %}
  union all select 'string_int_comma_true_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', is_distinct = true) }} as result from data d
  union all select 'string_int_colon_true_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', is_distinct = true) }} as result from data d
  union all select 'string_int_comma_true_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', is_distinct = true, order_desc = true) }} as result from data d
  union all select 'string_int_colon_true_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', is_distinct = true, order_desc = true) }} as result from data d
{% endif %}

-- All combinations with a string col and an int string order by
union all select 'string_strint_comma_false_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd') }} as result from data d
union all select 'string_strint_colon_false_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';') }} as result from data d
union all select 'string_strint_comma_false_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', order_desc = true) }} as result from data d
union all select 'string_strint_colon_false_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', order_desc = true) }} as result from data d
{% if target.type in ['databricks', 'spark'] %}
  union all select 'string_strint_comma_true_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', is_distinct = true) }} as result from data d
  union all select 'string_strint_colon_true_false' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', is_distinct = true) }} as result from data d
  union all select 'string_strint_comma_true_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', is_distinct = true, order_desc = true) }} as result from data d
  union all select 'string_strint_colon_true_true' as test_type, {{ snowplow_utils.get_string_agg('string_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', is_distinct = true, order_desc = true) }} as result from data d
{% endif %}

-- All combinations with a int col and default order by
union all select 'int_def_comma_false_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', sort_numeric = true) }} as result from data d
union all select 'int_def_colon_false_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', sort_numeric = true, separator = ';') }} as result from data d
union all select 'int_def_comma_false_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', sort_numeric = true, order_desc = true) }} as result from data d
union all select 'int_def_colon_false_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', sort_numeric = true, separator = ';', order_desc = true) }} as result from data d
{% if target.type in ['databricks', 'spark'] %}
  union all select 'int_def_comma_true_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', sort_numeric = true, is_distinct = true) }} as result from data d
  union all select 'int_def_colon_true_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', sort_numeric = true, separator = ';', is_distinct = true) }} as result from data d
  union all select 'int_def_comma_true_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', sort_numeric = true, is_distinct = true, order_desc = true) }} as result from data d
  union all select 'int_def_colon_true_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', sort_numeric = true, separator = ';', is_distinct = true, order_desc = true) }} as result from data d
{% endif %}

-- All combinations with a int col and a string order by
union all select 'int_string_comma_false_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd') }} as result from data d
union all select 'int_string_colon_false_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', separator = ';') }} as result from data d
union all select 'int_string_comma_false_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', order_desc = true) }} as result from data d
union all select 'int_string_colon_false_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', separator = ';', order_desc = true) }} as result from data d
{% if target.type in ['databricks', 'spark'] %}
  union all select 'int_string_comma_true_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', is_distinct = true) }} as result from data d
  union all select 'int_string_colon_true_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', separator = ';', is_distinct = true) }} as result from data d
  union all select 'int_string_comma_true_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', is_distinct = true, order_desc = true) }} as result from data d
  union all select 'int_string_colon_true_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_col', order_by_column_prefix = 'd', separator = ';', is_distinct = true, order_desc = true) }} as result from data d
{% endif %}

-- All combinations with a int col and an int order by
union all select 'int_int_comma_false_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd') }} as result from data d
union all select 'int_int_colon_false_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';') }} as result from data d
union all select 'int_int_comma_false_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', order_desc = true) }} as result from data d
union all select 'int_int_colon_false_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', order_desc = true) }} as result from data d
{% if target.type in ['databricks', 'spark'] %}
  union all select 'int_int_comma_true_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', is_distinct = true) }} as result from data d
  union all select 'int_int_colon_true_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', is_distinct = true) }} as result from data d
  union all select 'int_int_comma_true_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', is_distinct = true, order_desc = true) }} as result from data d
  union all select 'int_int_colon_true_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', is_distinct = true, order_desc = true) }} as result from data d
{% endif %}

-- All combinations with a int col and an int string order by
union all select 'int_strint_comma_false_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd') }} as result from data d
union all select 'int_strint_colon_false_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';') }} as result from data d
union all select 'int_strint_comma_false_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', order_desc = true) }} as result from data d
union all select 'int_strint_colon_false_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', order_desc = true) }} as result from data d
{% if target.type in ['databricks', 'spark'] %}
  union all select 'int_strint_comma_true_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', is_distinct = true) }} as result from data d
  union all select 'int_strint_colon_true_false' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', is_distinct = true) }} as result from data d
  union all select 'int_strint_comma_true_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', is_distinct = true, order_desc = true) }} as result from data d
  union all select 'int_strint_colon_true_true' as test_type, {{ snowplow_utils.get_string_agg('int_col', 'd', order_by_column = 'order_by_int_as_string_col', sort_numeric = true, order_by_column_prefix = 'd', separator = ';', is_distinct = true, order_desc = true) }} as result from data d
{% endif %}
