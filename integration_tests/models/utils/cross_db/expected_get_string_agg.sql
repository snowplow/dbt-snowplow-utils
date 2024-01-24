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

-- All combinations with a string col and default order by
select 'string_def_comma_false_false' as test_type, 'a,b,c,c,d' as result
union all select 'string_def_colon_false_false' as test_type, 'a;b;c;c;d' as result
union all select 'string_def_comma_false_true' as test_type, 'd,c,c,b,a' as result
union all select 'string_def_colon_false_true' as test_type, 'd;c;c;b;a' as result
union all select 'string_def_comma_true_false' as test_type, 'a,b,c,d' as result
union all select 'string_def_colon_true_false' as test_type, 'a;b;c;d' as result
union all select 'string_def_comma_true_true' as test_type, 'd,c,b,a' as result
union all select 'string_def_colon_true_true' as test_type, 'd;c;b;a' as result


-- All combinations with a string col and a string order by
union all select 'string_string_comma_false_false' as test_type, 'a,c,b,d,c' as result
union all select 'string_string_colon_false_false' as test_type, 'a;c;b;d;c' as result
union all select 'string_string_comma_false_true' as test_type, 'c,d,b,c,a' as result
union all select 'string_string_colon_false_true' as test_type, 'c;d;b;c;a' as result
{% if target.type in ['databricks','spark'] %}
  union all select 'string_string_comma_true_false' as test_type, 'a,c,b,d' as result
  union all select 'string_string_colon_true_false' as test_type, 'a;c;b;d' as result
  union all select 'string_string_comma_true_true' as test_type, 'c,d,b,a' as result
  union all select 'string_string_colon_true_true' as test_type, 'c;d;b;a' as result
{% endif %}

-- All combinations with a string col and an int order by
union all select 'string_int_comma_false_false' as test_type, 'a,b,c,d,c' as result
union all select 'string_int_colon_false_false' as test_type, 'a;b;c;d;c' as result
union all select 'string_int_comma_false_true' as test_type, 'c,d,c,b,a' as result
union all select 'string_int_colon_false_true' as test_type, 'c;d;c;b;a' as result
{% if target.type in ['databricks','spark'] %}
  union all select 'string_int_comma_true_false' as test_type, 'a,b,c,d' as result
  union all select 'string_int_colon_true_false' as test_type, 'a;b;c;d' as result
  union all select 'string_int_comma_true_true' as test_type, 'c,d,b,a' as result
  union all select 'string_int_colon_true_true' as test_type, 'c;d;b;a' as result
{% endif %}

-- All combinations with a string col and an int string order by
union all select 'string_strint_comma_false_false' as test_type, 'a,b,c,d,c' as result
union all select 'string_strint_colon_false_false' as test_type, 'a;b;c;d;c' as result
union all select 'string_strint_comma_false_true' as test_type, 'c,d,c,b,a' as result
union all select 'string_strint_colon_false_true' as test_type, 'c;d;c;b;a' as result
{% if target.type in ['databricks','spark'] %}
  union all select 'string_strint_comma_true_false' as test_type, 'a,b,c,d' as result
  union all select 'string_strint_colon_true_false' as test_type, 'a;b;c;d' as result
  union all select 'string_strint_comma_true_true' as test_type, 'c,d,b,a' as result
  union all select 'string_strint_colon_true_true' as test_type, 'c;d;b;a' as result
{% endif %}

-- All combinations with a int col and default order by
union all select 'int_def_comma_false_false' as test_type, '1,2,3,3,4' as result
union all select 'int_def_colon_false_false' as test_type, '1;2;3;3;4' as result
union all select 'int_def_comma_false_true' as test_type, '4,3,3,2,1' as result
union all select 'int_def_colon_false_true' as test_type, '4;3;3;2;1' as result
{% if target.type in ['databricks','spark'] %}
  union all select 'int_def_comma_true_false' as test_type, '1,2,3,4' as result
  union all select 'int_def_colon_true_false' as test_type, '1;2;3;4' as result
  union all select 'int_def_comma_true_true' as test_type, '4,3,2,1' as result
  union all select 'int_def_colon_true_true' as test_type, '4;3;2;1' as result
{% endif %}

-- All combinations with a int col and a string order by
union all select 'int_string_comma_false_false' as test_type, '1,3,2,4,3' as result
union all select 'int_string_colon_false_false' as test_type, '1;3;2;4;3' as result
union all select 'int_string_comma_false_true' as test_type, '3,4,2,3,1' as result
union all select 'int_string_colon_false_true' as test_type, '3;4;2;3;1' as result
{% if target.type in ['databricks','spark'] %}
union all select 'int_string_comma_true_false' as test_type, '1,3,2,4' as result
union all select 'int_string_colon_true_false' as test_type, '1;3;2;4' as result
union all select 'int_string_comma_true_true' as test_type, '3,4,2,1' as result
union all select 'int_string_colon_true_true' as test_type, '3;4;2;1' as result
{% endif %}

-- All combinations with a int col and an int order by
union all select 'int_int_comma_false_false' as test_type, '1,2,3,4,3' as result
union all select 'int_int_colon_false_false' as test_type, '1;2;3;4;3' as result
union all select 'int_int_comma_false_true' as test_type, '3,4,3,2,1' as result
union all select 'int_int_colon_false_true' as test_type, '3;4;3;2;1' as result
{% if target.type in ['databricks','spark'] %}
  union all select 'int_int_comma_true_false' as test_type, '1,2,3,4' as result
  union all select 'int_int_colon_true_false' as test_type, '1;2;3;4' as result
  union all select 'int_int_comma_true_true' as test_type, '3,4,2,1' as result
  union all select 'int_int_colon_true_true' as test_type, '3;4;2;1' as result
{% endif %}

-- All combinations with a int col and an int string order by
union all select 'int_strint_comma_false_false' as test_type, '1,2,3,4,3' as result
union all select 'int_strint_colon_false_false' as test_type, '1;2;3;4;3' as result
union all select 'int_strint_comma_false_true' as test_type, '3,4,3,2,1' as result
union all select 'int_strint_colon_false_true' as test_type, '3;4;3;2;1' as result
{% if target.type in ['databricks','spark'] %}
  union all select 'int_strint_comma_true_false' as test_type, '1,2,3,4' as result
  union all select 'int_strint_colon_true_false' as test_type, '1;2;3;4' as result
  union all select 'int_strint_comma_true_true' as test_type, '3,4,2,1' as result
  union all select 'int_strint_colon_true_true' as test_type, '3;4;2;1' as result
{% endif %}
