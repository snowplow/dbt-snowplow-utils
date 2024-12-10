{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{%- set lower_limit_1, upper_limit_1 = snowplow_utils.return_limits_from_model(ref('dummy_model_empty'), 'tstamp_col', 'tstamp_col') %}
{%- set lower_limit_2, upper_limit_2 = snowplow_utils.return_limits_from_model(ref('dummy_model_only_nulls'), 'tstamp_col', 'tstamp_col') %}
{%- set lower_limit_3, upper_limit_3 = snowplow_utils.return_limits_from_model(ref('dummy_model_standard'), 'tstamp_col', 'tstamp_col') %}
{%- set lower_limit_4, upper_limit_4 = snowplow_utils.return_limits_from_model(ref('dummy_model_empty'), 'tstamp_col', 'tstamp_col', lower_output=True) %}
{%- set lower_limit_5, upper_limit_5 = snowplow_utils.return_limits_from_model(ref('dummy_model_empty'), 'tstamp_col', 'tstamp_col', lower_output=False) %}
{%- set lower_limit_6, upper_limit_6 = snowplow_utils.return_limits_from_model(ref('dummy_model_only_nulls'), 'tstamp_col', 'tstamp_col', lower_output=True) %}
{%- set lower_limit_7, upper_limit_7 = snowplow_utils.return_limits_from_model(ref('dummy_model_only_nulls'), 'tstamp_col', 'tstamp_col', lower_output=False) %}
{%- set lower_limit_8, upper_limit_8 = snowplow_utils.return_limits_from_model(ref('dummy_model_standard'), 'tstamp_col', 'tstamp_col', lower_output=True) %}
{%- set lower_limit_9, upper_limit_9 = snowplow_utils.return_limits_from_model(ref('dummy_model_standard'), 'tstamp_col', 'tstamp_col', lower_output=False) %}

WITH input_1 AS (
  SELECT {{ lower_limit_1 }} as lower_limit, {{ upper_limit_1 }} AS upper_limit
)

, input_2 AS (
  SELECT {{ lower_limit_2 }} as lower_limit, {{ upper_limit_2 }} AS upper_limit
)

, input_3 AS (
  SELECT {{ lower_limit_3 }} as lower_limit, {{ upper_limit_3 }} AS upper_limit
)

, input_4 AS (
  SELECT {{ lower_limit_4 }} as lower_limit, {{ upper_limit_4 }} AS upper_limit
)

, input_5 AS (
  SELECT {{ lower_limit_5 }} as lower_limit, {{ upper_limit_5 }} AS upper_limit
)

, input_6 AS (
  SELECT {{ lower_limit_6 }} as lower_limit, {{ upper_limit_6 }} AS upper_limit
)

, input_7 AS (
  SELECT {{ lower_limit_7 }} as lower_limit, {{ upper_limit_7 }} AS upper_limit
)

, input_8 AS (
  SELECT {{ lower_limit_8 }} as lower_limit, {{ upper_limit_8 }} AS upper_limit
)

, input_9 AS (
  SELECT {{ lower_limit_9 }} as lower_limit, {{ upper_limit_9 }} AS upper_limit
)

SELECT 'dummy_model_empty' AS test_case, lower_limit, upper_limit FROM input_1
UNION ALL
SELECT 'dummy_model_only_nulls' AS test_case, lower_limit, upper_limit FROM input_2
UNION ALL
SELECT 'dummy_model_standard' AS test_case, lower_limit, upper_limit FROM input_3
UNION ALL
SELECT 'dummy_model_empty_with_lower_output_true' AS test_case, lower_limit, upper_limit FROM input_4
UNION ALL
SELECT 'dummy_model_empty_with_lower_output_false' AS test_case, lower_limit, upper_limit FROM input_5
UNION ALL
SELECT 'dummy_model_only_nulls_with_lower_output_true' AS test_case, lower_limit, upper_limit FROM input_6
UNION ALL
SELECT 'dummy_model_only_nulls_with_lower_output_false' AS test_case, lower_limit, upper_limit FROM input_7
UNION ALL
SELECT 'dummy_model_standard_with_lower_output_true' AS test_case, lower_limit, upper_limit FROM input_8
UNION ALL
SELECT 'dummy_model_standard_with_lower_output_false' AS test_case, lower_limit, upper_limit FROM input_9
