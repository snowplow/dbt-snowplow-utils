{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{%- set all_models = snowplow_utils.get_incremental_manifest_status(ref('data_get_incremental_manifest_status'), ['a','b','c']) -%}
{%- set partial_models = snowplow_utils.get_incremental_manifest_status(ref('data_get_incremental_manifest_status'), ['b','d']) -%}

select
  'all model_in_run exist in manifest' as test_case,
  {{ snowplow_utils.cast_to_tstamp(all_models[0]) }} as min_last_success,
  {{ snowplow_utils.cast_to_tstamp(all_models[1]) }} as max_last_success,
  {{all_models[2]}} as models_matched_from_manifest,
  {{all_models[3]}} as has_matched_all_models

union all

select
  'some model_in_run exist in manifest' as test_case,
  {{ snowplow_utils.cast_to_tstamp(partial_models[0]) }} as min_last_success,
  {{ snowplow_utils.cast_to_tstamp(partial_models[1]) }} as max_last_success,
  {{partial_models[2]}} as models_matched_from_manifest,
  {{partial_models[3]}} as has_matched_all_models
