{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{%- set all_models = snowplow_utils.get_incremental_manifest_status_t(ref('data_get_incremental_manifest_status_t'), ['a','b','c']) -%}
{%- set partial_models = snowplow_utils.get_incremental_manifest_status_t(ref('data_get_incremental_manifest_status_t'), ['b','d','e']) -%}

with prep as (
select
  'all model_in_run exist in manifest' as test_case,
  {{ snowplow_utils.cast_to_tstamp(all_models[0]) }} as min_first_success,
  {{ snowplow_utils.cast_to_tstamp(all_models[1]) }} as max_first_success,
  {{ snowplow_utils.cast_to_tstamp(all_models[2]) }} as min_last_success,
  {{ snowplow_utils.cast_to_tstamp(all_models[3]) }} as max_last_success,
  {{all_models[4]}} as models_matched_from_manifest,
  {{all_models[5]}} as sync_count,
  {{all_models[6]}} as has_matched_all_models

union all

select
  'some model_in_run exist in manifest' as test_case,
  {{ snowplow_utils.cast_to_tstamp(all_models[0]) }} as min_first_success,
  {{ snowplow_utils.cast_to_tstamp(all_models[1]) }} as max_first_success,
  {{ snowplow_utils.cast_to_tstamp(partial_models[2]) }} as min_last_success,
  {{ snowplow_utils.cast_to_tstamp(partial_models[3]) }} as max_last_success,
  {{partial_models[4]}} as models_matched_from_manifest,
  {{partial_models[5]}} as sync_count,
  {{partial_models[6]}} as has_matched_all_models

)

select
  test_case,
  min_first_success,
  max_first_success,
  min_last_success,
  max_last_success,
  models_matched_from_manifest,
  sync_count,
  cast(has_matched_all_models as {{ dbt.type_boolean() }}) as has_matched_all_models

from prep
