
{%- set all_models = snowplow_utils.get_incremental_manifest_status(ref('data_get_incremental_manifest_status'), ['a','b','c']) -%}
{%- set partial_models = snowplow_utils.get_incremental_manifest_status(ref('data_get_incremental_manifest_status'), ['b','d']) -%}
{%- set fake_incremental_manifest_table = snowplow_utils.get_incremental_manifest_table_relation('fake_manifest') -%}
{%- set no_manifest = snowplow_utils.get_incremental_manifest_status(fake_incremental_manifest_table, ['a','b','c']) -%}

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

union all

select
  'no manifest' as test_case,
  {{ snowplow_utils.cast_to_tstamp(no_manifest[0]) }} as min_last_success,
  {{ snowplow_utils.cast_to_tstamp(no_manifest[1]) }} as max_last_success,
  {{no_manifest[2]}} as models_matched_from_manifest,
  {{no_manifest[3]}} as has_matched_all_models
