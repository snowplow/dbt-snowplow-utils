-- BQ Only 
{{ config(enabled=(target.type == 'bigquery' | as_bool()),
          materialized='table',
          tags=["requires_script"])
}}


{# Test 1: Select fields from array of structs. No optional args passed.
   Test 2: Select subset of fields from struct, rename and use model alias.
   Test 3: Pass renamed_fields arg but not source_fields. Performed with bash script as returns compiler error.
   Test 4: Pass renamed_fields + source_field arg but mismatch in length. Performed with bash script as returns compiler error.  #}

{# use vars so we can sub in values for test 3,4 #}
{# source_fields hacky. Needed to allow us to pass source_fields as none without calling default value "y". Needed for Test 3. #}
{% set source_fields = none if var("source_fields", ["y"]) == 'none' else var("source_fields", ["y"]) %}
{% set renamed_fields = var("renamed_fields", ["j"]) %}

{% set test_1_actual_results = snowplow_utils.combine_column_versions(relation=ref('data_combine_column_versions'),
                                                                      column_prefix='array_of_structs') %}

{% set test_2_actual_results = snowplow_utils.combine_column_versions(relation=ref('data_combine_column_versions'),
                                                                      column_prefix='simple_struct',
                                                                      source_fields=source_fields,
                                                                      renamed_fields=renamed_fields,
                                                                      relation_alias='a' ) %}


select
   {{ test_1_actual_results|join(',\n') }},
   {{ test_2_actual_results|join(',\n') }}

from {{ ref('data_combine_column_versions') }} a
