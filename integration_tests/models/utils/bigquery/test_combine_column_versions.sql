{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# Test 1: Select all fields from array of structs, taking second element.
   Test 2: Select subset of fields from nested structs & rename
   Test 3: Select all > 1st level fields. Use relationship alias
   Test 4: Disable field alias #}

{% set test_2_required_fields = [
   'name',
   ('specs.power_rating', 'product_power_rating'),
   'specs.accessories'
] %}

{% set test_1_actual = snowplow_utils.combine_column_versions(relation=ref('data_combine_column_versions'),
                                                              column_prefix='staff_v',
                                                              array_index=1) %}

{% set test_2_actual = snowplow_utils.combine_column_versions(relation=ref('data_combine_column_versions'),
                                                              column_prefix='product_v',
                                                              required_fields=test_2_required_fields) %}

{% set test_3_actual = snowplow_utils.combine_column_versions(relation=ref('data_combine_column_versions'),
                                                              column_prefix='product_v',
                                                              nested_level=1,
                                                              relation_alias='a',
                                                              level_filter='greaterthan') %}

{% set test_4_actual = snowplow_utils.combine_column_versions(relation=ref('data_combine_column_versions'),
                                                              column_prefix='product_v',
                                                              include_field_alias=false,
                                                              required_fields=['specs.volume']) %}

{% set test_5_actual = snowplow_utils.combine_column_versions(relation=ref('data_combine_column_versions'),
                                                              column_prefix='person_1',
                                                              exclude_versions=['1_0_1', '1_0_2', '1_1_30']) %}

{% set test_6_actual = snowplow_utils.combine_column_versions(relation=ref('data_combine_column_versions'),
                                                              column_prefix='test',
                                                              exclude_versions=['new_1_0_0', '1_1_0']) %}

with prep as (
   select
      -- Test 1
      {{ test_1_actual|join(',\n') }},
      -- Test 2
      {{ test_2_actual|join(',\n') }},
      -- Test 3
      {{ test_3_actual|join(',\n') }},
      -- Test 4
      {{ test_4_actual|join(',') }} as product_volume,
      -- Test 5
      {{ test_5_actual|join(',') }},
      -- Test 6
      {{ test_6_actual|join(',') }}

   from {{ ref('data_combine_column_versions') }} a
)

-- Equality test doesn't like nested data. Stringify and agg.
select
   concat("[", string_agg(to_json_string(p), ","), "]") as summary

from prep as p
