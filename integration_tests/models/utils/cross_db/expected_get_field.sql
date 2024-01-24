{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{
  config(
    tags = ['get_field'],
    )
}}

select
    {% if target.type in ['snowflake'] -%}'alice'::variant{%- else -%}'alice'{%- endif %} as nas_col1,
    5 as nas_col3,
    {% if target.type in ['snowflake'] -%}'alice'::variant{%- else -%}'alice'{%- endif %} as nas_ta_col1,
    5 as nas_ta_col3,
    'alice' as nas_ty_col1,
    '5' as nas_ty_col3,
    'alice' as nas_ta_ty_col1,
    '5' as nas_ta_ty_col3,

    {% if target.type in ['snowflake'] -%}'alice'::variant{%- else -%}'alice'{%- endif %} as as_col1_ind0,
    5 as as_col3_ind0,
    {% if target.type in ['snowflake'] -%}'alice'::variant{%- else -%}'alice'{%- endif %} as as_ta_col1_ind0,
    5 as as_ta_col3_ind0,
    'alice' as as_ta_ty_col1_ind0,
    '5' as as_ta_ty_col3_ind0,
    'alice' as as_ty_col1_ind0,
    '5' as as_ty_col3_ind0,
    'charlie' as as_ta_ty_col1_ind1,
    '9' as as_ta_ty_col3_ind1
from
    {{ ref('data_get_field') }} a
