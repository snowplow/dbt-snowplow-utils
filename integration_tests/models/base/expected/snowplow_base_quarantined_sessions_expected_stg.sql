{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{
    config(
        tags=['base_macro']
    )
}}

{% if var("snowplow__custom_test", false) %}
select *

from {{ ref('snowplow_base_quarantined_sessions_expected_custom') }}
{% elif var("snowplow__session_test", false) %}
select *

from {{ ref('snowplow_base_quarantined_sessions_expected_sessions_custom') }}
{% else %}
select *

from {{ ref('snowplow_base_quarantined_sessions_expected') }}
{% endif %}
