{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}


{% macro base_create_snowplow_quarantined_sessions() %}

    {% set create_quarantined_query %}
        with prep as (
        select
            cast(null as {{ snowplow_utils.type_max_string() }}) session_identifier
        )

        select *

        from prep
        where false

    {% endset %}

    {{ return(create_quarantined_query) }}

{% endmacro %}
