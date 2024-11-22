{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro base_create_snowplow_incremental_manifest_t() %}

    {% set create_manifest_query %}
        with prep as (
        select
            cast(null as {{ snowplow_utils.type_max_string() }}) model,
            cast('1970-01-01' as {{ type_timestamp() }}) as first_success,
            cast('1970-01-01' as {{ type_timestamp() }}) as last_success
        )

        select *

        from prep
        where false
    {% endset %}

    {{ return(create_manifest_query) }}

{% endmacro %}
