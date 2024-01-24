{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{ config(post_hook="{{ snowplow_utils.update_incremental_manifest_table(this,
                                                                         ref('data_update_incremental_manifest_table'),
                                                                        ['a','b','c']) }}",
          materialized="table"
   )
}}

select
  model,
  last_success

from {{ ref('data_update_incremental_manifest_table') }}
where is_in_manifest
