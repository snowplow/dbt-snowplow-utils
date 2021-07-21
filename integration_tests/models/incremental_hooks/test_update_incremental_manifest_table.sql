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
