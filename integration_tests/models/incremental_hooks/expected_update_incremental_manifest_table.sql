
select 
  model,
  expected_last_success as last_success

from {{ ref('data_update_incremental_manifest_table') }}
where expected_last_success is not null
