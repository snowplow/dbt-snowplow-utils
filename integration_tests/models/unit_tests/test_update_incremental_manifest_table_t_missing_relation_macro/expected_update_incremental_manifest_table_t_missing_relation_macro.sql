{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
  The base events table referenced by the post_hook in test_update_incremental_manifest_table_t_missing_relation_macro
  does not exist, so the merge should be skipped entirely and the table should be left exactly as its own
  select statement produced it.
#}

select 'a' as model, cast('2021-01-01 00:00:00' as {{ dbt.type_timestamp() }}) as last_success, cast('2021-01-01 00:00:00' as {{ dbt.type_timestamp() }}) as first_success
union all
select 'b' as model, cast('2021-01-02 00:00:00' as {{ dbt.type_timestamp() }}) as last_success, cast('2021-01-02 00:00:00' as {{ dbt.type_timestamp() }}) as first_success
