-- BQ Only 
{{ config(enabled=(target.type == 'bigquery' | as_bool()),
          materialized='table',
          tags=["requires_script"])
}}


select   
  cast(coalesce(array_of_structs_2[safe_offset(0)].y, array_of_structs_1[safe_offset(0)].y) as integer) as y,
  coalesce(array_of_structs_2[safe_offset(0)].z, array_of_structs_1[safe_offset(0)].z) as z,
  coalesce(simple_struct_2.y, simple_struct_1.y) as j

from {{ ref('data_combine_column_versions') }}
