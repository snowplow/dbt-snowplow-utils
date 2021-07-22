{{ config(
     pre_hook=["{{ snowplow_utils.alter_table_sql(
                                relation=ref('data_alter_table_sql'),
                                required_columns=[['model', dbt_utils.type_string()],
                                                  ['id', dbt_utils.type_int()],
                                                  ['last_success', dbt_utils.type_timestamp()],
                                                  ['description', dbt_utils.type_string()]]
                                                  ) }}"]
        ) }}



select
  *
from {{ ref('data_alter_table_sql') }}
