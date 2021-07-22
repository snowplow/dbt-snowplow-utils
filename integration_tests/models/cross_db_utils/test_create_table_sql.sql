{# Hacky: Cant pass table_to_create to config so contruct twice.
   Pre hook: Create dummy staging table
   Model: select from dummy staging to create 'this' i.e. test_create_table_sql.
   Test: equality between test_create_table_sql and data_create_table_sql_expected. Both empty tables  #}

{%- set table_to_create = api.Relation.create(
      database=this.database,
      schema=this.schema,
      identifier='staging_create_table_sql') -%}

{{ config(
     pre_hook=["{{ snowplow_utils.create_table_sql(
                                relation=api.Relation.create(
                                        database=this.database,
                                        schema=this.schema,
                                        identifier='staging_create_table_sql'),
                                required_columns=[['model', dbt_utils.type_string()],
                                                  ['id', dbt_utils.type_int()],
                                                  ['last_success', dbt_utils.type_timestamp()],
                                                  ['description', dbt_utils.type_string()]]
                                                  ) }}"]
        ) }}

select
  *
from {{ table_to_create }}


