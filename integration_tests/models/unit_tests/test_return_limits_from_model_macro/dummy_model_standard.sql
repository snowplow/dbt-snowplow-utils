SELECT cast('2024-12-01 00:00:00' as {{ dbt.type_timestamp() }}) AS tstamp_col
UNION ALL
SELECT cast('2024-12-02 00:00:00' as {{ dbt.type_timestamp() }}) AS tstamp_col
  