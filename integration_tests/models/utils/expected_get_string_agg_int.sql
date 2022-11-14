{%- if target.type in ['databricks', 'spark'] %}

  with data as (

    select

      '1;3;10' as result
  )

  select * from data

{%- else %}

  with data as (

    select

      '1;10;3' as result
  )

  select * from data

  {%- endif %}
