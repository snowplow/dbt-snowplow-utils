{{ 
  config(tags=["requires_script"]) 
}}

select * {{ ref('fail_model') }}
