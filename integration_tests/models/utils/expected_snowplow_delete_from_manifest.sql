{{ config(tags=["requires_script"]) }}

select *

from {{ ref('data_snowplow_delete_from_manifest') }}
where model not in ({% for model in var("models_to_delete",[]) %} '{{ model }}' {% if not loop.last %}, {% endif %} {% endfor %})
