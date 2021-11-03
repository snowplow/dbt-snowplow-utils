{# Returns an array of enabled models tagged with snowplow_web_incremental using dbts graph object. 
   Throws an error if untagged models are found that depend on the base_events_this_run model#}
{% macro get_enabled_snowplow_models(package_name, graph_object=none, models_to_run=var("models_to_run","")) -%}
  
  {# Override dbt graph object if graph_object is passed. Testing purposes #}
  {% if graph_object is not none %}
    {% set graph = graph_object %}
  {% endif %}
  
  {# models_to_run optionally passed using dbt ls command. This returns a string of models to be run. Split into list #}
  {% if models_to_run|length %}
    {% set selected_models = models_to_run.split(" ") %}
  {% else %}
    {% set selected_models = none %}
  {% endif %}

  {% set enabled_models = [] %}
  {% set untagged_snowplow_models = [] %}
  {% set snowplow_model_tag = package_name+'_incremental' %}
  {% set snowplow_events_this_run_path = 'model.'+package_name+'.'+package_name+'_base_events_this_run' %}

  {% if execute %}
    
    {% set nodes = graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
    
    {% for node in nodes %}
      {# If selected_models is specified, filter for these models #}
      {% if selected_models is none or node.name in selected_models %}

        {% if node.config.enabled and snowplow_model_tag not in node.tags and snowplow_events_this_run_path in node.depends_on.nodes %}

          {%- do untagged_snowplow_models.append(node.name) -%}

        {% endif %}

        {% if node.config.enabled and snowplow_model_tag in node.tags %}

          {%- do enabled_models.append(node.name) -%}

        {% endif %}

      {% endif %}
      
    {% endfor %}

    {% if untagged_snowplow_models|length %}
    {#
      Prints warning for models that reference snowplow_base_events_this_run but are untagged as 'snowplow_web_incremental'
      Without this tagging these models will not be inserted into the manifest, breaking the incremental logic.
      Only catches first degree dependencies rather than all downstream models
    #}
      {%- do exceptions.raise_compiler_error("Snowplow Warning: Untagged models referencing '"+package_name+"_base_events_this_run'. Please refer to the Snowplow docs on tagging. " 
      + "Models: "+ ', '.join(untagged_snowplow_models)) -%}
    
    {% endif %}

  {% endif %}

  {{ return(enabled_models) }}

{%- endmacro %}
