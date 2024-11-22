{% macro print_debug_info() %}

    {%- do log('=== DBT Project Debug Information ===', info=True) -%}
    
    {# Project Information #}
    {%- do log('--- Project Details ---', info=True) -%}
    {%- do log('Project name: ' ~ project_name, info=True) -%}
    {%- do log('DBT version: ' ~ dbt_version, info=True) -%}
    
    {# Target Information #}
    {%- do log('--- Target Details ---', info=True) -%}
    {%- do log('Target name: ' ~ target.name, info=True) -%}
    {%- do log('Target schema: ' ~ target.schema, info=True) -%}
    {%- do log('Target type: ' ~ target.type, info=True) -%}

    {# Run Information #}
    {%- do log('--- Run Details ---', info=True) -%}
    {%- do log('Invocation ID: ' ~ invocation_id, info=True) -%}
    {%- do log('Run started at: ' ~ run_started_at, info=True) -%}
    {%- if flags.WHICH in ['run', 'build'] -%}
        {%- do log('Full refresh: ' ~ flags.FULL_REFRESH, info=True) -%}
    {%- endif -%}
    
    {%- do log('=== End Debug Information ===', info=True) -%}

{% endmacro %}