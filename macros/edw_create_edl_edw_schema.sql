{% macro edw_create_edl_edw_schema(param_schema_name) -%}

{%- set v_scripts -%} 

CREATE SCHEMA EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  with managed access;
        grant ALL on ALL TABLES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL EXTERNAL TABLES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL VIEWS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL MATERIALIZED VIEWS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL STAGES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL FILE FORMATS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL SEQUENCES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL FUNCTIONS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL STREAMS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL TASKS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL PROCEDURES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant SELECT on ALL TABLES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on ALL EXTERNAL TABLES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on ALL VIEWS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on ALL MATERIALIZED VIEWS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE,READ on ALL STAGES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on ALL FILE FORMATS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on ALL SEQUENCES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on ALL FUNCTIONS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on ALL STREAMS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant MONITOR on ALL TASKS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on ALL PROCEDURES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;

        -- Future Grants
        grant ALL on FUTURE TABLES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE EXTERNAL TABLES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE VIEWS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE MATERIALIZED VIEWS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE STAGES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE FILE FORMATS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE SEQUENCES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE FUNCTIONS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE STREAMS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE TASKS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE PROCEDURES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant SELECT on FUTURE TABLES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on FUTURE EXTERNAL TABLES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on FUTURE VIEWS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on FUTURE MATERIALIZED VIEWS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE,READ on FUTURE STAGES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on FUTURE FILE FORMATS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on FUTURE SEQUENCES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on FUTURE FUNCTIONS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on FUTURE STREAMS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant MONITOR on FUTURE TASKS in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on FUTURE PROCEDURES in schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant usage on schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant usage on schema EDL{{env_var('DBT_DEP_ENV')}}.ETL_{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        
CREATE SCHEMA EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  with managed access;
        grant ALL on ALL TABLES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL EXTERNAL TABLES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL VIEWS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL MATERIALIZED VIEWS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL STAGES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL FILE FORMATS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL SEQUENCES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL FUNCTIONS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL STREAMS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL TASKS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on ALL PROCEDURES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant SELECT on ALL TABLES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on ALL EXTERNAL TABLES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on ALL VIEWS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on ALL MATERIALIZED VIEWS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE,READ on ALL STAGES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on ALL FILE FORMATS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on ALL SEQUENCES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on ALL FUNCTIONS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on ALL STREAMS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant MONITOR on ALL TASKS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on ALL PROCEDURES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;

        -- Future Grants
        grant ALL on FUTURE TABLES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE EXTERNAL TABLES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE VIEWS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE MATERIALIZED VIEWS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE STAGES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE FILE FORMATS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE SEQUENCES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE FUNCTIONS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE STREAMS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE TASKS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant ALL on FUTURE PROCEDURES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant SELECT on FUTURE TABLES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on FUTURE EXTERNAL TABLES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on FUTURE VIEWS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on FUTURE MATERIALIZED VIEWS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE,READ on FUTURE STAGES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on FUTURE FILE FORMATS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on FUTURE SEQUENCES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on FUTURE FUNCTIONS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant SELECT on FUTURE STREAMS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant MONITOR on FUTURE TASKS in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant USAGE on FUTURE PROCEDURES in schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;
        grant usage on schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRW;
        grant usage on schema EDW{{env_var('DBT_DEP_ENV')}}.{{param_schema_name}}  to role ENT{{env_var('DBT_DEP_ENV')}}_GENRO;

 {%endset%}
{% do run_query(v_scripts) %}

{% endmacro %}
