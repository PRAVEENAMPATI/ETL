{% macro PCCT_622673()-%}
{################# Step 1 DECLARE SCRIPTS/DDL NEED TO EXECUTED #################}
{%- set v_scripts -%} 

DROP TABLE EDW{{env_var('DBT_DEP_ENV')}}.MART_SALES.PRODUCT_HIST;

{%endset%}

{################# Step 2 EXECUTE SCRIPTS #################}
{% do run_query(v_scripts) %}

{################# Step 3 CAPTURE INTO DBT TABLE #################}
{{
    config(
         description = 'Capture DBT DDL Execution Scripts'
        ,transient=false
        ,materialized='incremental'
        ,database = var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV')
        ,schema ='UTILITY_DBT'
        ,alias='DBT_SNOWFLAKE_BKP_DDL_EXECUTION'
        ,unique_key= 'PCCT_NUMBER'
        )
}}

{################# Step 3 CAPTURE INTO DBT TABLE #################}
Insert into var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV').UTILITY_DBT.DBT_SNOWFLAKE_DDL_EXECUTION
SELECT 
    586339 AS PCCT_NUMBER ,
    '{{v_scripts|replace("'","''")}}' AS DDL_EXECUTED,
    'RAMYA.NAGARAJ' AS BIW_CREATED_BY,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_CREATED_DTTM,
    'RAMYA.NAGARAJ' AS BIW_UPD_BY ,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_UPD_DTTM
{% endmacro %}