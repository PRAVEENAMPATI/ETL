{% macro PCCT_621906_NPD_BKP_TABLE() -%}
 {################# Step 1 DECLARE SCRIPTS/DDL NEED TO EXECUTED #################}
{%- set v_scripts -%} 

CREATE OR REPLACE TABLE {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_NPD.PROJECTS_RISK_RPT_PV20230112 CLONE 
 {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_NPD.PROJECTS_RISK_RPT;

CREATE OR REPLACE TABLE {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_NPD.PROJECTS_SUMMARY_RPT_PV20230112 CLONE 
 {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_NPD.PROJECTS_SUMMARY_RPT;

CREATE OR REPLACE TABLE {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_NPD.PROJECTS_TASK_RPT_PV20230112 CLONE 
 {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_NPD.PROJECTS_TASK_RPT;

{%endset%}

{################# Step 2 EXECUTE SCRIPTS #################}
{% do run_query(v_scripts) %}

{################# Step 3 CAPTURE INTO DBT TABLE #################}
Insert into var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV').UTILITY_DBT.DBT_SNOWFLAKE_DDL_EXECUTION
SELECT 
    621906_3 AS PCCT_NUMBER  ,
    '{{v_scripts|replace("'","''")}}' AS DDL_EXECUTED ,
    'VINAY.SUBRAMANIAN' AS BIW_CREATED_BY,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_CREATED_DTTM,
    'VINAY.SUBRAMANIAN' AS BIW_UPD_BY ,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_UPD_DTTM
{% endmacro %}