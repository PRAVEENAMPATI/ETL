{% macro PCCT_675460() -%}
 {################# Step 1 DECLARE SCRIPTS/DDL NEED TO EXECUTED #################}
{%- set v_scripts -%} 

DELETE FROM  {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_SALES.BACKLOG_FACT WHERE PROCESS_DATE IN (
SELECT CALENDAR_DATE FROM  {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART.DATE WHERE CALENDAR_DATE IN 
(SELECT DISTINCT PROCESS_DATE FROM  {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_SALES.BACKLOG_FACT) AND FISCAL_MONTH_KEY!=REPLACE(SUBSTRING(CALENDAR_DATE,1,7),'-'));

{%endset%}

{################# Step 2 EXECUTE SCRIPTS #################}
{% do run_query(v_scripts) %}

{################# Step 3 CAPTURE INTO DBT TABLE #################}
Insert into var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV').UTILITY_DBT.DBT_SNOWFLAKE_DDL_EXECUTION
SELECT 
    675460 AS PCCT_NUMBER  ,
    '{{v_scripts|replace("'","''")}}' AS DDL_EXECUTED ,
    'VINAY.SUBRAMANIAN' AS BIW_CREATED_BY,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_CREATED_DTTM,
    'VINAY.SUBRAMANIAN' AS BIW_UPD_BY ,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_UPD_DTTM
{% endmacro %}