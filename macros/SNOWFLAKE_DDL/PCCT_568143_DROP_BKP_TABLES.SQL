 {################# Step 1 DECLARE SCRIPTS/DDL NEED TO EXECUTED #################}
{%- set v_scripts -%} 
DROP TABLE EDWNEW{{env_var('DBT_DEP_ENV')}}.MART_SALES.POINT_OF_SALES_FACT_PV22221102;
DROP TABLE EDWNEW{{env_var('DBT_DEP_ENV')}}.MART_SALES.POINT_OF_SALES_CUSTOMER_BRIDGE_PV22221102;
DROP TABLE EDWNEW{{env_var('DBT_DEP_ENV')}}.MART_SALES.BILLING_FACT_PV22221102;
DROP TABLE EDWNEW{{env_var('DBT_DEP_ENV')}}.MART_SALES.BILLING_FACT_SRC_PV22221102;
DROP TABLE EDWNEW{{env_var('DBT_DEP_ENV')}}.MART_SALES.BILLING_CUSTOMER_BRIDGE_PV22221102;
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
SELECT 
    568143 AS PCCT_NUMBER  ,
    '{{v_scripts|replace("'","''")}}' AS DDL_EXECUTED ,
    'SRUTHI.KASBE' AS BIW_CREATED_BY,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_CREATED_DTTM,
    'SRUTHI.KASBE' AS BIW_UPD_BY ,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_UPD_DTTM
