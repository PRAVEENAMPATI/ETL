{% macro PCCT_590538() -%}
{################# Step 1 DECLARE SCRIPTS/DDL NEED TO EXECUTED #################}
{%- set v_scripts -%} 
-- NOT REQUIRED 
 --UPDATE EDWNEW{{env_var('DBT_DEP_ENV')}}.MART_SALES.CUSTOMER MART_CUST SET MART_CUST.CUSTOMER_SOURCE='GDMS'
 --WHERE MART_CUST.CUSTOMER_CODE IN (SELECT CUST_CD FROM EDL{{env_var('DBT_DEP_ENV')}}.STG_DWH_MARTS.DISTRIBUTOR_POS_CUSTOMER_DIM)
 --AND MART_CUST.CUSTOMER_SOURCE IS NULL;

{%endset%}

{################# Step 2 EXECUTE SCRIPTS #################}
{% do run_query(v_scripts) %}

{################# Step 3 CAPTURE INTO DBT TABLE #################}
Insert into var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV').UTILITY_DBT.DBT_SNOWFLAKE_DDL_EXECUTION
SELECT 
    590538 AS PCCT_NUMBER  ,
    '{{v_scripts|replace("'","''")}}' AS DDL_EXECUTED ,
    'SRUTHI.KASBE' AS BIW_CREATED_BY,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_CREATED_DTTM,
    'SRUTHI.KASBE' AS BIW_UPD_BY ,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_UPD_DTTM
{% endmacro %}