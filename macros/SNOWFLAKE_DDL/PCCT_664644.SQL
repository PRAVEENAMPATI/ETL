{% macro PCCT_664644()-%}
{################# Step 1 DECLARE SCRIPTS/DDL NEED TO EXECUTED #################}
{%- set v_scripts -%} 

DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.FISCAL_MONTH;
DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.FISCAL_QUARTER;
DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.FISCAL_WEEK;
DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.FISCAL_YEAR;
DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.MONTH;
DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.QUARTER;
DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.ENTERPRISE.YEAR;
DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.SALES.BILLING_FACT_QUARTERLY;
DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.SALES.BILLING_FACT_WEEKLY;
DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.SALES.END_CUSTOMER_CONSUMPTION_FACT_QUARTERLY;
DROP VIEW {{env_var('DBT_ANALYTICS_DB')~env_var('DBT_DEP_ENV')}}.SALES.PRODUCT_CLASSIFICATION_FACT_CURRENT;

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
    664644 AS PCCT_NUMBER ,
    '{{v_scripts|replace("'","''")}}' AS DDL_EXECUTED,
    'SRUTHI.KASBE' AS BIW_CREATED_BY,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_CREATED_DTTM,
    'SRUTHI.KASBE' AS BIW_UPD_BY ,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_UPD_DTTM
{% endmacro %}