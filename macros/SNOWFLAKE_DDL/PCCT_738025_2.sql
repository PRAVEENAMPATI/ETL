{% macro PCCT_738025_2() -%}
 {################# Step 1 DECLARE SCRIPTS/DDL NEED TO EXECUTED #################}
{%- set v_scripts -%} 

CREATE OR REPLACE TRANSIENT TABLE {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_SALES.BACKLOG_FACT_SRC CLONE 
 {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_SALES.BACKLOG_FACT;

ALTER TABLE {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_SALES.BACKLOG_FACT_SRC
ADD COLUMN BACKLOG_TYPE VARCHAR(12);


{%endset%}

{################# Step 2 EXECUTE SCRIPTS #################}
{% do run_query(v_scripts) %}

{################# Step 3 CAPTURE INTO DBT TABLE #################}
Insert into var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV').UTILITY_DBT.DBT_SNOWFLAKE_DDL_EXECUTION
SELECT 
    738025_2 AS PCCT_NUMBER  ,
    '{{v_scripts|replace("'","''")}}' AS DDL_EXECUTED ,
    'SHIPRA.SHETTY' AS BIW_CREATED_BY,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_CREATED_DTTM,
    'SHIPRA.SHETTY' AS BIW_UPD_BY ,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_UPD_DTTM
{% endmacro %}