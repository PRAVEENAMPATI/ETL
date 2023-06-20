{% macro PCCT_626640() -%}
 {################# Step 1 DECLARE SCRIPTS/DDL NEED TO EXECUTED #################}
{%- set v_scripts -%} 

DELETE FROM {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_SALES.CORPORATION WHERE  LENGTH(CORPORATION_CODE)=5;

{%endset%}

{################# Step 2 EXECUTE SCRIPTS #################}
{% do run_query(v_scripts) %}

{################# Step 3 CAPTURE INTO DBT TABLE #################}
Insert into var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV').UTILITY_DBT.DBT_SNOWFLAKE_DDL_EXECUTION
SELECT 
<<<<<<< HEAD
    601741 AS PCCT_NUMBER  ,
=======
    626640 AS PCCT_NUMBER  ,
>>>>>>> 531a20e5e146d2695400be0c50c6cae0bfbcc355
    '{{v_scripts|replace("'","''")}}' AS DDL_EXECUTED ,
    'VINAY.SUBRAMANIAN' AS BIW_CREATED_BY,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_CREATED_DTTM,
    'VINAY.SUBRAMANIAN' AS BIW_UPD_BY ,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_UPD_DTTM
{% endmacro %}