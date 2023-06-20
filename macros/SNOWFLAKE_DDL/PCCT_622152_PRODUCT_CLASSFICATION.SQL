/*---------------------------------------------------------------------------
command to run
dbt run-operation  PCCT_622152_INSERT_MART_SALES_PRODUCT_CLASSIFICATION_FACT
Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         22 Dec 2022      VINAYAKA CN       INSERTING HOSTIORICAL DATA
---------------------------------------------------------------------------*/
{% macro PCCT_622152_INSERT_MART_SALES_PRODUCT_CLASSIFICATION_FACT() -%}
{################# Step 1 DECLARE SCRIPTS/DDL NEED TO EXECUTED #################}
{%- set v_scripts -%} 

<<<<<<< HEAD


=======
>>>>>>> 531a20e5e146d2695400be0c50c6cae0bfbcc355
INSERT INTO  {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_SALES.PRODUCT_CLASSIFICATION_FACT (
    MARKET_PRODUCT_NUMBER_KEY,
    PRODUCT_ID,
    FISCAL_WEEK_KEY,
    WEB_PART_NAME,
    TOTAL_RESALE_QUANTITY,
    TOTAL_RESALE_QUANTITY_POS,
    TOTAL_RESALE_QUANTITY_BILLING,
    TOTAL_COST_USD,
    TOTAL_RESALE_NET_USD,
    DISTRIBUTOR_SHIP_TO_CUSTOMER_COUNT,
    INDIRECT_CUSTOMER_COUNT,
    END_CUSTOMER_COUNT,
    DIRECT_REGION_COUNT,
    END_REGION_COUNT,
    END_CORPORATION_COUNT,
    END_CORPORATION_POS_COUNT,
    END_CORPORATION_BILLING_COUNT,
    COMPETITOR_REPLACEMENT_SIMILAR_COUNT,
    COMPETITOR_REPLACEMENT_DIRECT_COUNT,
    COMPETITOR_REPLACEMENT_DIRECT_SIMILAR_COUNT,
    PRODUCT_CLASSIFICATION,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID,
    BIW_MD5_KEY
)
SELECT 
    MD5(OBJECT_CONSTRUCT('COL1', PRODUCT_ID::STRING
                          ,'COL2',YYYYWK::STRING
                      )::STRING) AS MARKET_PRODUCT_NUMBER_KEY,       
    PRODUCT_ID,
    YYYYWK,
    WEB_PART_NAME,
    QTY,
    QTY_POS,
    QTY_BILLING,
    COST_REPORTED_USD,
    RESALE_REPORTED_USD,
    SHIPTOCUST_CNT,
    INDCUST_CNT,
    ENDCUST_CNT,
    DIRREGION_CNT,
    ENDREGION_CNT,
    ENDCORP_CNT,
    ENDCORP_CNT_POS,
    ENDCORP_CNT_BILLING,
    COMPETITOR_CNT_SIMILARREPLACE,
    COMPETITOR_CNT_DIRECTREPLACE,
    COMPETITOR_CNT_SIMILARDIRECTREPLACE,
    PRODUCT_CLASSIFICATION,
    current_timestamp::timestamp_ntz BIW_INS_DTTM,
    current_timestamp::timestamp_ntz BIW_UPD_DTTM,
    9999 AS BIW_BATCH_ID,
    md5(object_construct ('col1',PRODUCT_ID::string, 'col2',YYYYWK::string,
            'col3',WEB_PART_NAME::string, 'col4',QTY::string, 'col5',QTY_POS::string,
            'col6',QTY_BILLING::string, 'col7',COST_REPORTED_USD::string, 'col8',RESALE_REPORTED_USD::string, 'col9',SHIPTOCUST_CNT::string,
            'col10',INDCUST_CNT::string, 'col11',ENDCUST_CNT::string, 'col12',DIRREGION_CNT::string, 'col13',ENDREGION_CNT::string,
            'col14',ENDCORP_CNT::string, 'col15',ENDCORP_CNT_POS::string, 'col16',ENDCORP_CNT_BILLING::string,
            'col17',COMPETITOR_CNT_SIMILARREPLACE::string, 'col18',COMPETITOR_CNT_DIRECTREPLACE::string, 'col19',COMPETITOR_CNT_SIMILARDIRECTREPLACE::string,
            'col20',PRODUCT_CLASSIFICATION::string)::string )::BINARY as BIW_MD5_KEY 
FROM {{env_var('DBT_EDL_DB')~env_var('DBT_DEP_ENV')}}.STG_SDM_PRESENTATION.FACT_PRODUCTCLASSIFICATION 
WHERE YYYYWK
NOT IN (SELECT FISCAL_WEEK_PRIOR FROM {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART.DATE where CALENDAR_DATE=CURRENT_DATE());

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
        ,alias='DBT_SNOWFLAKE_DDL_EXECUTION'
        ,unique_key= 'PCCT_NUMBER'
        )
}}
SELECT 
    622152 AS PCCT_NUMBER  ,
    '{{v_scripts|replace("'","''")}}' AS DDL_EXECUTED ,
    'VINAYAKA.CN' AS BIW_CREATED_BY,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_CREATED_DTTM,
    'VINAYAKA.CN' AS BIW_UPD_BY ,
    CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ AS BIW_UPD_DTTM

{% endmacro %}