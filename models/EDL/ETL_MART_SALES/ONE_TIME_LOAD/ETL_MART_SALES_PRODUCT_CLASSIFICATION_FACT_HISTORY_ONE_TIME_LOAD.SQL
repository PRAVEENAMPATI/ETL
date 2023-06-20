/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_PRODUCT_CLASSIFICATION_FACT_HISTORY_ONE_TIME_LOAD --vars 'is_backfill: True'

Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         23/02/2023      Sruthi Kasbe    Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['MARKET_PRODUCT_NUMBER_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_PRODUCT_CLASSIFICATION_FACT_HISTORY_ONE_TIME_LOAD'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{%- set V_EDW_DB = var('V_EDW_DEFAULT_DB') +env_var('DBT_DEP_ENV')+'.' -%}

{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building table PRODUCT_CLASSIFICATION_FACT_HISTORY_ONE_TIME_LOAD for MART_SALES'
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES'
        ,alias='PRODUCT_CLASSIFICATION_FACT_HISTORY_ONE_TIME_LOAD'
		,unique_key= v_pk_list
        ,tags =['MART_SALES']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

-----------since history table is a static table, we need to do one time load everytime we want to load all the history data-------

SELECT
    MD5(OBJECT_CONSTRUCT('COL1', PRODUCT_ID::STRING
                        ,'COL2',YYYYWK::STRING
                      )::STRING) AS MARKET_PRODUCT_NUMBER_KEY,
    PRODUCT_ID,
    YYYYWK AS FISCAL_WEEK_KEY,
    WEB_PART_NAME,
    QTY AS TOTAL_RESALE_QUANTITY,
    QTY_POS AS TOTAL_RESALE_QUANTITY_POS,
    QTY_BILLING AS TOTAL_RESALE_QUANTITY_BILLING,
    COST_REPORTED_USD AS TOTAL_COST_USD,
    RESALE_REPORTED_USD AS TOTAL_RESALE_NET_USD,
    SHIPTOCUST_CNT AS DISTRIBUTOR_SHIP_TO_CUSTOMER_COUNT,
    INDCUST_CNT AS INDIRECT_CUSTOMER_COUNT,
    ENDCUST_CNT AS END_CUSTOMER_COUNT,
    DIRREGION_CNT AS DIRECT_REGION_COUNT,
    ENDREGION_CNT AS END_REGION_COUNT,
    ENDCORP_CNT AS END_CORPORATION_COUNT,
    ENDCORP_CNT_POS AS END_CORPORATION_POS_COUNT,
    ENDCORP_CNT_BILLING AS END_CORPORATION_BILLING_COUNT,
    COMPETITOR_CNT_SIMILARREPLACE AS COMPETITOR_REPLACEMENT_SIMILAR_COUNT,
    COMPETITOR_CNT_DIRECTREPLACE AS COMPETITOR_REPLACEMENT_DIRECT_COUNT,
    COMPETITOR_CNT_SIMILARDIRECTREPLACE AS COMPETITOR_REPLACEMENT_DIRECT_SIMILAR_COUNT,
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
FROM 
    {{source('STG_SDM_PRESENTATION','FACT_PRODUCTCLASSIFICATION')}}