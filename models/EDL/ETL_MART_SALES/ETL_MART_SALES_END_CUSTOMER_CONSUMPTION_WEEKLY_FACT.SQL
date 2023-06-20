/*--------------------------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_END_CUSTOMER_CONSUMPTION_WEEKLY_FACT

Version     Date            Author             Description
-------     --------        -----------        ----------------------------------
1.0         31/01/2023      Sruthi Kasbe       End Customer Consumption model
----------------------------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
/*--DBT Variable used during development*/
{% if is_incremental() %}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list = edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
/*--DBT Variable*/
/*--SELECT {{v_all_column_list}}*/
/*--SELECT {{v_update_column_list}}*/
/*--SELECT {{v_md5_column_list}}*/
{% endif %}

{################# Batch control insert and update SQL #################}
{%-set v_pk_list = ['END_CUSTOMER_CONSUMPTION_WEEKLY_KEY']-%}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_END_CUSTOMER_CONSUMPTION_WEEKLY_FACT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}


{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building ETL view ECC_WEEKLY_FACT for Sales Mart'
        ,materialized='view'
        ,schema ='ETL_MART_SALES'
        ,alias='END_CUSTOMER_CONSUMPTION_WEEKLY_FACT'
        ,tags =['MART_SALES']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT 
    ECC_POS_KEY AS END_CUSTOMER_CONSUMPTION_WEEKLY_KEY,
    FISCAL_WEEK_KEY,
    DIRECT_CUSTOMER_KEY,
    INDIRECT_CUSTOMER_KEY,
    END_CUSTOMER_KEY,
    DIRECT_CORPORATION_KEY,
    INDIRECT_CORPORATION_KEY,
    END_CORPORATION_KEY,
    ADJUSTED_END_CORPORATION_KEY,
    REFERENCE_CORPORATION_KEY,
    MARKET_PRODUCT_NUMBER_KEY,
    INTERNAL_PART_NUMBER_KEY,
    CUSTOMER_PART_NUMBER_KEY,
    DIRECT_CUSTOMER_CODE,
    INDIRECT_CUSTOMER_CODE,
    END_CUSTOMER_CODE,
    DIRECT_CORPORATION_CODE,
    INDIRECT_CORPORATION_CODE,
    END_CORPORATION_CODE,
    ADJUSTED_END_CORPORATION_CODE,
    REFERENCE_CORPORATION_CODE,
    MARKET_PRODUCT_NUMBER,
    INTERNAL_PART_NUMBER,
    CUSTOMER_PART_NUMBER,
    TRANSACTION_CURRENCY_ISO_CODE,
    DISTRIBUTOR_END_CUSTOMER_IDENTIFIER,
    DISTRIBUTOR_END_CUSTOMER_NAME,
    DISTRIBUTOR_SHIP_TO_CUSTOMER_IDENTIFIER,
    DISTRIBUTOR_SHIP_TO_CUSTOMER_NAME,
    PRICE_CONTRACT,
    SOURCE_OF_SALE,
    SOURCE_OF_DATA,
    ACTIVITY,
    END_CORPORATION_DECODE,
    COST_ADJUSTED_USD,
    COST_ADJUSTED_JPY,
    COST_ADJUSTED_EUR,
    COST_USD,
    COST_JPY,
    COST_EUR,
    DISTRIBUTOR_PRICE_USD,
    LOCAL_AMOUNT,
    LOCAL_COST_AMOUNT,
    LOCAL_DISTRIBUTOR_PRICE_AMOUNT,
    RESALE_NET_USD,
    RESALE_NET_JPY,
    RESALE_NET_EUR,
    RESALE_QUANTITY,
    RESALE_GROSS_USD,
    RESALE_GROSS_JPY,
    RESALE_GROSS_EUR,
    LOCAL_GROSS_AMOUNT,
    RESALE_GROSS_QUANTITY,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID,
    BIW_MD5_KEY
FROM 
{{ref('ETL_MART_SALES_END_CUSTOMER_CONSUMPTION_POS_WEEKLY_FACT')}}
UNION ALL 
SELECT 
    ECC_BILLING_KEY AS END_CUSTOMER_CONSUMPTION_WEEKLY_KEY,
    FISCAL_WEEK_KEY,
    DIRECT_CUSTOMER_KEY,
    INDIRECT_CUSTOMER_KEY,
    END_CUSTOMER_KEY,
    DIRECT_CORPORATION_KEY,
    INDIRECT_CORPORATION_KEY,
    END_CORPORATION_KEY,
    ADJUSTED_END_CORPORATION_KEY,
    REFERENCE_CORPORATION_KEY,
    MARKET_PRODUCT_NUMBER_KEY,
    INTERNAL_PART_NUMBER_KEY,
    CUSTOMER_PART_NUMBER_KEY,
    DIRECT_CUSTOMER_CODE,
    INDIRECT_CUSTOMER_CODE,
    END_CUSTOMER_CODE,
    DIRECT_CORPORATION_CODE,
    INDIRECT_CORPORATION_CODE,
    END_CORPORATION_CODE,
    ADJUSTED_END_CORPORATION_CODE,
    REFERENCE_CORPORATION_CODE,
    MARKET_PRODUCT_NUMBER,
    INTERNAL_PART_NUMBER,
    CUSTOMER_PART_NUMBER,
    TRANSACTION_CURRENCY_ISO_CODE,
    DISTRIBUTOR_END_CUSTOMER_IDENTIFIER,
    DISTRIBUTOR_END_CUSTOMER_NAME,
    DISTRIBUTOR_SHIP_TO_CUSTOMER_IDENTIFIER,
    DISTRIBUTOR_SHIP_TO_CUSTOMER_NAME,
    PRICE_CONTRACT,
    SOURCE_OF_SALE,
    SOURCE_OF_DATA,
    ACTIVITY,
    END_CORPORATION_DECODE,
    COST_ADJUSTED_USD,
    COST_ADJUSTED_JPY,
    COST_ADJUSTED_EUR,
    COST_USD,
    COST_JPY,
    COST_EUR,
    DISTRIBUTOR_PRICE_USD,
    LOCAL_AMOUNT,
    LOCAL_COST_AMOUNT,
    LOCAL_DISTRIBUTOR_PRICE_AMOUNT,
    RESALE_NET_USD,
    RESALE_NET_JPY,
    RESALE_NET_EUR,
    RESALE_QUANTITY,
    RESALE_GROSS_USD,
    RESALE_GROSS_JPY,
    RESALE_GROSS_EUR,
    LOCAL_GROSS_AMOUNT,
    RESALE_GROSS_QUANTITY,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID,
    BIW_MD5_KEY
FROM 
{{ref('ETL_MART_SALES_END_CUSTOMER_CONSUMPTION_BILLING_WEEKLY_FACT')}}