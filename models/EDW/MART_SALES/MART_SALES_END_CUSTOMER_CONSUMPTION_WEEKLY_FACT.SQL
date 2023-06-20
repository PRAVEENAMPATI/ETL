/*---------------------------------------------------------------------------
Command to run model:
--dbt run --full-refresh --select MART_SALES_END_CUSTOMER_CONSUMPTION_WEEKLY_FACT
--dbt run --select MART_SALES_END_CUSTOMER_CONSUMPTION_WEEKLY_FACT

Version     Date            Author               Description
-------     --------        -----------          --------------------------
1.0         31/01/2023      SRUTHI KASBE         Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['END_CUSTOMER_CONSUMPTION_WEEKLY_KEY']-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_END_CUSTOMER_CONSUMPTION_WEEKLY_FACT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_END_CUSTOMER_CONSUMPTION_WEEKLY_FACT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_END_CUSTOMER_CONSUMPTION_WEEKLY_FACT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{{
    config(
         description = 'Building table ECC_WEEKLY_FACT for sales mart '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='END_CUSTOMER_CONSUMPTION_WEEKLY_FACT'
        ,unique_key= v_pk_list
        ,tags ='MART_SALES'
        ,merge_update_columns=['FISCAL_WEEK_KEY', 'DIRECT_CUSTOMER_KEY', 'INDIRECT_CUSTOMER_KEY', 'END_CUSTOMER_KEY', 'DIRECT_CORPORATION_KEY', 'INDIRECT_CORPORATION_KEY',
                                'END_CORPORATION_KEY', 'ADJUSTED_END_CORPORATION_KEY', 'REFERENCE_CORPORATION_KEY', 'MARKET_PRODUCT_NUMBER_KEY', 'INTERNAL_PART_NUMBER_KEY',
                                'CUSTOMER_PART_NUMBER_KEY', 'DIRECT_CUSTOMER_CODE', 'INDIRECT_CUSTOMER_CODE', 'END_CUSTOMER_CODE', 'DIRECT_CORPORATION_CODE', 'INDIRECT_CORPORATION_CODE',
                                'END_CORPORATION_CODE', 'ADJUSTED_END_CORPORATION_CODE', 'REFERENCE_CORPORATION_CODE',
                                'MARKET_PRODUCT_NUMBER', 'INTERNAL_PART_NUMBER', 'CUSTOMER_PART_NUMBER', 'TRANSACTION_CURRENCY_ISO_CODE', 'DISTRIBUTOR_END_CUSTOMER_IDENTIFIER',
                                'DISTRIBUTOR_END_CUSTOMER_NAME', 'DISTRIBUTOR_SHIP_TO_CUSTOMER_IDENTIFIER', 'DISTRIBUTOR_SHIP_TO_CUSTOMER_NAME', 'PRICE_CONTRACT',
                                'SOURCE_OF_SALE', 'SOURCE_OF_DATA', 'ACTIVITY', 'END_CORPORATION_DECODE', 'COST_ADJUSTED_USD', 'COST_ADJUSTED_JPY', 'COST_ADJUSTED_EUR',
                                'COST_USD', 'COST_JPY', 'COST_EUR', 'DISTRIBUTOR_PRICE_USD', 'LOCAL_AMOUNT', 'LOCAL_COST_AMOUNT', 'LOCAL_DISTRIBUTOR_PRICE_AMOUNT',
                                'RESALE_NET_USD', 'RESALE_NET_JPY', 'RESALE_NET_EUR', 'RESALE_QUANTITY', 'RESALE_GROSS_USD', 'RESALE_GROSS_JPY', 'RESALE_GROSS_EUR',
                                'LOCAL_GROSS_AMOUNT', 'RESALE_GROSS_QUANTITY', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']
        ,post_hook= [ v_sql_upd_success_batch]
        )
}}

SELECT 
    END_CUSTOMER_CONSUMPTION_WEEKLY_KEY,
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
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    BIW_MD5_KEY
FROM 
{{ref('ETL_MART_SALES_END_CUSTOMER_CONSUMPTION_WEEKLY_FACT')}}