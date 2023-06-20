/*---------------------------------------------------------------------------
Command to run model:
--dbt build --full-refresh --select +MART_SALES_LTSA_DEMAND_WEEKLY_FACT+
--dbt build --select MART_SALES_LTSA_DEMAND_WEEKLY_FACT 

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         03/27/2023      Vinay Subramanian    Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['LTSA_DEMAND_WEEKLY_KEY' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_LTSA_DEMAND_WEEKLY_FACT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_LTSA_DEMAND_WEEKLY_FACT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_with_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_SALES_LTSA_DEMAND_WEEKLY_FACT'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_with_select_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_LTSA_DEMAND_WEEKLY_FACT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building table CUSTOMER dimension for sales mart '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='LTSA_DEMAND_WEEKLY_FACT'
        ,unique_key= v_pk_list
        ,tags ='LTSA_WEBAPP'
        ,merge_update_columns =  ['SNAPSHOT_WEEK_KEY', 'HEADER_ID', 'DETAIL_ID', 'MONTH_KEY', 'YEAR_KEY', 'CORPORATION_KEY',
                                    'MARKET_PRODUCT_NUMBER_KEY', 'LTSA_STATUS', 'TITLE', 'CORPORATION_CODE', 'MARKET_PRODUCT_NUMBER', 'INTERNAL_PRODUCT_NUMBER',
                                    'GROUP_ID', 'ORIGINAL_DEMAND', 'POR_ORIG_AMT', 'DEMAND', 'DETAIL_PRICE_YEAR', 'AVERAGE_SELLING_PRICE',
                                    'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']

		,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT 
    LTSA_DEMAND_WEEKLY_KEY,
    SNAPSHOT_WEEK_KEY,
    HEADER_ID,
    DETAIL_ID,
    MONTH_KEY,
    YEAR_KEY,
    CORPORATION_KEY,
    MARKET_PRODUCT_NUMBER_KEY,
    LTSA_STATUS,
    TITLE,
    CORPORATION_CODE,
    MARKET_PRODUCT_NUMBER,
    INTERNAL_PRODUCT_NUMBER,
    GROUP_ID,
    ORIGINAL_DEMAND,
    POR_ORIG_AMT,
    DEMAND,
    DETAIL_PRICE_YEAR,
    AVERAGE_SELLING_PRICE,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID,
    BIW_MD5_KEY
 FROM 
 {{ref('ETL_MART_SALES_LTSA_DEMAND_WEEKLY_FACT')}}