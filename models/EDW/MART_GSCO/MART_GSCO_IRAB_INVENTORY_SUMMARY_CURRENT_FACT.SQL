/*---------------------------------------------------------------------------
Command to run model:
--dbt run --full-refresh --select MART_GSCO_IRAB_INVENTORY_SUMMARY_CURRENT_FACT
--dbt run --select MART_GSCO_IRAB_INVENTORY_SUMMARY_CURRENT_FACT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.1         08 FEB 2023      RAMYA NAGARAJ       Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['IRAB_INVENTORY_SUMMARY_KEY' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_GSCO_IRAB_INVENTORY_SUMMARY_CURRENT_FACT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_GSCO_IRAB_INVENTORY_SUMMARY_CURRENT_FACT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_with_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_GSCO_IRAB_INVENTORY_SUMMARY_CURRENT_FACT'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_with_select_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_GSCO_IRAB_INVENTORY_SUMMARY_CURRENT_FACT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}
{%- set V_EDL_DB = var('V_EDL_DEFAULT_DB') +env_var('DBT_DEP_ENV')+'.' -%}
{%- set V_EDW_DB = var('V_EDW_DEFAULT_DB') +env_var('DBT_DEP_ENV')+'.' -%}
{%- set v_sql_update_tgt -%}
UPDATE {{V_EDW_DB}}MART_GSCO.IRAB_INVENTORY_SUMMARY_CURRENT_FACT 
SET BIW_LOGICAL_DELETE_FLAG='Y',
BIW_UPD_DTTM = current_timestamp()::TIMESTAMP_NTZ  
WHERE BIW_LOGICAL_DELETE_FLAG <> 'Y'
AND IRAB_INVENTORY_SUMMARY_KEY NOT IN (SELECT IRAB_INVENTORY_SUMMARY_KEY FROM {{V_EDL_DB}}ETL_MART_GSCO.IRAB_INVENTORY_SUMMARY_CURRENT_FACT)
{%endset%} 

{{
    config(
         description = 'Building table Inventory summary fact'
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_GSCO'
        ,alias='IRAB_INVENTORY_SUMMARY_CURRENT_FACT'
        ,unique_key= v_pk_list
        ,merge_update_columns = ['PART_ID','BANK_CODE','OWNER_CODE','TECH_GROUP_CODE','DIVISION_CODE','OPERATION_ORGANISATION_CODE','SBU_CODE',
        'PAL_CODE','PHYSICAL_REGION_CODE','PHYSICAL_COUNTRY_CODE','PHYSICAL_LOC_CODE','LOG_REGION_CODE','LOG_COUNTRY_CODE','LOG_LOCATION_CODE','OWNER_REGION_CODE',
        'OWNER_COUNTRY_CODE','OWNER_LOCATION_CODE','BULK_QUANTITY','TOTAL_LOT_QUANTITY','TOTAL_QUANTITY','UPDATE_DATE','MFG_AREA_CODE','MFG_STAGE_CODE','BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']
        ,tags ='GSCO_INVENTORY'
        ,post_hook= [v_sql_update_tgt,v_sql_upd_success_batch]
        )
}}


    SELECT 
        STG.IRAB_INVENTORY_SUMMARY_KEY,
        STG.PART_ID,
        STG.BANK_CODE,
        STG.OWNER_CODE,
        STG.TECH_GROUP_CODE,
        STG.DIVISION_CODE,
        STG.OPERATION_ORGANISATION_CODE,
        STG.SBU_CODE,
        STG.PAL_CODE,
        STG.PHYSICAL_REGION_CODE,
        STG.PHYSICAL_COUNTRY_CODE,
        STG.PHYSICAL_LOC_CODE,
        STG.LOG_REGION_CODE,
        STG.LOG_COUNTRY_CODE,
        STG.LOG_LOCATION_CODE,
        STG.OWNER_REGION_CODE,
        STG.OWNER_COUNTRY_CODE,
        STG.OWNER_LOCATION_CODE,
        STG.BULK_QUANTITY,
        STG.TOTAL_LOT_QUANTITY,
        STG.TOTAL_QUANTITY,
        STG.UPDATE_DATE,
        STG.MFG_AREA_CODE,
        STG.MFG_STAGE_CODE,
        STG.BIW_INS_DTTM ,
        STG.BIW_UPD_DTTM ,
        STG.BIW_BATCH_ID,
        STG.BIW_MD5_KEY,
        STG.BIW_LOGICAL_DELETE_FLAG
    FROM 
    {{ref('ETL_MART_GSCO_IRAB_INVENTORY_SUMMARY_CURRENT_FACT')}} STG
    {% if is_incremental() %}
    LEFT JOIN {{ this }} TGT
    on STG.IRAB_INVENTORY_SUMMARY_KEY= TGT.IRAB_INVENTORY_SUMMARY_KEY
    WHERE TGT.BIW_MD5_KEY<>STG.BIW_MD5_KEY 
      OR TGT.BIW_MD5_KEY IS NULL
    {% endif %}
