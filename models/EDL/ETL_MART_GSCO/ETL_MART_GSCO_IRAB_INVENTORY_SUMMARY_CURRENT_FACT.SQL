/*---------------------------------------------------------------------------
Command to run model:
-- dbt build --full-refresh --select ETL_MART_IRAB_INVENTORY_SUMMARY_CURRENT_FACT
-- dbt build --select ETL_MART_IRAB_INVENTORY_SUMMARY_CURRENT_FACT

https://confluence.onsemi.com/display/EABIDW/IRAB%3A+Inventory+Recording+and+Balancing
Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         02 FEB 2023     RAMYA NAGARAJ        Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['IRAB_INVENTORY_SUMMARY_KEY']-%}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_GSCO_IRAB_INVENTORY_SUMMARY_CURRENT_FACT'-%}
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
         description = 'Building table IRAB_INVENTORY_SUMMARY_CURRENT_FACT'
        ,transient=true
        ,materialized='view'
        ,schema ='ETL_MART_GSCO'
        ,alias='IRAB_INVENTORY_SUMMARY_CURRENT_FACT'
		,unique_key= v_pk_list
        ,tags =['GSCO_INVENTORY']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

 SELECT 
        MD5(OBJECT_CONSTRUCT ('COL1',PART_ID::STRING
                            , 'COL2',BANK_CD::STRING
                            , 'COL3',OWNER_CD::STRING)::STRING ) AS IRAB_INVENTORY_SUMMARY_KEY,
        PART_ID,
        BANK_CD AS BANK_CODE,
        OWNER_CD AS OWNER_CODE,
        TECH_GROUP_CD AS TECH_GROUP_CODE,
        DIVISION_CD AS DIVISION_CODE,
        OPERATION_ORG_CD AS OPERATION_ORGANISATION_CODE,
        SBU_CD AS SBU_CODE,
        PAL_CD AS PAL_CODE,
        PHYS_REGION_CD AS PHYSICAL_REGION_CODE,
        PHYS_COUNTRY_CD AS PHYSICAL_COUNTRY_CODE,
        PHYS_LOC_CD AS PHYSICAL_LOC_CODE,
        LOG_REGION_CD AS LOG_REGION_CODE,
        LOG_COUNTRY_CD AS LOG_COUNTRY_CODE,
        LOG_LOC_CD AS LOG_LOCATION_CODE,
        OWNER_REGION_CD AS OWNER_REGION_CODE,
        OWNER_COUNTRY_CD AS OWNER_COUNTRY_CODE,
        OWNER_LOC_CD AS OWNER_LOCATION_CODE,
        BULK_QTY AS BULK_QUANTITY,
        TOTAL_LOT_QTY AS TOTAL_LOT_QUANTITY,
        TOTAL_QTY AS TOTAL_QUANTITY,
        UPDATE_DT AS UPDATE_DATE,
        MFG_AREA_CD AS MFG_AREA_CODE,
        MFG_STAGE_CD AS MFG_STAGE_CODE,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
        '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
        {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID,
        'N' AS BIW_LOGICAL_DELETE_FLAG,
        md5(object_construct ('col1',PART_ID::string, 'col2',BANK_CD::string, 'col3',OWNER_CD::string,
        'col4',DIVISION_CD::string, 'col5',OPERATION_ORG_CD::string, 'col6',SBU_CD::string,
        'col7',PAL_CD::string, 'col8',PHYS_REGION_CD::string, 'col9',PHYS_COUNTRY_CD::string,
        'col10',PHYS_LOC_CD::string, 'col11',LOG_REGION_CD::string, 'col12',LOG_COUNTRY_CD::string,
        'col13',LOG_LOC_CD::string, 'col14',OWNER_REGION_CD::string, 'col15',OWNER_COUNTRY_CD::string,
        'col16',OWNER_LOC_CD::string, 'col17',BULK_QTY::string, 'col18',TOTAL_LOT_QTY::string,
        'col19',TOTAL_QTY::string, 'col20',UPDATE_DT::string, 'col21',MFG_AREA_CD::string, 'col22',MFG_STAGE_CD::string)::string )::BINARY as BIW_MD5_KEY
    FROM {{source('STG_MAINFRAME','MFG_INVENTORY_SUM')}}
    WHERE BIW_BATCH_ID = (select MAX(BIW_BATCH_ID) FROM {{source('STG_MAINFRAME','MFG_INVENTORY_SUM')}})