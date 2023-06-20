/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT
-- dbt build --full-refresh --select MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022      KALI DANDAPANI     Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{% if is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_with_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT'),'STG.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_with_select_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}


{## Delete the snapshot if already exists in the target #}
{%- set v_sql_delete_tgt -%}
 DELETE FROM {{env_var('DBT_EDW_DB')~env_var('DBT_DEP_ENV')}}.MART_GSCO.INVENTORY_WORKING_CAPITAL_WEEKLY_RPT
    WHERE SNAPSHOT_WEEK_KEY IN (SELECT 
                                SNAPSHOT_WEEK_KEY
                                FROM  {{env_var('DBT_EDL_DB')~env_var('DBT_DEP_ENV')}}.ETL_MART_GSCO.INVENTORY_WORKING_CAPITAL_WEEKLY_RPT
                                    )
{%- endset -%} 

{##to handle when table is not available#}
{%- set v_dummy_select  = " select 1 col1" -%}

{## identify whether table exists or not #}
{%- set source_relation = adapter.get_relation(
      database=env_var('DBT_EDW_DB')+env_var('DBT_DEP_ENV'),
      schema='MART_GSCO',
      identifier= 'INVENTORY_WORKING_CAPITAL_WEEKLY_RPT') -%}
{%- set table_exists =  source_relation is not none -%}


{# running delete on exist#}
{% if table_exists %}
    {%- set v_pre_sql = v_sql_delete_tgt -%}
{% else %}
    {%- set v_pre_sql = v_dummy_select -%}
{% endif %}

{%- if execute -%}
    {%- set results = run_query(v_pre_sql) -%}
{%- endif -%}

{################# Snowflake Object Configuration #################}
{{
    config(
         description = 'Building WEEKLY SNAPSHOT table FOR EFK MERGER PROJECT'
        ,transient=false   
        ,materialized='incremental'
        ,schema ='MART_GSCO'
        ,alias='INVENTORY_WORKING_CAPITAL_WEEKLY_RPT'
        ,tags =['GSCO_EFK']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

SELECT 
    SNAPSHOT_WEEK_KEY,
    ORGANIZATION_ID,
    LOT_NUMBER,
    CATEGORY,
    CATEGORY_LEVEL_2,
    CATEGORY_LEVEL_3,
    PART_NUMBER,
    SUPPLIER,
    IS_CONSIGNED,
    SUBINVENTORY_CODE,
    INVENTORY_ITEM_ID,
    ONHAND_QUANTITY,
    ORG_CODE,
    INVENTORY_AMOUNT,
    DATE_RECEIVED,
    ITEM_COST,
    CONSIGNED_AMOUNT,
    OWNED_AMOUNT,
    TRANSACTION_MONTH,
    WEEK_OF_YEAR,
    TRANSACTION_YEAR,
    QUARTER_OF_YEAR,
    MATERIAL_DESCRIPTION,
    PRIMARY_UOM_CODE,
    SUPPLIER_PART,
    PLANNER_NAME,
    LOT_STATUS,
    OWNING_PARTY,
    PURCHASING_PRICE_CONSIGNMENT,
    PURCHASING_PRICE_OWN,
    CURRENCY_CODE,
    SLED,
    ORIGINATION_DATE,
    LAST_MOVEMENT_DATE,
    PURCHASING_QUANTITY,
    CONSUMABLE,
    MPN,
    SUPPLIER_LOT_NUMBER,
    INVENTORY_PLANNING_METHOD,
    INVENTORY_TYPE,
    PURCHASING_UOM,
    CONVERSION_UOM,
    INVENTORY_AMOUNT_USD,
    FINANCIAL_VALUATION_USD,
    FAB_MODULE,
    GF_CATEGORY_CODE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    BIW_MD5_KEY
FROM 
    {{ref ('ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_WEEKLY_RPT')}} 

