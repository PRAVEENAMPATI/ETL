/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_MONTHLY_RPT
-- dbt build --full-refresh --select ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_MONTHLY_RPT+

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         17-NOV-2022      KALI DANDAPANI     Initial Version
---------------------------------------------------------------------------*/

{################# EDW Job Template Variables #################}
{% if not is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( this ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( this ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_md5_column_list =  edw_get_md5_column_list( this ,v_pk_list|list+ v_house_keeping_column|list ) -%}
--DBT Variable
--SELECT {{v_all_column_list}}  
--SELECT {{v_update_column_list}}  
--SELECT {{v_md5_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_GSCO_INVENTORY_WORKING_CAPITAL_MONTHLY_RPT'-%}
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
         description = 'Building ETL table INVENTORY_WORKING_CAPITAL_MONTHLY_RPT for EFK MERGER PROJECT'
        ,transient=true   
        ,materialized='table'
        ,schema ='ETL_MART_GSCO'
        ,alias='INVENTORY_WORKING_CAPITAL_MONTHLY_RPT'
        ,tags =['GSCO_EFK']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH FISCAL_MONTH AS 
(
    SELECT 
        DISTINCT FISCAL_MONTH_KEY
    FROM 
    {{source('MART','DATE')}}
    WHERE 
        CALENDAR_DATE = (CURRENT_TIMESTAMP() - INTERVAL '7 HOUR')::DATE
        or CALENDAR_DATE = (CURRENT_TIMESTAMP() )::DATE
)

,XXON_INV_WORKING_CAPITAL_V  AS ( 
SELECT 
    ORGANIZATION_ID AS ORGANIZATION_ID,
    LOT_NUMBER AS LOT_NUMBER,
    CATEGORY AS CATEGORY,
    CATEGORY_LEVEL_2 AS CATEGORY_LEVEL_2,
    CATEGORY_LEVEL_3 AS CATEGORY_LEVEL_3,
    PART_NUMBER AS PART_NUMBER,
    SUPPLIER AS SUPPLIER,
    CONSIGNED_FLAG AS IS_CONSIGNED,
    SUBINVENTORY_CODE AS SUBINVENTORY_CODE,
    INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID,
    ONHAND_QUANTITY AS ONHAND_QUANTITY,
    ORG_CODE AS ORG_CODE,
    INVENTORY_AMOUNT AS INVENTORY_AMOUNT,
    DATE_RECEIVED AS DATE_RECEIVED,
    ITEM_COST AS ITEM_COST,
    CONSIGNED_AMOUNT AS CONSIGNED_AMOUNT,
    OWNED_AMOUNT AS OWNED_AMOUNT,
    TRX_MONTH AS TRANSACTION_MONTH,
    WEEK_OF_YEAR AS WEEK_OF_YEAR,
    TRX_YEAR AS TRANSACTION_YEAR,
    QTR_OF_YEAR AS QUARTER_OF_YEAR,
    MATERIAL_DESCRIPTION AS MATERIAL_DESCRIPTION,
    PRIMARY_UOM_CODE AS PRIMARY_UOM_CODE,
    SUPPLIER_PART AS SUPPLIER_PART,
    PLANNER_NAME AS PLANNER_NAME,
    LOT_STATUS AS LOT_STATUS,
    OWNING_PARTY AS OWNING_PARTY,
    PURCHASING_PRICE_CONSIGNMENT AS PURCHASING_PRICE_CONSIGNMENT,
    PURCHASING_PRICE_OWN AS PURCHASING_PRICE_OWN,
    CURRENCY_CODE AS CURRENCY_CODE,
    SLED AS SLED,
    ORIGINATION_DATE AS ORIGINATION_DATE,
    LAST_MOVEMENT_DATE AS LAST_MOVEMENT_DATE,
    PURCHASING_QTY AS PURCHASING_QUANTITY,
    CONSUMABLE AS CONSUMABLE,
    MPN AS MPN,
    SUPPLIER_LOT_NUMBER AS SUPPLIER_LOT_NUMBER,
    INVENTORY_PLANNING_METHOD AS INVENTORY_PLANNING_METHOD,
    INVENTORY_TYPE AS INVENTORY_TYPE,
    PURCHASING_UOM AS PURCHASING_UOM,
    CONVERSION_UOM AS CONVERSION_UOM,
    INVENTORY_AMOUNT_USD AS INVENTORY_AMOUNT_USD,
    FINANCIAL_VALUATION_USD AS FINANCIAL_VALUATION_USD,
    FAB_MODULE AS FAB_MODULE,
    GF_CATEGORY_CODE AS GF_CATEGORY_CODE
FROM     
{{source ('STG_EBS_APPS','XXON_INV_WORKING_CAPITAL_V')}}      
WHERE BIW_BATCH_ID = (SELECT MAX(BIW_BATCH_ID) FROM {{source ('STG_EBS_APPS','XXON_INV_WORKING_CAPITAL_V')}} ) 
)

SELECT      
    FSC_MN.FISCAL_MONTH_KEY AS SNAPSHOT_MONTH_KEY,
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
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM,      
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM,    
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID,
    md5(object_construct ('col1',SNAPSHOT_MONTH_KEY::string, 'col2',ORGANIZATION_ID::string,
    'col3',LOT_NUMBER::string, 'col4',CATEGORY::string, 'col5',CATEGORY_LEVEL_2::string, 'col6',CATEGORY_LEVEL_3::string,
    'col7',PART_NUMBER::string, 'col8',SUPPLIER::string, 'col9',IS_CONSIGNED::string, 'col10',SUBINVENTORY_CODE::string,
    'col11',INVENTORY_ITEM_ID::string, 'col12',ONHAND_QUANTITY::string, 'col13',ORG_CODE::string, 'col14',INVENTORY_AMOUNT::string,
    'col15',DATE_RECEIVED::string, 'col16',ITEM_COST::string, 'col17',CONSIGNED_AMOUNT::string, 'col18',OWNED_AMOUNT::string,
    'col19',TRANSACTION_MONTH::string, 'col20',WEEK_OF_YEAR::string, 'col21',TRANSACTION_YEAR::string, 'col22',QUARTER_OF_YEAR::string,
    'col23',MATERIAL_DESCRIPTION::string, 'col24',PRIMARY_UOM_CODE::string, 'col25',SUPPLIER_PART::string, 'col26',PLANNER_NAME::string,
    'col27',LOT_STATUS::string, 'col28',OWNING_PARTY::string, 'col29',PURCHASING_PRICE_CONSIGNMENT::string,
    'col30',PURCHASING_PRICE_OWN::string, 'col31',CURRENCY_CODE::string, 'col32',SLED::string, 'col33',ORIGINATION_DATE::string,
    'col34',LAST_MOVEMENT_DATE::string, 'col35',PURCHASING_QUANTITY::string, 'col36',CONSUMABLE::string, 'col37',MPN::string,
    'col38',SUPPLIER_LOT_NUMBER::string, 'col39',INVENTORY_PLANNING_METHOD::string, 'col40',INVENTORY_TYPE::string,
    'col41',PURCHASING_UOM::string, 'col42',CONVERSION_UOM::string, 'col43',INVENTORY_AMOUNT_USD::string,
    'col44',FINANCIAL_VALUATION_USD::string, 'col45',FAB_MODULE::string, 'col46',GF_CATEGORY_CODE::string)::string )::BINARY as BIW_MD5_KEY
FROM      XXON_INV_WORKING_CAPITAL_V  STG  
CROSS JOIN FISCAL_MONTH FSC_MN