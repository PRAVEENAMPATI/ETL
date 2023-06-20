/*---------------------------------------------------------------------------
Command to run model:
--dbt build --select ETL_MART_SALES_LTSA_DEMAND_WEEKLY_FACT

Version     Date            Author               Description
-------     --------        -----------          ----------------------------------
1.0         03/27/2023      Vinay Subramanian    Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['LTSA_DEMAND_WEEKLY_KEY']-%}
{% if  is_incremental() %}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_LTSA_DEMAND_WEEKLY_FACT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building ETL table FUNNEL_FACT for sales mart'
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES'
        ,alias='LTSA_DEMAND_WEEKLY_FACT'
        ,unique_key= v_pk_list
        ,tags ='LTSA_WEBAPP'
        ,post_hook= [v_sql_upd_success_batch]
        )
}}


WITH 
LTSA_HDR AS (    
SELECT  
    ID
    , TITLE
    , CORP_CODE as CORPORATION_CODE
    , LTSA_STATUS
FROM {{source("STG_SAB_LTSA","CBA_LTSA_HDR")}}
WHERE  UPPER(LTSA_STATUS) ='ACTIVE'
AND ID NOT IN (SELECT ID 
                FROM 
                {{ref('SALES_LTSA_HDR_AUDIT')}} 
                WHERE AUDIT_OPERATION ='DELETE')
QUALIFY( ROW_NUMBER() OVER (PARTITION BY ID ORDER BY BIW_UPD_DTTM DESC)=1)
            )

,LTSA_DETAIL AS (
 SELECT  
    ID
    , HDR_ID
    , REC_TYPE
    , MPN as MARKET_PRODUCT_NUMBER
    , GROUP_ID
    , IPN AS INTERNAL_PRODUCT_NUMBER
    
FROM {{source("STG_SAB_LTSA","CBA_LTSA_DETAIL")}}
WHERE UPPER(REC_TYPE) ='POR'
AND ID NOT IN (SELECT ID 
                FROM 
                {{ref('SALES_LTSA_DETAIL_AUDIT')}} 
                WHERE AUDIT_OPERATION ='DELETE')
QUALIFY( ROW_NUMBER() OVER (PARTITION BY ID ORDER BY BIW_UPD_DTTM DESC)=1)
            )
,LTSA_DEMAND AS (
SELECT  
    ID
    , DETAIL_ID 
    ,MONTH
    , SUBSTRING(REPLACE(MONTH::STRING ,'-'),1,6) MONTH_KEY
    , ORIGINAL_DEMAND
    ,DEMAND
FROM {{source("STG_SAB_LTSA","CBA_LTSA_DEMAND")}} 
WHERE ID NOT IN (SELECT ID 
                FROM 
                {{ref('SALES_LTSA_DEMAND_AUDIT')}} 
                WHERE AUDIT_OPERATION ='DELETE')
QUALIFY( ROW_NUMBER() OVER (PARTITION BY ID ORDER BY BIW_UPD_DTTM DESC)=1)
            )
,LTSA_DETAIL_PRICE AS (
SELECT  
    ID
    , DETAIL_ID
    , YEAR AS YEAR_KEY
    , CURRENT_ASP_PRICE
    
FROM {{source("STG_SAB_LTSA","CBA_LTSA_DETAIL_PRICE")}}
WHERE ID NOT IN (SELECT ID 
                FROM 
                {{ref('SALES_LTSA_DETAIL_PRICE_AUDIT')}} 
                WHERE AUDIT_OPERATION ='DELETE')
QUALIFY( ROW_NUMBER() OVER (PARTITION BY ID ORDER BY BIW_UPD_DTTM DESC)=1)
            )

,FISCAL_WEEK AS 
(
    SELECT 
        DISTINCT FISCAL_WEEK_KEY
    FROM 
    {{ref('MART_DATE') }}
    WHERE 
        CALENDAR_DATE = (CURRENT_TIMESTAMP() - INTERVAL '7 HOUR')::DATE
        or CALENDAR_DATE = (CURRENT_TIMESTAMP() -  INTERVAL '30 MINUTE')::DATE
)

SELECT 
    MD5(OBJECT_CONSTRUCT ('COL1',FSC_WK.FISCAL_WEEK_KEY::STRING
                        , 'COL2',HDR.ID::STRING
                        , 'COL3',DETL.ID::STRING
                        , 'COL4',DMD.MONTH_KEY::STRING
                        )::STRING) AS LTSA_DEMAND_WEEKLY_KEY,
    FSC_WK.FISCAL_WEEK_KEY AS SNAPSHOT_WEEK_KEY,
    HDR.ID AS HEADER_ID,
    DETL.ID AS DETAIL_ID,
    DMD.MONTH_KEY,
    
    TO_NUMBER(TO_CHAR(DMD.MONTH,'YYYY'),'9999') AS YEAR_KEY,
    MD5(HDR.CORPORATION_CODE) AS CORPORATION_KEY,
    MD5(DETL.MARKET_PRODUCT_NUMBER) AS MARKET_PRODUCT_NUMBER_KEY,
    
    HDR.LTSA_STATUS,
    HDR.TITLE,
    HDR.CORPORATION_CODE,
    DETL.MARKET_PRODUCT_NUMBER,
    DETL.INTERNAL_PRODUCT_NUMBER,
    DETL.GROUP_ID,
    DMD.ORIGINAL_DEMAND,
    DMD.ORIGINAL_DEMAND * COALESCE(PRC.CURRENT_ASP_PRICE,0) AS POR_ORIG_AMT,
    DMD.DEMAND,
    PRC.YEAR_KEY AS DETAIL_PRICE_YEAR,
    PRC.CURRENT_ASP_PRICE AS AVERAGE_SELLING_PRICE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ AS BIW_INS_DTTM,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ AS BIW_UPD_DTTM,
    {{V_BIW_BATCH_ID}}::NUMBER AS BIW_BATCH_ID,
    md5(object_construct ('col1',SNAPSHOT_WEEK_KEY::string, 'col2',MARKET_PRODUCT_NUMBER_KEY::string,
    'col3',CORPORATION_KEY::string, 'col4',MONTH_KEY::string, 'col5',YEAR_KEY::string, 'col6',HEADER_ID::string,
    'col7',DMD.DETAIL_ID::string, 'col8',LTSA_STATUS::string, 'col9',CORPORATION_CODE::string, 'col10',TITLE::string,
    'col11',MARKET_PRODUCT_NUMBER::string, 'col12',INTERNAL_PRODUCT_NUMBER::string, 'col13',GROUP_ID::string,
    'col14',ORIGINAL_DEMAND::string, 'col15',POR_ORIG_AMT::string, 'col16',DEMAND::string, 'col17',DETAIL_PRICE_YEAR::string,
    'col18',AVERAGE_SELLING_PRICE::string)::string )::BINARY as BIW_MD5_KEY 
FROM LTSA_HDR HDR
INNER JOIN LTSA_DETAIL DETL
    ON DETL.HDR_ID= HDR.ID
LEFT OUTER JOIN LTSA_DEMAND DMD 
    ON DMD.DETAIL_ID = DETL.ID
LEFT OUTER JOIN LTSA_DETAIL_PRICE PRC 
    ON PRC.DETAIL_ID = DETL.ID 
    AND TO_NUMBER(TO_CHAR(DMD.MONTH,'YYYY'),'9999') = PRC.YEAR_KEY
CROSS JOIN FISCAL_WEEK FSC_WK


