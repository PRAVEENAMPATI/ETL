/*---------------------------------------------------------------------------
Command to run model:
--dbt build --select ETL_MART_SALES_PRODUCT_CLASSIFICATION_FACT
--dbt build --full-refresh --select ETL_MART_SALES_PRODUCT_CLASSIFICATION_FACT+
--dbt build --full-refresh --select ETL_MART_SALES_PRODUCT_CLASSIFICATION_FACT+  --vars 'is_backfill: True'
Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         19 Dec 2022      VINAYAKA CN        Initial Version
2.0         01 Mar 2023      VINAY SUBRAMANIAN  Replaced billing and POS model with ECC
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_PRODUCT_CLASSIFICATION_FACT'-%}
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
         description = 'Building table PRODUCT_CLASSIFICATION_FACT for MART_SALES'
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES'
        ,alias='PRODUCT_CLASSIFICATION_FACT'
		,unique_key= v_pk_list
        ,tags =['MART_SALES']
		,post_hook= [v_sql_upd_success_batch]	
        )
}}

--STEP 1 GET ALL THE PARTS & PRODUCTS
WITH DIM_PRODUCTS AS 
(
    SELECT
       PART_ID AS PRODUCT_ID,
       MARKET_PN AS MARKET_PRODUCT_NUMBER ,
       PART_CLASS_CODE ,
       IS_SPEC_CNTRL_BOM AS IS_CUST_SPEC
    FROM  {{ref('SALES_PRODUCT_DIM')}}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY PART_ID ORDER BY BIW_UPD_DTTM DESC)=1) 
)

--STEP 2 GET THE COMPETITOR RAW DATA
,EWPC_ORDERABLE_PART AS
(
    SELECT 
        ORDERABLE_PART ,
        BASE_PART AS WEB_PART_NAME
    FROM {{source ('STG_WEBPRD_PUBLIC','EWPC_ORDERABLE_PART')}}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY ORDERABLE_PART ORDER BY BIW_UPD_DTTM DESC)=1)
)

,EWPC_XREF_M1 AS (
    SELECT 
        ORDERABLE_PART ,
        COMPETITOR_PART ,
        REPLACEMENT_TYPE
    FROM 
        {{source('STG_WEBPRD_PUBLIC','EWPC_XREF_M1')}}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY ORDERABLE_PART,COMPETITOR_PART,REPLACEMENT_TYPE ORDER BY BIW_UPD_DTTM DESC)=1)
) 

,EWPC_XREF_M1_PART AS (
    SELECT 
        COMPETITOR_PART ,
        COMPETITOR
    FROM 
        {{source('STG_WEBPRD_PUBLIC','EWPC_XREF_M1_PART')}}
    QUALIFY( ROW_NUMBER() OVER (PARTITION BY COMPETITOR_PART ORDER BY BIW_UPD_DTTM DESC)=1)
) 

-- STEP 3 : IDENITIFY THE WEEK WHICH NEED SUMMARY DATA
,RUN_WEEK AS 
(
    SELECT 
       DISTINCT FISCAL_WEEK_PRIOR AS FISCAL_WEEK_KEY
    FROM 
    {{ref('MART_DATE')}}
    WHERE 
        CALENDAR_DATE>= TRUNC(CURRENT_DATE - INTERVAL '5 YEAR','YEAR')   
        AND CALENDAR_DATE >= '{{V_LWM}}'::DATE-INTERVAL '1 year'
		AND CALENDAR_DATE <= '{{V_HWM}}'::DATE
)

,FISCAL_WEEK_VW AS 
(
    SELECT 
        CALENDAR_DATE,
        MAX(FISCAL_WEEK_KEY) OVER (ORDER BY 1)  AS FISCAL_WEEK_KEY
    FROM 
    {{ref('MART_DATE')}}
    WHERE 
        FISCAL_WEEK_KEY IN (SELECT FISCAL_WEEK_KEY FROM RUN_WEEK)
),

--STEP 4: SUMMARY OF COMPETITOR
COMPETITOR_SUMMARY AS (
    SELECT 
        PRDCT.PRODUCT_ID,
        FIS_WK.FISCAL_WEEK_KEY ,
        PRDCT.PART_CLASS_CODE,
        PRDCT.IS_CUST_SPEC,
        WEB.WEB_PART_NAME,
        COUNT(DISTINCT 
                CASE
                    WHEN CMPTR_PRT.REPLACEMENT_TYPE = 'Similar' 
                        THEN CMPTR_NM.COMPETITOR
                END
            )    AS COMPETITOR_REPLACEMENT_SIMILAR_COUNT ,
        COUNT(DISTINCT 
                CASE
                    WHEN CMPTR_PRT.REPLACEMENT_TYPE = 'Direct' 
                        THEN CMPTR_NM.COMPETITOR
                END
            )    AS COMPETITOR_REPLACEMENT_DIRECT_COUNT ,
        COUNT(DISTINCT 
                CASE
                    WHEN CMPTR_PRT.REPLACEMENT_TYPE IN ('Direct', 'Similar') 
                        THEN CMPTR_NM.COMPETITOR
                END
            )  AS COMPETITOR_REPLACEMENT_DIRECT_SIMILAR_COUNT
    FROM 
        DIM_PRODUCTS PRDCT 
    LEFT JOIN EWPC_XREF_M1 CMPTR_PRT
        ON PRDCT.PRODUCT_ID = CMPTR_PRT.ORDERABLE_PART
    LEFT JOIN EWPC_XREF_M1_PART CMPTR_NM
        ON CMPTR_PRT.COMPETITOR_PART =  CMPTR_NM.COMPETITOR_PART
    LEFT JOIN EWPC_ORDERABLE_PART WEB 
        ON PRDCT.PRODUCT_ID = WEB.ORDERABLE_PART
    CROSS JOIN FISCAL_WEEK_VW FIS_WK
    GROUP BY 
        PRDCT.PRODUCT_ID ,
        FIS_WK.FISCAL_WEEK_KEY ,
        PRDCT.PART_CLASS_CODE,
        PRDCT.IS_CUST_SPEC,
        WEB.WEB_PART_NAME
) 

--STEP 5 : GET RAW DATA  SALES REGION , BILLING AND POS
,SALES_REGION  AS 
(
    SELECT 
        CUSTOMER_CODE,
        CASE
            WHEN SALES_REGION_CODE = 'GCHN'
                THEN 'AP'
            ELSE SALES_REGION_CODE
        END AS SALES_REGION_CODE
    FROM {{ref ('SALES_CUSTOMER_DIM')}}
),

-- STEP 6: SUMMARY THE ECC DATA
ECC_SUMMARY AS (
    SELECT 
        ECC.MARKET_PRODUCT_NUMBER ,
        FIS_WK_VW.FISCAL_WEEK_KEY AS FISCAL_WEEK_KEY ,

        SUM(NVL(ECC.RESALE_QUANTITY,0)) AS TOTAL_RESALE_QUANTITY,
        SUM(CASE WHEN ECC.SOURCE_OF_DATA ='POS' THEN NVL(ECC.RESALE_QUANTITY,0) END) AS TOTAL_RESALE_QUANTITY_POS,
        SUM(CASE WHEN ECC.SOURCE_OF_DATA ='BILLING' THEN NVL(ECC.RESALE_QUANTITY,0) END) AS TOTAL_RESALE_QUANTITY_BILLING,

        SUM(NVL(ECC.COST_USD,0)) AS TOTAL_COST_USD,
        SUM(NVL(ECC.RESALE_NET_USD,0)) AS TOTAL_RESALE_NET_USD,

        COUNT( DISTINCT ECC.DISTRIBUTOR_SHIP_TO_CUSTOMER_IDENTIFIER) AS DISTRIBUTOR_SHIP_TO_CUSTOMER_COUNT,
        COUNT( DISTINCT ECC.INDIRECT_CUSTOMER_CODE) AS  INDIRECT_CUSTOMER_COUNT,
        COUNT( DISTINCT ECC.END_CUSTOMER_CODE) AS END_CUSTOMER_COUNT,

		COUNT(DISTINCT DIR_REGION.SALES_REGION_CODE	) AS DIRECT_REGION_COUNT,
        COUNT(DISTINCT END_REGION.SALES_REGION_CODE	) AS END_REGION_COUNT,        

        COUNT( DISTINCT ECC.END_CORPORATION_CODE) AS END_CORPORATION_COUNT,
        COUNT( DISTINCT CASE WHEN ECC.SOURCE_OF_DATA ='POS' THEN  ECC.END_CORPORATION_CODE END) AS END_CORPORATION_POS_COUNT,
        COUNT( DISTINCT CASE WHEN ECC.SOURCE_OF_DATA ='BILLING' THEN ECC.END_CORPORATION_CODE END) AS END_CORPORATION_BILLING_COUNT
FROM
        {{ref('MART_SALES_END_CUSTOMER_CONSUMPTION_WEEKLY_FACT')}} ECC
        LEFT JOIN SALES_REGION DIR_REGION 
        ON ECC.INDIRECT_CUSTOMER_CODE = DIR_REGION.CUSTOMER_CODE
        LEFT JOIN  SALES_REGION END_REGION 
        ON ECC.END_CUSTOMER_CODE = END_REGION.CUSTOMER_CODE
        INNER JOIN FISCAL_WEEK_VW FIS_WK_VW
        ON ECC.FISCAL_WEEK_KEY = FIS_WK_VW.FISCAL_WEEK_KEY    
    GROUP BY 
        MARKET_PRODUCT_NUMBER,
        FIS_WK_VW.FISCAL_WEEK_KEY
) 
      
-- STEP 7: FINAL SQL COMBINE ECC SUMMARY AND PRODUCT COMPETITOR SUMMARY DATA 
SELECT 
    MD5(OBJECT_CONSTRUCT('COL1', PRDCT.PRODUCT_ID::STRING
                        ,'COL2', PRDCT.FISCAL_WEEK_KEY::STRING
                        )::STRING        
        )AS MARKET_PRODUCT_NUMBER_KEY
    ,PRDCT.PRODUCT_ID AS PRODUCT_ID
    ,PRDCT.FISCAL_WEEK_KEY  
    ,PRDCT.WEB_PART_NAME
    ,ECC_SUM.TOTAL_RESALE_QUANTITY
    ,ECC_SUM.TOTAL_RESALE_QUANTITY_POS
    ,ECC_SUM.TOTAL_RESALE_QUANTITY_BILLING
    ,ECC_SUM.TOTAL_COST_USD
    ,ECC_SUM.TOTAL_RESALE_NET_USD
    ,ECC_SUM.DISTRIBUTOR_SHIP_TO_CUSTOMER_COUNT
    ,ECC_SUM.INDIRECT_CUSTOMER_COUNT
    ,ECC_SUM.END_CUSTOMER_COUNT
    ,ECC_SUM.DIRECT_REGION_COUNT
    ,ECC_SUM.END_REGION_COUNT
    ,ECC_SUM.END_CORPORATION_COUNT
    ,ECC_SUM.END_CORPORATION_POS_COUNT
    ,ECC_SUM.END_CORPORATION_BILLING_COUNT
    ,PRDCT.COMPETITOR_REPLACEMENT_SIMILAR_COUNT
    ,PRDCT.COMPETITOR_REPLACEMENT_DIRECT_COUNT
    ,PRDCT.COMPETITOR_REPLACEMENT_DIRECT_SIMILAR_COUNT
    ,CASE 
        WHEN PRDCT.PART_CLASS_CODE IN ('NP', 'KT')    
            THEN 'Non Product'     
        WHEN PRDCT.IS_CUST_SPEC=1                        
            THEN 'Custom'
        WHEN ECC_SUM.TOTAL_RESALE_QUANTITY > 50000000 
                AND ECC_SUM.END_CORPORATION_COUNT > 500 
            THEN 'Super Commodity'
        WHEN ECC_SUM.TOTAL_RESALE_QUANTITY > 10000000 
                AND ECC_SUM.END_CORPORATION_COUNT > 100 
            THEN 'Commodity 1'
        WHEN ECC_SUM.TOTAL_RESALE_QUANTITY > 5000000  
                AND ECC_SUM.END_CORPORATION_COUNT > 20 
                AND PRDCT.COMPETITOR_REPLACEMENT_DIRECT_COUNT >= 1 
            THEN 'Commodity 2'
        WHEN ECC_SUM.END_CORPORATION_COUNT >= 30    
            THEN 'Firm - High Cust'
        WHEN ECC_SUM.END_CORPORATION_COUNT <= 4     
            THEN 'Firm - Low Cust'
        WHEN ECC_SUM.TOTAL_RESALE_QUANTITY = 0 
                OR ECC_SUM.TOTAL_RESALE_QUANTITY IS NULL 
            THEN 'No 6Mo Revenue'
        ELSE 'Firm'
    END    AS PRODUCT_CLASSIFICATION ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}::NUMBER AS BIW_BATCH_ID,
    md5(object_construct ('col1',PRODUCT_ID::string, 'col2',PRDCT.FISCAL_WEEK_KEY::string,
    'col3',WEB_PART_NAME::string, 'col4',TOTAL_RESALE_QUANTITY::string, 'col5',TOTAL_RESALE_QUANTITY_POS::string,
    'col6',TOTAL_RESALE_QUANTITY_BILLING::string, 'col7',TOTAL_COST_USD::string, 'col8',TOTAL_RESALE_NET_USD::string, 'col9',DISTRIBUTOR_SHIP_TO_CUSTOMER_COUNT::string,
    'col10',INDIRECT_CUSTOMER_COUNT::string, 'col11',END_CUSTOMER_COUNT::string, 'col12',DIRECT_REGION_COUNT::string, 'col13',END_REGION_COUNT::string,
    'col14',END_CORPORATION_COUNT::string, 'col15',END_CORPORATION_POS_COUNT::string, 'col16',END_CORPORATION_BILLING_COUNT::string,
    'col17',COMPETITOR_REPLACEMENT_SIMILAR_COUNT::string, 'col18',COMPETITOR_REPLACEMENT_DIRECT_COUNT::string, 'col19',COMPETITOR_REPLACEMENT_DIRECT_SIMILAR_COUNT::string,
    'col20',PRODUCT_CLASSIFICATION::string)::string )::BINARY as BIW_MD5_KEY  
    FROM 
    COMPETITOR_SUMMARY PRDCT
    LEFT JOIN ECC_SUMMARY ECC_SUM
        ON PRDCT.PRODUCT_ID= ECC_SUM.MARKET_PRODUCT_NUMBER
        AND PRDCT.FISCAL_WEEK_KEY = ECC_SUM.FISCAL_WEEK_KEY