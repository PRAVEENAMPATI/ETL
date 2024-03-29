/*--------------------------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_BILLING_CURRENCY_VIEW

Version     Date            Author             Description
-------     --------        -----------        ----------------------------------
1.0         02/07/2023      Sruthi Kasbe       Initial Version
----------------------------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['BILLING_KEY']-%}
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_BILLING_CURRENCY_VIEW'-%}
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
         description = 'Building ETL view BILLING_CURRENCY_VIEW for Sales Mart'
        ,materialized='view'
        ,schema ='ETL_MART_SALES'
        ,alias='BILLING_CURRENCY_VIEW'
        ,tags =['MART_SALES']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

WITH
CURRENCY AS
(
SELECT
    CURRENCY_KEY,
    FISCAL_MTH_NUM,
    FROM_CURRENCY_CODE,
    TO_CURRENCY_CODE,
    EXCHANGE_RATE
FROM
   {{ref('MART_CURRENCY_EXCHANGE')}}
),

DATE AS
(
SELECT
    CALENDAR_DATE,
    FISCAL_MONTH_KEY
FROM
    {{ref('MART_DATE')}}
)

SELECT 
    MD5(COALESCE(SHIP_TO_CUST_CD::STRING,'-1')) AS DIRECT_CUSTOMER_KEY,
    MD5(COALESCE(SHIP_TO_CUST_CD::STRING,'-1')) AS INDIRECT_CUSTOMER_KEY,
    MD5(COALESCE(END_CUST_CD::STRING,'-1')) AS END_CUSTOMER_KEY,
    MD5(COALESCE(CORP_CD::STRING,'-1')) AS DIRECT_CORPORATION_KEY,
    MD5(COALESCE(END_CORP::STRING,'-1')) AS END_CORPORATION_KEY,
    MD5(COALESCE(INTERNAL_PN::STRING,'-1')) AS INTERNAL_PART_NUMBER_KEY,
    MD5(COALESCE(MARKET_PN::STRING,'-1')) AS MARKET_PRODUCT_NUMBER_KEY,
    PROCESS_DT AS PROCESS_DATE,
    COALESCE(REPLACE(SUBSTRING(PROCESS_DT,1,10),'-')::NUMBER,'19000101') AS BILLING_DATE_KEY,
    'ON' AS SOURCE_OF_SALE,
    SALES_ORDER_LI_DL AS SALES_ORDER_LINE_ITEM_DELIVERY,
    REGION||order_num as SALES_ORDER_NUMBER,
    ORDER_LINE_ITEM_NUM AS SALES_ITEM_NUMBER,
    ORDER_DELIVERY_NUM AS SALES_DELIVERY_NUMBER,
    INTERNAL_PN AS INTERNAL_PART_NUMBER,
    MARKET_PN AS MARKET_PRODUCT_NUMBER,
    REGION,
    ORDER_TYPE,
    HANDLING_CODE,
    CASE 
    when CUST_PART_NUM='No Value'
    Then NULL
    Else CUST_PART_NUM 
    END AS CUSTOMER_PART_NUMBER,
    CUST_PA_NUM AS CUSTOMER_PA_NUMBER,
    CORP_CD AS DIRECT_CORPORATION_CODE,
    END_CORP AS END_CORPORATION_CODE,
    END_CUST_CD AS END_CUSTOMER_CODE,
    DIST_END_CUST_CD AS DIST_END_CUSTOMER_CODE,
    SHIP_TO_CUST_CD AS INDIRECT_CUSTOMER_CODE,
    SHIP_TO_CUST_CD AS DIRECT_CUSTOMER_CODE,
    CRD_DT AS REQUESTED_DATE,
    MSD_DT AS ORIGINAL_SCHEDULED_SHIP_DATE,
    RSD_DT AS SCHEDULED_SHIPPED_DATE,
    EPSD_DT AS EARLIEST_POSSIBLE_SHIP_DATE,
    PDD_DT AS PLANNED_DELIVERY_DATE,
    RDD_DT AS RESCHEDULED_DELIVERY_DATE,
    CRSD_DT AS REQUESTED_SHIP_DATE,
    ENTRY_DT AS ENTRY_DATE,
    COI AS CUSTOMER_OF_INTEREST,
    NNCO::BOOLEAN AS IS_NNCO,
    NNCO_AGREEMENT_TYPE AS NNCO_AGREEMENT_TYPE,
    ODM_END_CORP_CD AS ODM_END_CORP_CODE,
    BILL_TO_COUNTRY,
    BILL_TO_CUSTCODE AS BILL_TO_CUST_CODE,
    SUB_CORP_CD AS SUB_CORP_CODE,
    SHIP_FROM_AREA,
    CUST_PO_NUM AS CUSTOMER_PO_NUMBER,
    QUOTE_ID,
    OPERATING_UNIT,
    ITEM_STATUS,
    REVERSAL_REQUIRED_FLAG::BOOLEAN AS IS_REVERSAL_REQUIRED,
    ACTIVITY,
    BOOKING_CATEGORY,
    BILL_SOURCE_CD AS BILL_SOURCE_CODE,
    BUSINESS_CLASS_CD AS BUSINESS_CLASS_CODE,
    CREDIT_REASON_CD AS CREDIT_REASON_CODE,
    CREDIT_CODE_DESC as CREDIT_CODE_DESCRIPTION,
    PRICE_SOURCE,
    GL_POSTED_DATE,
    TRANSACTION_DATE,
    CUSTOMER_TRX_ID,
    CUSTOMER_TRX_LINE_ID,
    OBR_TRX_ID,
    TRX_LINE_NUM AS TRX_LINE_NUMBER ,
    BILLING_DOCUMENT_NUMBER,
    ORIGINAL_DOCUMENT_NUM AS ORIGINAL_DOCUMENT_NUMBER,
    TRUE_BILL_FLG::BOOLEAN AS IS_TRUE_BILL,
    GL_REV_ACCT AS GL_REVENUE_ACCOUNT,
    ACCRUAL_CATEGORY,
    JOURNAL_DESCRIPTION,
    LEDGER,
    DEBIT_ACCOUNT,
    CREDIT_ACCOUNT,
    ACCRUAL_RULESET AS ACCRUAL_RULE_SET,
    ACCRUAL_RULEID AS ACCRUAL_RULE_ID,
    ACCRUAL_COMMENTS,
    ACCRUAL_TYPE,
    ACCRUAL_FACTOR,
    ACCRUAL_TRANSACTION_DATE,
    ACCRUAL_FLAG_DSA::BOOLEAN AS IS_ACCRUAL_DSA,
    ACCRUAL_FLAG_DCD::BOOLEAN AS IS_ACCRUAL_DCD,
    ACCRUAL_FLAG_REB::BOOLEAN AS IS_ACCRUAL_REB,
    ACCRUAL_FLAG_DSC::BOOLEAN AS IS_ACCRUAL_DSC,
    ACCRUAL_FLAG_REVERSAL::BOOLEAN AS IS_ACCRUAL_REVERSAL,
    QUANTITY_OVERRIDE_FLAG::BOOLEAN AS IS_QUANTITY_OVERRIDE,
    TRANSACTION_CURRENCY AS TRANSACTION_CURRENCY_ISO_CODE,
    CONTRACT_CURRENCY,
    GROSS_QTY AS BILLING_GROSS_QUANTITY,
    NET_QTY AS BILLING_NET_QUANTITY,
    UNIT_PRICE AS UNIT_PRICE_USD,
    UNIT_PRICE_USD/EUR.EXCHANGE_RATE AS UNIT_PRICE_EUR,
    UNIT_PRICE_USD/JPY.EXCHANGE_RATE AS UNIT_PRICE_JPY,
    TRX_UNIT_PRICE AS UNIT_PRICE_TRANSACTION_CURRENCY,
    GROSS_AMT AS GROSS_AMOUNT_USD,
    TRX_GROSS_AMT AS GROSS_AMOUNT_TRANSACTION_CURRENCY,
    TRUNCATE(GROSS_AMOUNT_TRANSACTION_CURRENCY/TRX_EUR.EXCHANGE_RATE,4) AS GROSS_AMOUNT_EUR,
    TRUNCATE(GROSS_AMOUNT_TRANSACTION_CURRENCY/TRX_JPY.EXCHANGE_RATE,4) AS GROSS_AMOUNT_JPY,
    CONTRACT_CURRENCY_GROSS_AMT AS GROSS_AMOUNT_CONTRACT_CURRENCY,
    NET_AMT AS BILLING_NET_AMOUNT_USD,
    TRX_NET_AMT AS BILLING_NET_AMOUNT_TRANSACTION_CURRENCY,
    TRUNCATE(BILLING_NET_AMOUNT_TRANSACTION_CURRENCY/TRX_EUR.EXCHANGE_RATE,4) AS BILLING_NET_AMOUNT_EUR,
	TRUNCATE(BILLING_NET_AMOUNT_TRANSACTION_CURRENCY/TRX_JPY.EXCHANGE_RATE,4) AS BILLING_NET_AMOUNT_JPY,
    CONTRACT_CURRENCY_NET_AMT AS BILLING_NET_AMOUNT_CONTRACT_CURRENCY,
    ACCRUAL_AMOUNT AS ACCRUAL_AMOUNT_USD,
    TRX_ACCRUAL_AMT AS ACCRUAL_AMOUNT_TRANSACTION_CURRENCY,
    CONTRACT_CURRENCY_ACCRUAL_AMT AS ACCRUAL_AMOUNT_CONTRACT_CURRENCY,
    DW_CREATE_DT,
    STAGE1_BILL_SEQ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID

FROM 
    {{ source('STG_ONE', 'BILL_ACTIVITY_DTL') }} BILL
    -- filter date : (note: current date data are getting filtered)

LEFT JOIN DATE DT
    ON BILL.PROCESS_DT=DT.CALENDAR_DATE

LEFT JOIN CURRENCY EUR
    ON EUR.FISCAL_MTH_NUM = DT.FISCAL_MONTH_KEY
    AND EUR.TO_CURRENCY_CODE  = 'EUR'
    AND EUR.FROM_CURRENCY_CODE = 'USD'

LEFT JOIN CURRENCY JPY
    ON JPY.FISCAL_MTH_NUM = DT.FISCAL_MONTH_KEY
    AND JPY.TO_CURRENCY_CODE  = 'JPY'
    AND JPY.FROM_CURRENCY_CODE = 'USD'

LEFT JOIN CURRENCY TRX_EUR
    ON TRX_EUR.FISCAL_MTH_NUM = DT.FISCAL_MONTH_KEY
    AND TRX_EUR.TO_CURRENCY_CODE  = 'EUR'
    AND TRX_EUR.FROM_CURRENCY_CODE= BILL.TRANSACTION_CURRENCY

LEFT JOIN CURRENCY TRX_JPY
    ON TRX_JPY.FISCAL_MTH_NUM = DT.FISCAL_MONTH_KEY
    AND TRX_JPY.TO_CURRENCY_CODE  = 'JPY'
    AND TRX_JPY.FROM_CURRENCY_CODE= BILL.TRANSACTION_CURRENCY

