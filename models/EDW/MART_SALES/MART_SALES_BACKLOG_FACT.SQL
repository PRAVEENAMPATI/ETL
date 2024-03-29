/*---------------------------------------------------------------------------
Command to run model:
-- dbt run --select MART_SALES_BACKLOG_FACT
-- dbt run --full-refresh --select MART_SALES_BACKLOG_FACT
-- dbt run --select MART_SALES_BACKLOG_FACT --vars 'is_one_time_load: True'
Version     Date            Author           Description
-------     --------        -----------      ----------------------------------
1.0         10-SEP-2022      Kali Dandapani    Initial Version
2.0         10-OCT-2022      Kali Dandapani    Added FCS and QUANTENNA one time load
3.0         09-DEC-2022      Sruthi Kasbe      Adding new columns and Rearranging the columns
4.0         19-APR-2023      Mattuvarkulali M   Added new column ODM_END_CORP_CD
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['BACKLOG_KEY','SNAPSHOT_DATE_KEY']-%}
{% if not is_incremental() %}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_md5_column_list =  edw_get_md5_column_list( ref('ETL_MART_SALES_BACKLOG_FACT') ,v_pk_list|list+ v_house_keeping_column|list ) -%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_BACKLOG_FACT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_BACKLOG_FACT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_BACKLOG_FACT'-%}
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
         description = 'Building table BACKLOG_FACT for MART_SALES'
        ,transient=true
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='BACKLOG_FACT'
		,unique_key= v_pk_list
        ,tags =['MART_SALES']
        ,cluster_by=['PROCESS_DATE']
        ,merge_update_columns = ['DIRECT_CUSTOMER_KEY', 'INDIRECT_CUSTOMER_KEY', 'END_CUSTOMER_KEY', 'DIRECT_CORPORATION_KEY',
                'END_CORPORATION_KEY', 'MARKET_PRODUCT_NUMBER_KEY', 'INTERNAL_PART_NUMBER_KEY', 'PROCESS_DATE', 'DIRECT_CUSTOMER_CODE',
                'INDIRECT_CUSTOMER_CODE', 'END_CUSTOMER_CODE', 'DIRECT_CORPORATION_CODE', 'END_CORPORATION_CODE', 'MARKET_PRODUCT_NUMBER',
                'INTERNAL_PART_NUMBER', 'CUSTOMER_PART_NUMBER', 'SOURCE_OF_SALE', 'SALES_ORDER_LINE_ITEM_DELIVERY', 'REGION',
                'LEGACY_SALES_ORDER_NUMBER', 'LEGACY_SALES_ITEM_NUMBER', 'LEGACY_SALES_DELIVERY_NUMBER', 'SALES_QUOTE_NUM', 'ORDER_TYPE_CODE',
                'ACCRUAL_AMOUNT_USD', 'ACCRUAL_CATEGORY', 'ACCRUAL_COMMENTS', 'ACCRUAL_FACTOR', 'ACCRUAL_RULE_ID', 'ACCRUAL_RULE_SET',
                'ACCRUAL_TRANSACTION_DATE', 'ACCRUAL_TYPE', 'ACTIVITY', 'BACKLOG_GROSS_USD', 'BACKLOG_GROSS_EUR', 'BACKLOG_GROSS_JPY',
                'BACKLOG_NET_USD', 'BACKLOG_NET_EUR', 'BACKLOG_NET_JPY', 'BACKLOG_NET_QUANTITY',
                'BACKLOG_GROSS_AMOUNT_TRANSACTION_CURRENCY', 'BACKLOG_NET_AMOUNT_TRANSACTION_CURRENCY', 'BILL_SOURCE_CODE', 'IS_BLANKET_ORDER',
                'BOOKING_AND_BILLING_CODE', 'BOOKING_CATEGORY', 'IS_BOOKING', 'CUSTOMER_OF_INTEREST', 'CONTRACT_CURRENCY',
                'ACCRUAL_AMOUNT_CONTRACT_CURRENCY', 'GROSS_AMOUNT_CONTRACT_CURRENCY', 'BACKLOG_NET_AMOUNT_CONTRACT_CURRENCY',
                'CUSTOMER_REQUESTED_DATE', 'IS_CRD_DELQ', 'CREDIT_ACCOUNT', 'REQUESTED_SHIP_DATE', 'IS_CRSD_DELQ',
                'CUSTOMER_SERVICE_REPRESENTATIVE', 'TRANSACTION_CURRENCY_ISO_CODE', 'CUSTOMER_PA_NUMBER', 'CUSTOMER_PO_NUMBER',
                'CUSTOMER_RECIEPT_BANK_CODE', 'DEBIT_ACCOUNT', 'DELIVERY_REQUEST_DATE', 'DELIVERY_SOURCE_CD', 'ORDER_DELIVERY_HOLD_STATUS',
                'DEMAND_BANK', 'DEMAND_CLASS', 'DEMAND_TYPE', 'ORDER_DELIVERY_STATUS_DTME', 'ENTRY_DATE',
                'EARLIEST_POSSIBLE_SHIP_DATE', 'FMS_SERVICE_PROGRAM', 'HANDLING_CODE', 'ORDER_HDR_HOLD_STATUS', 'INVENTORY_INSURANCE_COVERAGE',
                'INVOICE_NUM', 'INVOICED_QUANTITY', 'ORDER_ITM_HOLD_STATUS', 'JOURNAL_DESCRIPTION', 'LIABILITY_AGREEMENT',
                'ORDER_LINE_STATUS', 'MANUFACTURER_RESCHEDULED_SHIP_DATE', 'IS_MRSD_DELQ', 'ORIGINAL_SCHEDULED_SHIP_DATE',
                'IS_MSD_DELQ', 'NNCO_AGREEMENT_TYPE', 'IS_NNCO', 'OBR_TRX_ID', 'OPERATING_UNIT', 'ORA_DELIVERY_STATUS_CODE',
                'DELIVERY_STATUS_CODE', 'LINE_NUMBER', 'ORDER_DLVRY_SOURCE_CODE', 'ORDER_NUMBER', 'ORDER_SOURCE_CODE', 'ORDER_STATUS',
                'PLANNED_DELIVERY_DATE', 'IS_PDD_DELQ', 'PICK_QUANTITY', 'PRICE_SOURCE', 'RESCHEDULED_DELIVERY_DATE', 'IS_RDD_DELQ',
                'SCHEDULED_SHIPPED_DATE', 'IS_SAMPLES', 'SHIP_CONFIRMED_QTY', 'SOLD_TO_CUST_CODE', 'SOURCE_DEMAND_BANK',
                'SERVICE_PROGRAM_CODE', 'SERVICE_PROGRAM_CUSTOMER', 'TRANSACTION_DATE', 'TRANSIT_DAYS', 'IS_TRUE_BACKLOG',
                'IS_SALES_TRUE_BACKLOG', 'ACCRUAL_AMOUNT_TRANSACTION_CURRENCY', 'UNIT_PRICE_TRANSACTION_CURRENCY', 'UNIT_PRICE_CRRT_AMT',
                'UNIT_PRICE_SOURCE_CD', 'UNIT_PRICE_USD', 'UNIT_PRICE_EUR', 'UNIT_PRICE_JPY', 'IS_WS_FLAG', 'BUSINESS_CLASS_CODE',
                'ACCRUAL_STATUS', 'CPN_REVISION_CODE', 'FILE_NAME', 'ODM_END_CORP_CD', 'BIW_UPD_DTTM', 'BIW_BATCH_ID',
                'BIW_MD5_KEY'] 
        ,post_hook= [v_sql_upd_success_batch]
        )
}}

WITH STG_BACKLOG AS 
(
  SELECT 
    BACKLOG_KEY, 
    REPLACE(PROCESS_DATE,'-')::number  as SNAPSHOT_DATE_KEY,
    DIRECT_CUSTOMER_KEY,
    INDIRECT_CUSTOMER_KEY,       
    END_CUSTOMER_KEY,      
    DIRECT_CORPORATION_KEY,
    END_CORPORATION_KEY,        
    MARKET_PRODUCT_NUMBER_KEY, 
    INTERNAL_PART_NUMBER_KEY,   
    PROCESS_DATE,
    DIRECT_CUSTOMER_CODE,
    INDIRECT_CUSTOMER_CODE,                                  
    END_CUSTOMER_CODE,                                  
    DIRECT_CORPORATION_CODE,
    END_CORPORATION_CODE,
    MARKET_PRODUCT_NUMBER,
    INTERNAL_PART_NUMBER,
    CUSTOMER_PART_NUMBER,
    SOURCE_OF_SALE,
    SALES_ORDER_LINE_ITEM_DELIVERY,
    REGION,
    LEGACY_SALES_ORDER_NUMBER,
    LEGACY_SALES_ITEM_NUMBER,
    LEGACY_SALES_DELIVERY_NUMBER,
    SALES_QUOTE_NUM,
    ORDER_TYPE_CODE,
    ACCRUAL_AMOUNT_USD,
    ACCRUAL_CATEGORY,
    ACCRUAL_COMMENTS,
    ACCRUAL_FACTOR,
    ACCRUAL_RULE_ID,
    ACCRUAL_RULE_SET,
    ACCRUAL_TRANSACTION_DATE,
    ACCRUAL_TYPE,
    ACTIVITY,
    BACKLOG_GROSS_USD, 
    BACKLOG_GROSS_EUR,
    BACKLOG_GROSS_JPY,
    BACKLOG_NET_USD, 
    BACKLOG_NET_EUR,
    BACKLOG_NET_JPY,
    BACKLOG_NET_QUANTITY,
    BACKLOG_GROSS_AMOUNT_TRANSACTION_CURRENCY,
    BACKLOG_NET_AMOUNT_TRANSACTION_CURRENCY,
    BILL_SOURCE_CODE,
    IS_BLANKET_ORDER,
    BOOKING_AND_BILLING_CODE,
    BOOKING_CATEGORY,
    IS_BOOKING,
    CUSTOMER_OF_INTEREST,
    CONTRACT_CURRENCY,
    ACCRUAL_AMOUNT_CONTRACT_CURRENCY,
    GROSS_AMOUNT_CONTRACT_CURRENCY,
    BACKLOG_NET_AMOUNT_CONTRACT_CURRENCY,
    CUSTOMER_REQUESTED_DATE,
    IS_CRD_DELQ,
    CREDIT_ACCOUNT,
    REQUESTED_SHIP_DATE,
    IS_CRSD_DELQ,
    CUSTOMER_SERVICE_REPRESENTATIVE,
    TRANSACTION_CURRENCY_ISO_CODE,
    CUSTOMER_PA_NUMBER,
    CUSTOMER_PO_NUMBER,
    CUSTOMER_RECIEPT_BANK_CODE,
    DEBIT_ACCOUNT,
    DELIVERY_REQUEST_DATE,
    DELIVERY_SOURCE_CD,
    ORDER_DELIVERY_HOLD_STATUS,
    DEMAND_BANK,
    DEMAND_CLASS,
    DEMAND_TYPE,
    ORDER_DELIVERY_STATUS_DTME,
    ENTRY_DATE,
    EARLIEST_POSSIBLE_SHIP_DATE,
    FMS_SERVICE_PROGRAM,
    HANDLING_CODE,
    ORDER_HDR_HOLD_STATUS,
    INVENTORY_INSURANCE_COVERAGE,
    INVOICE_NUM,
    INVOICED_QUANTITY,
    ORDER_ITM_HOLD_STATUS,
    JOURNAL_DESCRIPTION,
    LIABILITY_AGREEMENT,
    ORDER_LINE_STATUS,
    MANUFACTURER_RESCHEDULED_SHIP_DATE,
    IS_MRSD_DELQ,
    ORIGINAL_SCHEDULED_SHIP_DATE,
    IS_MSD_DELQ,
    NNCO_AGREEMENT_TYPE,
    IS_NNCO,
    OBR_TRX_ID,
    OPERATING_UNIT,
    ORA_DELIVERY_STATUS_CODE,
    DELIVERY_STATUS_CODE,
    LINE_NUMBER,
    ORDER_DLVRY_SOURCE_CODE,
    ORDER_NUMBER,
    ORDER_SOURCE_CODE,
    ORDER_STATUS,
    PLANNED_DELIVERY_DATE,
    IS_PDD_DELQ,
    PICK_QUANTITY,
    PRICE_SOURCE,
    RESCHEDULED_DELIVERY_DATE,
    IS_RDD_DELQ,
    SCHEDULED_SHIPPED_DATE,
    IS_SAMPLES,
    SHIP_CONFIRMED_QTY,
    SOLD_TO_CUST_CODE,
    SOURCE_DEMAND_BANK,
    SERVICE_PROGRAM_CODE,
    SERVICE_PROGRAM_CUSTOMER,
    TRANSACTION_DATE,
    TRANSIT_DAYS,
    IS_TRUE_BACKLOG,
    IS_SALES_TRUE_BACKLOG,
    ACCRUAL_AMOUNT_TRANSACTION_CURRENCY,
    UNIT_PRICE_TRANSACTION_CURRENCY,
    UNIT_PRICE_CRRT_AMT,
    UNIT_PRICE_SOURCE_CD,
    UNIT_PRICE_USD,
    UNIT_PRICE_EUR,
    UNIT_PRICE_JPY,
    IS_WS_FLAG,
    BUSINESS_CLASS_CODE,
    ACCRUAL_STATUS,
    CPN_REVISION_CODE,
    FILE_NAME,
	ODM_END_CORP_CD,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY 
FROM {{ref('ETL_MART_SALES_BACKLOG_FACT')}}
)

SELECT 
STG.*
FROM STG_BACKLOG STG
{% if is_incremental() %}
LEFT JOIN {{ this }} TGT
ON
	STG.BACKLOG_KEY = TGT.BACKLOG_KEY
    and STG.SNAPSHOT_DATE_KEY = TGT.SNAPSHOT_DATE_KEY 
WHERE
	TGT.BIW_MD5_KEY <> STG.BIW_MD5_KEY
	OR TGT.BIW_MD5_KEY IS NULL
{% endif %}
{% if not is_incremental() or var('is_one_time_load') %} 
    UNION ALL
SELECT    
    BACKLOG_KEY, 
    REPLACE(PROCESS_DATE,'-')::number  as SNAPSHOT_DATE_KEY,
    DIRECT_CUSTOMER_KEY,
    INDIRECT_CUSTOMER_KEY,       
    END_CUSTOMER_KEY,      
    DIRECT_CORPORATION_KEY,
    END_CORPORATION_KEY,        
    MARKET_PRODUCT_NUMBER_KEY, 
    INTERNAL_PART_NUMBER_KEY,   
    PROCESS_DATE,
    DIRECT_CUSTOMER_CODE,
    INDIRECT_CUSTOMER_CODE,                                  
    END_CUSTOMER_CODE,                                  
    DIRECT_CORPORATION_CODE,
    END_CORPORATION_CODE,
    MARKET_PRODUCT_NUMBER,
    INTERNAL_PART_NUMBER,
    CUSTOMER_PART_NUMBER,
    SOURCE_OF_SALE,
    SALES_ORDER_LINE_ITEM_DELIVERY,
    REGION,
    LEGACY_SALES_ORDER_NUMBER,
    LEGACY_SALES_ITEM_NUMBER,
    LEGACY_SALES_DELIVERY_NUMBER,
    SALES_QUOTE_NUM,
    ORDER_TYPE_CODE,
    ACCRUAL_AMOUNT_USD,
    ACCRUAL_CATEGORY,
    ACCRUAL_COMMENTS,
    ACCRUAL_FACTOR,
    ACCRUAL_RULE_ID,
    ACCRUAL_RULE_SET,
    ACCRUAL_TRANSACTION_DATE,
    ACCRUAL_TYPE,
    ACTIVITY,
    BACKLOG_GROSS_USD, 
    BACKLOG_GROSS_EUR,
    BACKLOG_GROSS_JPY,
    BACKLOG_NET_USD, 
    BACKLOG_NET_EUR,
    BACKLOG_NET_JPY,
    BACKLOG_NET_QUANTITY,
    BACKLOG_GROSS_AMOUNT_TRANSACTION_CURRENCY,
    BACKLOG_NET_AMOUNT_TRANSACTION_CURRENCY,
    BILL_SOURCE_CODE,
    IS_BLANKET_ORDER,
    BOOKING_AND_BILLING_CODE,
    BOOKING_CATEGORY,
    IS_BOOKING,
    CUSTOMER_OF_INTEREST,
    CONTRACT_CURRENCY,
    ACCRUAL_AMOUNT_CONTRACT_CURRENCY,
    GROSS_AMOUNT_CONTRACT_CURRENCY,
    BACKLOG_NET_AMOUNT_CONTRACT_CURRENCY,
    CUSTOMER_REQUESTED_DATE,
    IS_CRD_DELQ,
    CREDIT_ACCOUNT,
    REQUESTED_SHIP_DATE,
    IS_CRSD_DELQ,
    CUSTOMER_SERVICE_REPRESENTATIVE,
    TRANSACTION_CURRENCY_ISO_CODE,
    CUSTOMER_PA_NUMBER,
    CUSTOMER_PO_NUMBER,
    CUSTOMER_RECIEPT_BANK_CODE,
    DEBIT_ACCOUNT,
    DELIVERY_REQUEST_DATE,
    DELIVERY_SOURCE_CD,
    ORDER_DELIVERY_HOLD_STATUS,
    DEMAND_BANK,
    DEMAND_CLASS,
    DEMAND_TYPE,
    ORDER_DELIVERY_STATUS_DTME,
    ENTRY_DATE,
    EARLIEST_POSSIBLE_SHIP_DATE,
    FMS_SERVICE_PROGRAM,
    HANDLING_CODE,
    ORDER_HDR_HOLD_STATUS,
    INVENTORY_INSURANCE_COVERAGE,
    INVOICE_NUM,
    INVOICED_QUANTITY,
    ORDER_ITM_HOLD_STATUS,
    JOURNAL_DESCRIPTION,
    LIABILITY_AGREEMENT,
    ORDER_LINE_STATUS,
    MANUFACTURER_RESCHEDULED_SHIP_DATE,
    IS_MRSD_DELQ,
    ORIGINAL_SCHEDULED_SHIP_DATE,
    IS_MSD_DELQ,
    NNCO_AGREEMENT_TYPE,
    IS_NNCO,
    OBR_TRX_ID,
    OPERATING_UNIT,
    ORA_DELIVERY_STATUS_CODE,
    DELIVERY_STATUS_CODE,
    LINE_NUMBER,
    ORDER_DLVRY_SOURCE_CODE,
    ORDER_NUMBER,
    ORDER_SOURCE_CODE,
    ORDER_STATUS,
    PLANNED_DELIVERY_DATE,
    IS_PDD_DELQ,
    PICK_QUANTITY,
    PRICE_SOURCE,
    RESCHEDULED_DELIVERY_DATE,
    IS_RDD_DELQ,
    SCHEDULED_SHIPPED_DATE,
    IS_SAMPLES,
    SHIP_CONFIRMED_QTY,
    SOLD_TO_CUST_CODE,
    SOURCE_DEMAND_BANK,
    SERVICE_PROGRAM_CODE,
    SERVICE_PROGRAM_CUSTOMER,
    TRANSACTION_DATE,
    TRANSIT_DAYS,
    IS_TRUE_BACKLOG,
    IS_SALES_TRUE_BACKLOG,
    ACCRUAL_AMOUNT_TRANSACTION_CURRENCY,
    UNIT_PRICE_TRANSACTION_CURRENCY,
    UNIT_PRICE_CRRT_AMT,
    UNIT_PRICE_SOURCE_CD,
    UNIT_PRICE_USD,
    UNIT_PRICE_EUR,
    UNIT_PRICE_JPY,
    IS_WS_FLAG,
    BUSINESS_CLASS_CODE,
    ACCRUAL_STATUS,
    CPN_REVISION_CODE,
    FILE_NAME,
	ODM_END_CORP_CD,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY 
FROM {{ref('ETL_MART_SALES_BACKLOG_FACT_HISTORY_ONE_TIME_LOAD')}}
UNION ALL
SELECT    
    BACKLOG_KEY, 
    REPLACE(PROCESS_DATE,'-')::number  as SNAPSHOT_DATE_KEY,
    DIRECT_CUSTOMER_KEY,
    INDIRECT_CUSTOMER_KEY,       
    END_CUSTOMER_KEY,      
    DIRECT_CORPORATION_KEY,
    END_CORPORATION_KEY,        
    MARKET_PRODUCT_NUMBER_KEY, 
    INTERNAL_PART_NUMBER_KEY,   
    PROCESS_DATE,
    DIRECT_CUSTOMER_CODE,
    INDIRECT_CUSTOMER_CODE,                                  
    END_CUSTOMER_CODE,                                  
    DIRECT_CORPORATION_CODE,
    END_CORPORATION_CODE,
    MARKET_PRODUCT_NUMBER,
    INTERNAL_PART_NUMBER,
    CUSTOMER_PART_NUMBER,
    SOURCE_OF_SALE,
    SALES_ORDER_LINE_ITEM_DELIVERY,
    REGION,
    LEGACY_SALES_ORDER_NUMBER,
    LEGACY_SALES_ITEM_NUMBER,
    LEGACY_SALES_DELIVERY_NUMBER,
    SALES_QUOTE_NUM,
    ORDER_TYPE_CODE,
    ACCRUAL_AMOUNT_USD,
    ACCRUAL_CATEGORY,
    ACCRUAL_COMMENTS,
    ACCRUAL_FACTOR,
    ACCRUAL_RULE_ID,
    ACCRUAL_RULE_SET,
    ACCRUAL_TRANSACTION_DATE,
    ACCRUAL_TYPE,
    ACTIVITY,
    BACKLOG_GROSS_USD, 
    BACKLOG_GROSS_EUR,
    BACKLOG_GROSS_JPY,
    BACKLOG_NET_USD, 
    BACKLOG_NET_EUR,
    BACKLOG_NET_JPY,
    BACKLOG_NET_QUANTITY,
    BACKLOG_GROSS_AMOUNT_TRANSACTION_CURRENCY,
    BACKLOG_NET_AMOUNT_TRANSACTION_CURRENCY,
    BILL_SOURCE_CODE,
    IS_BLANKET_ORDER,
    BOOKING_AND_BILLING_CODE,
    BOOKING_CATEGORY,
    IS_BOOKING,
    CUSTOMER_OF_INTEREST,
    CONTRACT_CURRENCY,
    ACCRUAL_AMOUNT_CONTRACT_CURRENCY,
    GROSS_AMOUNT_CONTRACT_CURRENCY,
    BACKLOG_NET_AMOUNT_CONTRACT_CURRENCY,
    CUSTOMER_REQUESTED_DATE,
    IS_CRD_DELQ,
    CREDIT_ACCOUNT,
    REQUESTED_SHIP_DATE,
    IS_CRSD_DELQ,
    CUSTOMER_SERVICE_REPRESENTATIVE,
    TRANSACTION_CURRENCY_ISO_CODE,
    CUSTOMER_PA_NUMBER,
    CUSTOMER_PO_NUMBER,
    CUSTOMER_RECIEPT_BANK_CODE,
    DEBIT_ACCOUNT,
    DELIVERY_REQUEST_DATE,
    DELIVERY_SOURCE_CD,
    ORDER_DELIVERY_HOLD_STATUS,
    DEMAND_BANK,
    DEMAND_CLASS,
    DEMAND_TYPE,
    ORDER_DELIVERY_STATUS_DTME,
    ENTRY_DATE,
    EARLIEST_POSSIBLE_SHIP_DATE,
    FMS_SERVICE_PROGRAM,
    HANDLING_CODE,
    ORDER_HDR_HOLD_STATUS,
    INVENTORY_INSURANCE_COVERAGE,
    INVOICE_NUM,
    INVOICED_QUANTITY,
    ORDER_ITM_HOLD_STATUS,
    JOURNAL_DESCRIPTION,
    LIABILITY_AGREEMENT,
    ORDER_LINE_STATUS,
    MANUFACTURER_RESCHEDULED_SHIP_DATE,
    IS_MRSD_DELQ,
    ORIGINAL_SCHEDULED_SHIP_DATE,
    IS_MSD_DELQ,
    NNCO_AGREEMENT_TYPE,
    IS_NNCO,
    OBR_TRX_ID,
    OPERATING_UNIT,
    ORA_DELIVERY_STATUS_CODE,
    DELIVERY_STATUS_CODE,
    LINE_NUMBER,
    ORDER_DLVRY_SOURCE_CODE,
    ORDER_NUMBER,
    ORDER_SOURCE_CODE,
    ORDER_STATUS,
    PLANNED_DELIVERY_DATE,
    IS_PDD_DELQ,
    PICK_QUANTITY,
    PRICE_SOURCE,
    RESCHEDULED_DELIVERY_DATE,
    IS_RDD_DELQ,
    SCHEDULED_SHIPPED_DATE,
    IS_SAMPLES,
    SHIP_CONFIRMED_QTY,
    SOLD_TO_CUST_CODE,
    SOURCE_DEMAND_BANK,
    SERVICE_PROGRAM_CODE,
    SERVICE_PROGRAM_CUSTOMER,
    TRANSACTION_DATE,
    TRANSIT_DAYS,
    IS_TRUE_BACKLOG,
    IS_SALES_TRUE_BACKLOG,
    ACCRUAL_AMOUNT_TRANSACTION_CURRENCY,
    UNIT_PRICE_TRANSACTION_CURRENCY,
    UNIT_PRICE_CRRT_AMT,
    UNIT_PRICE_SOURCE_CD,
    UNIT_PRICE_USD,
    UNIT_PRICE_EUR,
    UNIT_PRICE_JPY,
    IS_WS_FLAG,
    BUSINESS_CLASS_CODE,
    ACCRUAL_STATUS,
    CPN_REVISION_CODE,
    FILE_NAME,
	ODM_END_CORP_CD,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY 
FROM {{ref('ETL_MART_SALES_BACKLOG_FACT_FCS_ONE_TIME_LOAD')}}
UNION ALL
SELECT    
    BACKLOG_KEY, 
    REPLACE(PROCESS_DATE,'-')::number  as SNAPSHOT_DATE_KEY,
    DIRECT_CUSTOMER_KEY,
    INDIRECT_CUSTOMER_KEY,       
    END_CUSTOMER_KEY,      
    DIRECT_CORPORATION_KEY,
    END_CORPORATION_KEY,        
    MARKET_PRODUCT_NUMBER_KEY, 
    INTERNAL_PART_NUMBER_KEY,   
    PROCESS_DATE,
    DIRECT_CUSTOMER_CODE,
    INDIRECT_CUSTOMER_CODE,                                  
    END_CUSTOMER_CODE,                                  
    DIRECT_CORPORATION_CODE,
    END_CORPORATION_CODE,
    MARKET_PRODUCT_NUMBER,
    INTERNAL_PART_NUMBER,
    CUSTOMER_PART_NUMBER,
    SOURCE_OF_SALE,
    SALES_ORDER_LINE_ITEM_DELIVERY,
    REGION,
    LEGACY_SALES_ORDER_NUMBER,
    LEGACY_SALES_ITEM_NUMBER,
    LEGACY_SALES_DELIVERY_NUMBER,
    SALES_QUOTE_NUM,
    ORDER_TYPE_CODE,
    ACCRUAL_AMOUNT_USD,
    ACCRUAL_CATEGORY,
    ACCRUAL_COMMENTS,
    ACCRUAL_FACTOR,
    ACCRUAL_RULE_ID,
    ACCRUAL_RULE_SET,
    ACCRUAL_TRANSACTION_DATE,
    ACCRUAL_TYPE,
    ACTIVITY,
    BACKLOG_GROSS_USD, 
    BACKLOG_GROSS_EUR,
    BACKLOG_GROSS_JPY,
    BACKLOG_NET_USD, 
    BACKLOG_NET_EUR,
    BACKLOG_NET_JPY,
    BACKLOG_NET_QUANTITY,
    BACKLOG_GROSS_AMOUNT_TRANSACTION_CURRENCY,
    BACKLOG_NET_AMOUNT_TRANSACTION_CURRENCY,
    BILL_SOURCE_CODE,
    IS_BLANKET_ORDER,
    BOOKING_AND_BILLING_CODE,
    BOOKING_CATEGORY,
    IS_BOOKING,
    CUSTOMER_OF_INTEREST,
    CONTRACT_CURRENCY,
    ACCRUAL_AMOUNT_CONTRACT_CURRENCY,
    GROSS_AMOUNT_CONTRACT_CURRENCY,
    BACKLOG_NET_AMOUNT_CONTRACT_CURRENCY,
    CUSTOMER_REQUESTED_DATE,
    IS_CRD_DELQ,
    CREDIT_ACCOUNT,
    REQUESTED_SHIP_DATE,
    IS_CRSD_DELQ,
    CUSTOMER_SERVICE_REPRESENTATIVE,
    TRANSACTION_CURRENCY_ISO_CODE,
    CUSTOMER_PA_NUMBER,
    CUSTOMER_PO_NUMBER,
    CUSTOMER_RECIEPT_BANK_CODE,
    DEBIT_ACCOUNT,
    DELIVERY_REQUEST_DATE,
    DELIVERY_SOURCE_CD,
    ORDER_DELIVERY_HOLD_STATUS,
    DEMAND_BANK,
    DEMAND_CLASS,
    DEMAND_TYPE,
    ORDER_DELIVERY_STATUS_DTME,
    ENTRY_DATE,
    EARLIEST_POSSIBLE_SHIP_DATE,
    FMS_SERVICE_PROGRAM,
    HANDLING_CODE,
    ORDER_HDR_HOLD_STATUS,
    INVENTORY_INSURANCE_COVERAGE,
    INVOICE_NUM,
    INVOICED_QUANTITY,
    ORDER_ITM_HOLD_STATUS,
    JOURNAL_DESCRIPTION,
    LIABILITY_AGREEMENT,
    ORDER_LINE_STATUS,
    MANUFACTURER_RESCHEDULED_SHIP_DATE,
    IS_MRSD_DELQ,
    ORIGINAL_SCHEDULED_SHIP_DATE,
    IS_MSD_DELQ,
    NNCO_AGREEMENT_TYPE,
    IS_NNCO,
    OBR_TRX_ID,
    OPERATING_UNIT,
    ORA_DELIVERY_STATUS_CODE,
    DELIVERY_STATUS_CODE,
    LINE_NUMBER,
    ORDER_DLVRY_SOURCE_CODE,
    ORDER_NUMBER,
    ORDER_SOURCE_CODE,
    ORDER_STATUS,
    PLANNED_DELIVERY_DATE,
    IS_PDD_DELQ,
    PICK_QUANTITY,
    PRICE_SOURCE,
    RESCHEDULED_DELIVERY_DATE,
    IS_RDD_DELQ,
    SCHEDULED_SHIPPED_DATE,
    IS_SAMPLES,
    SHIP_CONFIRMED_QTY,
    SOLD_TO_CUST_CODE,
    SOURCE_DEMAND_BANK,
    SERVICE_PROGRAM_CODE,
    SERVICE_PROGRAM_CUSTOMER,
    TRANSACTION_DATE,
    TRANSIT_DAYS,
    IS_TRUE_BACKLOG,
    IS_SALES_TRUE_BACKLOG,
    ACCRUAL_AMOUNT_TRANSACTION_CURRENCY,
    UNIT_PRICE_TRANSACTION_CURRENCY,
    UNIT_PRICE_CRRT_AMT,
    UNIT_PRICE_SOURCE_CD,
    UNIT_PRICE_USD,
    UNIT_PRICE_EUR,
    UNIT_PRICE_JPY,
    IS_WS_FLAG,
    BUSINESS_CLASS_CODE,
    ACCRUAL_STATUS,
    CPN_REVISION_CODE,
    FILE_NAME,
	ODM_END_CORP_CD, 
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}}	 as BIW_BATCH_ID,
    BIW_MD5_KEY 
FROM {{ref('ETL_MART_SALES_BACKLOG_FACT_QUANTENNA_ONE_TIME_LOAD')}}
{% endif %}