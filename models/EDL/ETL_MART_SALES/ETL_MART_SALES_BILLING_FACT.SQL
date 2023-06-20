/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_BILLING_FACT

Version     Date            Author             Description
-------     --------        -----------        ----------------------------------
1.0         10/18/2022      Sruthi Kasbe       Updated customer columns and its order
2.0         12/16/2022      Vinay Subramanian  Corrected Currency_exchange logic
---------------------------------------------------------------------------*/
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
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_BILLING_FACT'-%}
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
         description = 'Building ETL table BILLING_FACT for Sales Mart '
        ,transient=true
        ,materialized='table'
        ,schema ='ETL_MART_SALES'
        ,alias='BILLING_FACT'
        ,tags =['MART_SALES']
        ,post_hook= [v_sql_upd_success_batch]	
        )
}}

/* Step 5 Build the ETL table*/

SELECT
    DISTINCT
    BILLING_DATE_KEY,
    DIRECT_CUSTOMER_KEY,
    INDIRECT_CUSTOMER_KEY,
    END_CUSTOMER_KEY,
    DIRECT_CORPORATION_KEY,
    END_CORPORATION_KEY,
    MARKET_PRODUCT_NUMBER_KEY,
    INTERNAL_PART_NUMBER_KEY,
    DIRECT_CUSTOMER_CODE,
    INDIRECT_CUSTOMER_CODE,
    END_CUSTOMER_CODE,
    DIRECT_CORPORATION_CODE,
    END_CORPORATION_CODE,
    DIST_END_CUSTOMER_CODE,
    MARKET_PRODUCT_NUMBER,
    INTERNAL_PART_NUMBER,
    PROCESS_DATE,
    SOURCE_OF_SALE,
    SALES_ORDER_LINE_ITEM_DELIVERY,
    SALES_ORDER_NUMBER,
    SALES_ITEM_NUMBER,
    SALES_DELIVERY_NUMBER,
    REGION,
    ORDER_TYPE,
    HANDLING_CODE,
    CUSTOMER_PART_NUMBER,
    CUSTOMER_PA_NUMBER,
    REQUESTED_DATE,
    ORIGINAL_SCHEDULED_SHIP_DATE,
    SCHEDULED_SHIPPED_DATE,
    EARLIEST_POSSIBLE_SHIP_DATE,
    PLANNED_DELIVERY_DATE,
    RESCHEDULED_DELIVERY_DATE,
    REQUESTED_SHIP_DATE,
    ENTRY_DATE,
    CUSTOMER_OF_INTEREST,
    IS_NNCO,
    NNCO_AGREEMENT_TYPE,
    ODM_END_CORP_CODE,
    BILL_TO_COUNTRY,
    BILL_TO_CUST_CODE,
    SUB_CORP_CODE,
    SHIP_FROM_AREA,
    CUSTOMER_PO_NUMBER,
    QUOTE_ID,
    OPERATING_UNIT,
    ITEM_STATUS,
    IS_REVERSAL_REQUIRED,
    ACTIVITY,
    BOOKING_CATEGORY,
    BILL_SOURCE_CODE,
    BUSINESS_CLASS_CODE,
    CREDIT_REASON_CODE,
    CREDIT_CODE_DESCRIPTION,
    PRICE_SOURCE,
    GL_POSTED_DATE,
    TRANSACTION_DATE,
    CUSTOMER_TRX_ID,
    CUSTOMER_TRX_LINE_ID,
    OBR_TRX_ID,
    TRX_LINE_NUMBER,
    BILLING_DOCUMENT_NUMBER,
    ORIGINAL_DOCUMENT_NUMBER,
    IS_TRUE_BILL,
    GL_REVENUE_ACCOUNT,
    ACCRUAL_CATEGORY,
    JOURNAL_DESCRIPTION,
    LEDGER,
    DEBIT_ACCOUNT,
    CREDIT_ACCOUNT,
    ACCRUAL_RULE_SET,
    ACCRUAL_RULE_ID,
    ACCRUAL_COMMENTS,
    ACCRUAL_TYPE,
    ACCRUAL_FACTOR,
    ACCRUAL_TRANSACTION_DATE,
    IS_ACCRUAL_DSA,
    IS_ACCRUAL_DCD,
    IS_ACCRUAL_REB,
    IS_ACCRUAL_DSC,
    IS_ACCRUAL_REVERSAL,
    IS_QUANTITY_OVERRIDE,
    TRANSACTION_CURRENCY_ISO_CODE,
    CONTRACT_CURRENCY,
    BILLING_GROSS_QUANTITY,
    BILLING_NET_QUANTITY,
    UNIT_PRICE_USD,
    UNIT_PRICE_EUR,
    UNIT_PRICE_JPY,
    UNIT_PRICE_TRANSACTION_CURRENCY,
    GROSS_AMOUNT_USD,
    GROSS_AMOUNT_TRANSACTION_CURRENCY,
	GROSS_AMOUNT_EUR,
    GROSS_AMOUNT_JPY,
    GROSS_AMOUNT_CONTRACT_CURRENCY,
    BILLING_NET_AMOUNT_USD,
	BILLING_NET_AMOUNT_EUR,
	BILLING_NET_AMOUNT_JPY,
    BILLING_NET_AMOUNT_TRANSACTION_CURRENCY,
    BILLING_NET_AMOUNT_CONTRACT_CURRENCY,
    ACCRUAL_AMOUNT_USD,
    ACCRUAL_AMOUNT_TRANSACTION_CURRENCY,
    ACCRUAL_AMOUNT_CONTRACT_CURRENCY,
    DW_CREATE_DT,
    MD5(object_construct ('col1',BILLING_DATE_KEY::string, 'col2',DIRECT_CUSTOMER_KEY::string, 'col3',INDIRECT_CUSTOMER_KEY::string, 
    'col4',END_CUSTOMER_KEY::string, 'col5',DIRECT_CORPORATION_KEY::string, 'col6',END_CORPORATION_KEY::string, 'col7',MARKET_PRODUCT_NUMBER_KEY::string, 
    'col8',INTERNAL_PART_NUMBER_KEY::string, 'col9',DIRECT_CUSTOMER_CODE::string, 'col10',INDIRECT_CUSTOMER_CODE::string, 'col11',END_CUSTOMER_CODE::string, 
    'col12',DIRECT_CORPORATION_CODE::string, 'col13',END_CORPORATION_CODE::string, 'col14',DIST_END_CUSTOMER_CODE::string, 'col15',MARKET_PRODUCT_NUMBER::string, 
    'col16',INTERNAL_PART_NUMBER::string, 'col17',PROCESS_DATE::string, 'col18',SOURCE_OF_SALE::string, 'col19',SALES_ORDER_LINE_ITEM_DELIVERY::string, 
    'col20',SALES_ORDER_NUMBER::string, 'col21',SALES_ITEM_NUMBER::string, 'col22',SALES_DELIVERY_NUMBER::string, 'col23',REGION::string, 'col24',ORDER_TYPE::string, 
    'col25',HANDLING_CODE::string, 'col26',CUSTOMER_PART_NUMBER::string, 'col27',CUSTOMER_PA_NUMBER::string, 'col28',REQUESTED_DATE::string, 
    'col29',ORIGINAL_SCHEDULED_SHIP_DATE::string, 'col30',SCHEDULED_SHIPPED_DATE::string, 'col31',EARLIEST_POSSIBLE_SHIP_DATE::string, 
    'col32',PLANNED_DELIVERY_DATE::string, 'col33',RESCHEDULED_DELIVERY_DATE::string, 'col34',REQUESTED_SHIP_DATE::string, 'col35',ENTRY_DATE::string, 
    'col36',CUSTOMER_OF_INTEREST::string, 'col37',IS_NNCO::string, 'col38',NNCO_AGREEMENT_TYPE::string, 'col39',ODM_END_CORP_CODE::string, 
    'col40',BILL_TO_COUNTRY::string, 'col41',BILL_TO_CUST_CODE::string, 'col42',SUB_CORP_CODE::string, 'col43',SHIP_FROM_AREA::string, 
    'col44',CUSTOMER_PO_NUMBER::string, 'col45',QUOTE_ID::string, 'col46',OPERATING_UNIT::string, 'col47',ITEM_STATUS::string, 'col48',IS_REVERSAL_REQUIRED::string, 
    'col49',ACTIVITY::string, 'col50',BOOKING_CATEGORY::string, 'col51',BILL_SOURCE_CODE::string, 'col52',BUSINESS_CLASS_CODE::string, 
    'col53',CREDIT_REASON_CODE::string, 'col54',CREDIT_CODE_DESCRIPTION::string, 'col55',PRICE_SOURCE::string, 'col56',GL_POSTED_DATE::string, 
    'col57',TRANSACTION_DATE::string, 'col58',CUSTOMER_TRX_ID::string, 'col59',CUSTOMER_TRX_LINE_ID::string, 'col60',OBR_TRX_ID::string, 
    'col61',TRX_LINE_NUMBER::string, 'col62',BILLING_DOCUMENT_NUMBER::string, 'col63',ORIGINAL_DOCUMENT_NUMBER::string, 'col64',IS_TRUE_BILL::string, 
    'col65',GL_REVENUE_ACCOUNT::string, 'col66',ACCRUAL_CATEGORY::string, 'col67',JOURNAL_DESCRIPTION::string, 'col68',LEDGER::string, 
    'col69',DEBIT_ACCOUNT::string, 'col70',CREDIT_ACCOUNT::string, 'col71',ACCRUAL_RULE_SET::string, 'col72',ACCRUAL_RULE_ID::string, 
    'col73',ACCRUAL_COMMENTS::string, 'col74',ACCRUAL_TYPE::string, 'col75',ACCRUAL_FACTOR::string, 'col76',ACCRUAL_TRANSACTION_DATE::string, 
    'col77',IS_ACCRUAL_DSA::string, 'col78',IS_ACCRUAL_DCD::string, 'col79',IS_ACCRUAL_REB::string, 'col80',IS_ACCRUAL_DSC::string, 
    'col81',IS_ACCRUAL_REVERSAL::string, 'col82',IS_QUANTITY_OVERRIDE::string, 'col83',TRANSACTION_CURRENCY_ISO_CODE::string, 
    'col84',CONTRACT_CURRENCY::string, 'col85',BILLING_GROSS_QUANTITY::string, 'col86',BILLING_NET_QUANTITY::string, 
    'col87',UNIT_PRICE_USD::string, 'col88',UNIT_PRICE_EUR::string, 'col89',UNIT_PRICE_JPY::string, 'col90',UNIT_PRICE_TRANSACTION_CURRENCY::string, 
    'col91',GROSS_AMOUNT_USD::string, 'col92',GROSS_AMOUNT_TRANSACTION_CURRENCY::string, 'col93',GROSS_AMOUNT_EUR::string, 
    'col94',GROSS_AMOUNT_JPY::string, 'col95',GROSS_AMOUNT_CONTRACT_CURRENCY::string, 'col96',BILLING_NET_AMOUNT_USD::string, 
    'col97',BILLING_NET_AMOUNT_EUR::string, 'col98',BILLING_NET_AMOUNT_JPY::string, 'col99',BILLING_NET_AMOUNT_TRANSACTION_CURRENCY::string, 
    'col100',BILLING_NET_AMOUNT_CONTRACT_CURRENCY::string, 'col101',ACCRUAL_AMOUNT_USD::string, 'col102',ACCRUAL_AMOUNT_TRANSACTION_CURRENCY::string, 
    'col103',ACCRUAL_AMOUNT_CONTRACT_CURRENCY::string, 'col104',DW_CREATE_DT::string)::string ) as BIW_MD5_KEY,
    COALESCE(STAGE1_BILL_SEQ::varchar(50),BIW_MD5_KEY::varchar(50)) AS BILLING_KEY,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} as BIW_BATCH_ID
FROM 
    {{ ref('ETL_MART_SALES_BILLING_CURRENCY_VIEW')}}
    WHERE  
    {% if var('is_backfill') %}
        DW_CREATE_DT >= '{{var('refresh_start_ts')}}'
		AND DW_CREATE_DT < '{{V_START_DTTM}}'
    {% else %}
        DW_CREATE_DT >= '{{V_LWM}}' 
		AND DW_CREATE_DT <= '{{V_HWM}}'
    {% endif %}

