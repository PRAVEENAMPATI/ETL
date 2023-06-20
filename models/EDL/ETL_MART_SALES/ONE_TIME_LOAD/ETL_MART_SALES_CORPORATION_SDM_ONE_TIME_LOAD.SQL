/*---------------------------------------------------------------------------
Command to run model:
--dbt run --select ETL_MART_SALES_CORPORATION_SDM_ONE_TIME_LOAD  

Version     Date            Author               Description
-------     --------        -----------          ----------------------------------
1.0         02/23/2023      Vinay Subramanian    Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['CORPORATION_KEY']-%}
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
/*-SELECT {{v_md5_column_list}}*/
{% endif %}

{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_ETL_MART_SALES_CORPORATION_SDM_ONE_TIME_LOAD'-%}
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
         description = 'Building ETL table BILLING Fact FCS for Sales Mart '
        ,transient=true
        ,materialized='view'
        ,schema ='ETL_MART_SALES'
        ,alias='CORPORATION_SDM'
        ,tags =['MART_SALES']
        )
}}

WITH CORP_SDM AS (
    SELECT
        MD5(CORP_CD) AS CORPORATION_KEY,
        CORP_CD AS CORPORATION_CODE,
        CORP_DESC AS CORPORATION_DESCRIPTION,
        NULL AS ERP_CUST_ACCOUNT_ID,
        CORP_CATEGORY_CD AS CORPORATION_CATEGORY_CODE,
        CORP_CATEGORY_DESC AS CORPORATION_CATEGORY_DESCRIPTION,
        ERP_STATUS_CD AS ERP_STATUS_CODE,
        HOMEREGION AS HOME_REGION,
        MARKET_MAKER_FLAG::BOOLEAN AS IS_MARKET_MAKER,
        BUSINESS_CLASS_OVERRIDE AS CLASS,
        NULL AS URL,
        NULL AS ACCOUNT_TYPE,
        NULL AS IS_CUSTOMER_ITEM_CONTROL_REQUIRED,
        NULL AS BARCODE_LABEL_CODE,
        NULL AS VENDOR_CODE,
        NULL AS COLLECTOR_NAME,
        NULL AS TAX_PRINTING_DESCRIPTION,
        NULL AS GROUPING_RULE_CODE,
        NULL AS HAS_CREDIT_CHECK,
        NULL AS HAS_CREDIT_HOLD,
        NULL AS CONTRACT_NAME,
        GAM AS GAM_USER_ID,
        NULL AS GAM_FIRST_NAME,
        NULL AS GAM_LAST_NAME,
        NULL AS GAM_EMAIL,
        NULL AS GAM_PHONE,
        NULL AS GAM_CONTACT_LAST_UPDATE_BY,
        NULL AS GAM_CONTACT_CREATED_BY,
        NULL AS GSM_USER_ID,
        NULL AS GSM_FIRST_NAME,
        NULL AS GSM_LAST_NAME,
        NULL AS GSM_EMAIL,
        NULL AS GSM_PHONE,
        NULL AS GSM_CONTACT_LAST_UPDATE_BY,
        NULL AS GSM_CONTACT_CREATED_BY,
        NULL AS ADDRESS1, 
        NULL AS ADDRESS2, 
        NULL AS ADDRESS3, 
        NULL AS ADDRESS4, 
        NULL AS CITY, 
        NULL AS STATE, 
        NULL AS ZIPCODE, 
        NULL AS COUNTRY_CODE
FROM  {{source('ZZZ_SDM_PRESENTATION', 'LKP_CORPORATIONS') }} 
WHERE CORP_CD IN ('XUNK','CHRP','HFRC','MISC','VPNC','AVPN','BBEC','BIOC','CHHC','CLIC','COBC','COMC','NANC','BUSC','ULFC','AIVC','TWKC','ICDC','DMPC',
                    'YKCC','AAIC','ITZC','AJHC','BLTC','TUKC','DKIC','ZZHC','SGDC','LMXC','LZZC','ZGGC','SBUC','CSTC','TSYC','SZLC','VELC','ADZC','CVNC',
                    'SAXC','CTSC','GSPC','AWCC','PTVC','CPZC','MLWW','GZXC','ECFY','ECGC') 
)

SELECT
    CORPORATION_KEY,
    CORPORATION_CODE,
    CORPORATION_DESCRIPTION,
    ERP_CUST_ACCOUNT_ID,
    CORPORATION_CATEGORY_CODE,
    CORPORATION_CATEGORY_DESCRIPTION,
    ERP_STATUS_CODE,
    HOME_REGION,
    IS_MARKET_MAKER,
    CLASS,
    URL,
    ACCOUNT_TYPE,
    IS_CUSTOMER_ITEM_CONTROL_REQUIRED,
    BARCODE_LABEL_CODE,
    VENDOR_CODE,
    COLLECTOR_NAME,
    TAX_PRINTING_DESCRIPTION,
    GROUPING_RULE_CODE,
    HAS_CREDIT_CHECK,
    HAS_CREDIT_HOLD,
    CONTRACT_NAME,
    GAM_USER_ID,
    GAM_FIRST_NAME,
    GAM_LAST_NAME,
    GAM_EMAIL,
    GAM_PHONE,
    GAM_CONTACT_LAST_UPDATE_BY,
    GAM_CONTACT_CREATED_BY,
    GSM_USER_ID,
    GSM_FIRST_NAME,
    GSM_LAST_NAME,
    GSM_EMAIL,
    GSM_PHONE,
    GSM_CONTACT_LAST_UPDATE_BY,
    GSM_CONTACT_CREATED_BY,
    ADDRESS1, 
    ADDRESS2, 
    ADDRESS3, 
    ADDRESS4, 
    CITY, 
    STATE, 
    ZIPCODE, 
    COUNTRY_CODE,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_INS_DTTM ,
    '{{V_START_DTTM}}'::TIMESTAMP_NTZ BIW_UPD_DTTM ,
    {{V_BIW_BATCH_ID}} AS BIW_BATCH_ID,
     md5(object_construct ('col1',CORPORATION_CODE::string, 'col2',CORPORATION_DESCRIPTION::string,
        'col3',ERP_CUST_ACCOUNT_ID::string, 'col4',CORPORATION_CATEGORY_CODE::string, 'col5',CORPORATION_CATEGORY_DESCRIPTION::string,
        'col6',ERP_STATUS_CODE::string, 'col7',HOME_REGION::string, 'col8',IS_MARKET_MAKER::string, 'col9',CLASS::string,
        'col10',URL::string, 'col11',ACCOUNT_TYPE::string, 'col12',IS_CUSTOMER_ITEM_CONTROL_REQUIRED::string,
        'col13',BARCODE_LABEL_CODE::string, 'col14',VENDOR_CODE::string, 'col15',COLLECTOR_NAME::string,
        'col16',TAX_PRINTING_DESCRIPTION::string, 'col17',GROUPING_RULE_CODE::string, 'col18',HAS_CREDIT_CHECK::string,
        'col19',HAS_CREDIT_HOLD::string, 'col20',CONTRACT_NAME::string, 'col21',GAM_USER_ID::string, 'col22',GAM_FIRST_NAME::string,
        'col23',GAM_LAST_NAME::string, 'col24',GAM_EMAIL::string, 'col25',GAM_PHONE::string, 'col26',GAM_CONTACT_LAST_UPDATE_BY::string,
        'col27',GAM_CONTACT_CREATED_BY::string, 'col28',GSM_USER_ID::string, 'col29',GSM_FIRST_NAME::string, 'col30',GSM_LAST_NAME::string,
        'col31',GSM_EMAIL::string, 'col32',GSM_PHONE::string, 'col33',GSM_CONTACT_LAST_UPDATE_BY::string,
        'col34',GSM_CONTACT_CREATED_BY::string, 'col35',ADDRESS1::string, 'col36',ADDRESS2::string, 'col37',ADDRESS3::string,
        'col38',ADDRESS4::string, 'col39',CITY::string, 'col40',STATE::string, 'col41',ZIPCODE::string,
        'col42',COUNTRY_CODE::string)::string)::BINARY as BIW_MD5_KEY  
FROM CORP_SDM
