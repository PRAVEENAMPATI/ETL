/*---------------------------------------------------------------------------
Command to run model:
--dbt build --full-refresh --select +MART_SALES_LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT+
--dbt build --select MART_SALES_LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT 
--dbt build --select MART_SALES_LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT+ --vars 'is_one_time_load: True'

Version     Date            Author              Description
-------     --------        -----------         ----------------------------------
1.0         03/20/2023      Vinay Subramanian    Initial Version
---------------------------------------------------------------------------*/
{################# EDW Job Template Variables #################}
{%-set v_pk_list = ['LTSA_DEMAND_MANAGEMENT_WEEKLY_KEY' ]-%}
{%-set v_house_keeping_column = ['BIW_INS_DTTM','BIW_UPD_DTTM','BIW_BATCH_ID','BIW_MD5_KEY']-%}
{%-set v_all_column_list =  edw_get_column_list( ref('ETL_MART_SALES_LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT') ) -%}
{%-set v_update_column_list =  edw_get_quoted_column_list( ref('ETL_MART_SALES_LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT') ,v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%-set v_with_select_column_list =  edw_get_column_list_with_prefix( ref('ETL_MART_SALES_LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT'),'stg.') -%}
--DBT Variable
--SELECT {{v_all_column_list}}
--SELECT {{v_update_column_list}}
--select {{v_with_select_column_list}}
{################# Batch control insert and update SQL #################}
{%- set v_dbt_job_name = 'DBT_MART_SALES_LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT'-%}
-- Step 1 Batch process info
{%- set v_watermark = edw_batch_control(v_dbt_job_name,config.get('schema'),config.get('alias') ,config.get('tags'),config.get('materialized') ) -%}
{%- set V_LWM = v_watermark[0] -%}
{%- set V_HWM = v_watermark[1] -%}
{%- set V_START_DTTM = v_watermark[2] -%}
{%- set V_BIW_BATCH_ID = v_watermark[3] -%}
{%- set v_sql_upd_success_batch = "CALL UTILITY.EDW_BATCH_SUCCESS_PROC('"~v_dbt_job_name~"')" -%}

{{
    config(
         description = 'Building table CUSTOMER dimension for sales mart '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT'
        ,unique_key= v_pk_list
        ,tags ='MART_SALES_LTSA'
        ,merge_update_columns = ['SNAPSHOT_WEEK_KEY', 'PART_KEY', 'CORPORATION_KEY', 'YEAR_KEY', 'MONTH_KEY',
                                'PART_ID', 'CORPORATION_CODE', 'CURRENT_MONTH_KEY', 'FRZ_MONTH_KEY', 'CRSD_QUANTITY', 'BILL_QUANTITY',
                                'MRSD_QUANTITY', 'BB_QUANTITY', 'MRSD_BB_QUANTITY', 'WORKING_SALES_FCST_QUANTITY', 'CONSENSUS_FCST_QUANTITY',
                                'CUR_LTSA_LIA_QUANTITY', 'LTSA_POR_QUANTITY', 'LTSA_ORIG_QUANTITY', 'LTSA_STRETCH_QUANTITY', 'CRSD_AMOUNT', 'BILL_AMOUNT',
                                'MRSD_AMOUNT', 'BB_AMOUNT', 'MRSD_BB_AMOUNT', 'WORKING_SALES_FCST_AMOUNT', 'CONSENSUS_FCST_AMOUNT',
                                'CUR_LTSA_LIA_AMOUNT', 'LTSA_POR_AMOUNT', 'LTSA_ORIG_AMOUNT', 'LTSA_STRETCH_AMOUNT', 'NEW_LTSA_LIA_QUANTITY',
                                'NEW_LTSA_LIA_AMOUNT', 'PART_CORP_ASP', 'PART_CORP_GROSS_ASP', 'PART_CORP_CONTRACT_ASP', 'CUME_BB_QUANTITY',
                                'CUME_LTSA_POR_QUANTITY', 'CUME_LTSA_ORIG_QUANTITY', 'CUME_LTSA_STRETCH_QUANTITY', 'CUME_NEW_LTSA_QUANTITY',
                                'CUME_MRSD_BB_QUANTITY', 'CUME_CUR_LTSA_QUANTITY', 'CUME_BB_AMOUNT', 'CUME_LTSA_POR_AMOUNT', 'CUME_LTSA_ORIG_AMOUNT',
                                'CUME_LTSA_STRETCH_AMOUNT', 'CUME_NEW_LTSA_AMOUNT', 'CUME_MRSD_BB_AMOUNT', 'CUME_CUR_LTSA_AMOUNT', 'AGREEMENT_ID', 'GROUP_ID',
                                'IS_REVIEW_STATUS', 'GRP_CUME_BB_QUANTITY', 'GRP_CUME_LTSA_POR_QUANTITY', 'GRP_CUME_LTSA_ORIG_QUANTITY',
                                'GRP_CUME_LTSA_STRETCH_QUANTITY', 'GRP_CUME_NEW_LTSA_QUANTITY', 'GRP_CUME_MRSD_BB_QUANTITY', 'GRP_CUME_CUR_LTSA_QUANTITY',
                                'GRP_CUME_BB_AMOUNT', 'GRP_CUME_LTSA_POR_AMOUNT', 'GRP_CUME_LTSA_ORIG_AMOUNT', 'GRP_CUME_LTSA_STRETCH_AMOUNT',
                                'GRP_CUME_NEW_LTSA_AMOUNT', 'GRP_CUME_MRSD_BB_AMOUNT', 'GRP_CUME_CUR_LTSA_AMOUNT', 'BACKLOG_COMPLIANCE_QUANTITY_PCT',
                                'BACKLOG_COMPLIANCE_QUANTITY_GAP', 'SUPPLY_COMPLIANCE_QUANTITY_PCT', 'SUPPLY_COMPLIANCE_QUANTITY_GAP',
                                'BACKLOG_COMPLIANCE_AMOUNT_PCT', 'BACKLOG_COMPLIANCE_AMOUNT_GAP', 'SUPPLY_COMPLIANCE_AMOUNT_PCT', 'SUPPLY_COMPLIANCE_AMOUNT_GAP',
                                'GRP_BACKLOG_COMPLIANCE_QUANTITY_PCT', 'GRP_BACKLOG_COMPLIANCE_QUANTITY_GAP', 'GRP_SUPPLY_COMPLIANCE_QUANTITY_PCT',
                                'GRP_SUPPLY_COMPLIANCE_QUANTITY_GAP', 'GRP_BACKLOG_COMPLIANCE_AMOUNT_PCT', 'GRP_BACKLOG_COMPLIANCE_AMOUNT_GAP',
                                'GRP_SUPPLY_COMPLIANCE_AMOUNT_PCT', 'GRP_SUPPLY_COMPLIANCE_AMOUNT_GAP', 'KPI_QUANTITY', 'KPI_AMOUNT', 'GRP_KPI_QUANTITY',
                                'GRP_KPI_AMOUNT', 'SUPPLY_ENTITLEMENT_QUANTITY', 'SUPPLY_ENTITLEMENT_AMOUNT', 'GRP_SUPPLY_ENTITLEMENT_QUANTITY',
                                'GRP_SUPPLY_ENTITLEMENT_AMOUNT', 'BIW_UPD_DTTM', 'BIW_BATCH_ID', 'BIW_MD5_KEY']


		,post_hook= [v_sql_upd_success_batch]	
        )
}}


SELECT
    LTSA_DEMAND_MANAGEMENT_WEEKLY_KEY,
    SNAPSHOT_WEEK_KEY,
    PART_KEY,
    CORPORATION_KEY,
    YEAR_KEY,
    MONTH_KEY,
    PART_ID,
    CORPORATION_CODE,
    CURRENT_MONTH_KEY,
    FRZ_MONTH_KEY,
    CRSD_QUANTITY,
    BILL_QUANTITY,
    MRSD_QUANTITY,
    BB_QUANTITY,
    MRSD_BB_QUANTITY,
    WORKING_SALES_FCST_QUANTITY,
    CONSENSUS_FCST_QUANTITY,
    CUR_LTSA_LIA_QUANTITY,
    LTSA_POR_QUANTITY,
    LTSA_ORIG_QUANTITY,
    LTSA_STRETCH_QUANTITY,
    CRSD_AMOUNT,
    BILL_AMOUNT,
    MRSD_AMOUNT,
    BB_AMOUNT,
    MRSD_BB_AMOUNT,
    WORKING_SALES_FCST_AMOUNT,
    CONSENSUS_FCST_AMOUNT,
    CUR_LTSA_LIA_AMOUNT,
    LTSA_POR_AMOUNT,
    LTSA_ORIG_AMOUNT,
    LTSA_STRETCH_AMOUNT,
    NEW_LTSA_LIA_QUANTITY,
    NEW_LTSA_LIA_AMOUNT,
    PART_CORP_ASP,
    PART_CORP_GROSS_ASP,
    PART_CORP_CONTRACT_ASP,
    CUME_BB_QUANTITY,
    CUME_LTSA_POR_QUANTITY,
    CUME_LTSA_ORIG_QUANTITY,
    CUME_LTSA_STRETCH_QUANTITY,
    CUME_NEW_LTSA_QUANTITY,
    CUME_MRSD_BB_QUANTITY,
    CUME_CUR_LTSA_QUANTITY,
    CUME_BB_AMOUNT,
    CUME_LTSA_POR_AMOUNT,
    CUME_LTSA_ORIG_AMOUNT,
    CUME_LTSA_STRETCH_AMOUNT,
    CUME_NEW_LTSA_AMOUNT,
    CUME_MRSD_BB_AMOUNT,
    CUME_CUR_LTSA_AMOUNT,
    AGREEMENT_ID,
    GROUP_ID,
    IS_REVIEW_STATUS,
    GRP_CUME_BB_QUANTITY,
    GRP_CUME_LTSA_POR_QUANTITY,
    GRP_CUME_LTSA_ORIG_QUANTITY,
    GRP_CUME_LTSA_STRETCH_QUANTITY,
    GRP_CUME_NEW_LTSA_QUANTITY,
    GRP_CUME_MRSD_BB_QUANTITY,
    GRP_CUME_CUR_LTSA_QUANTITY,
    GRP_CUME_BB_AMOUNT,
    GRP_CUME_LTSA_POR_AMOUNT,
    GRP_CUME_LTSA_ORIG_AMOUNT,
    GRP_CUME_LTSA_STRETCH_AMOUNT,
    GRP_CUME_NEW_LTSA_AMOUNT,
    GRP_CUME_MRSD_BB_AMOUNT,
    GRP_CUME_CUR_LTSA_AMOUNT,
    BACKLOG_COMPLIANCE_QUANTITY_PCT,
    BACKLOG_COMPLIANCE_QUANTITY_GAP,
    SUPPLY_COMPLIANCE_QUANTITY_PCT,
    SUPPLY_COMPLIANCE_QUANTITY_GAP,
    BACKLOG_COMPLIANCE_AMOUNT_PCT,
    BACKLOG_COMPLIANCE_AMOUNT_GAP,
    SUPPLY_COMPLIANCE_AMOUNT_PCT,
    SUPPLY_COMPLIANCE_AMOUNT_GAP,
    GRP_BACKLOG_COMPLIANCE_QUANTITY_PCT,
    GRP_BACKLOG_COMPLIANCE_QUANTITY_GAP,
    GRP_SUPPLY_COMPLIANCE_QUANTITY_PCT,
    GRP_SUPPLY_COMPLIANCE_QUANTITY_GAP,
    GRP_BACKLOG_COMPLIANCE_AMOUNT_PCT,
    GRP_BACKLOG_COMPLIANCE_AMOUNT_GAP,
    GRP_SUPPLY_COMPLIANCE_AMOUNT_PCT,
    GRP_SUPPLY_COMPLIANCE_AMOUNT_GAP,
    KPI_QUANTITY,
    KPI_AMOUNT,
    GRP_KPI_QUANTITY,
    GRP_KPI_AMOUNT,
    SUPPLY_ENTITLEMENT_QUANTITY,
    SUPPLY_ENTITLEMENT_AMOUNT,
    GRP_SUPPLY_ENTITLEMENT_QUANTITY,
    GRP_SUPPLY_ENTITLEMENT_AMOUNT,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID,
    BIW_MD5_KEY
FROM 
{{ref('ETL_MART_SALES_LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT')}}

{% if not ( is_incremental()) or var('is_one_time_load') %} 
UNION ALL
SELECT
    LTSA_DEMAND_MANAGEMENT_WEEKLY_KEY,
    SNAPSHOT_WEEK_KEY,
    PART_KEY,
    CORPORATION_KEY,
    YEAR_KEY,
    MONTH_KEY,
    PART_ID,
    CORPORATION_CODE,
    CURRENT_MONTH_KEY,
    FRZ_MONTH_KEY,
    CRSD_QUANTITY,
    BILL_QUANTITY,
    MRSD_QUANTITY,
    BB_QUANTITY,
    MRSD_BB_QUANTITY,
    WORKING_SALES_FCST_QUANTITY,
    CONSENSUS_FCST_QUANTITY,
    CUR_LTSA_LIA_QUANTITY,
    LTSA_POR_QUANTITY,
    LTSA_ORIG_QUANTITY,
    LTSA_STRETCH_QUANTITY,
    CRSD_AMOUNT,
    BILL_AMOUNT,
    MRSD_AMOUNT,
    BB_AMOUNT,
    MRSD_BB_AMOUNT,
    WORKING_SALES_FCST_AMOUNT,
    CONSENSUS_FCST_AMOUNT,
    CUR_LTSA_LIA_AMOUNT,
    LTSA_POR_AMOUNT,
    LTSA_ORIG_AMOUNT,
    LTSA_STRETCH_AMOUNT,
    NEW_LTSA_LIA_QUANTITY,
    NEW_LTSA_LIA_AMOUNT,
    PART_CORP_ASP,
    PART_CORP_GROSS_ASP,
    PART_CORP_CONTRACT_ASP,
    CUME_BB_QUANTITY,
    CUME_LTSA_POR_QUANTITY,
    CUME_LTSA_ORIG_QUANTITY,
    CUME_LTSA_STRETCH_QUANTITY,
    CUME_NEW_LTSA_QUANTITY,
    CUME_MRSD_BB_QUANTITY,
    CUME_CUR_LTSA_QUANTITY,
    CUME_BB_AMOUNT,
    CUME_LTSA_POR_AMOUNT,
    CUME_LTSA_ORIG_AMOUNT,
    CUME_LTSA_STRETCH_AMOUNT,
    CUME_NEW_LTSA_AMOUNT,
    CUME_MRSD_BB_AMOUNT,
    CUME_CUR_LTSA_AMOUNT,
    AGREEMENT_ID,
    GROUP_ID,
    IS_REVIEW_STATUS,
    GRP_CUME_BB_QUANTITY,
    GRP_CUME_LTSA_POR_QUANTITY,
    GRP_CUME_LTSA_ORIG_QUANTITY,
    GRP_CUME_LTSA_STRETCH_QUANTITY,
    GRP_CUME_NEW_LTSA_QUANTITY,
    GRP_CUME_MRSD_BB_QUANTITY,
    GRP_CUME_CUR_LTSA_QUANTITY,
    GRP_CUME_BB_AMOUNT,
    GRP_CUME_LTSA_POR_AMOUNT,
    GRP_CUME_LTSA_ORIG_AMOUNT,
    GRP_CUME_LTSA_STRETCH_AMOUNT,
    GRP_CUME_NEW_LTSA_AMOUNT,
    GRP_CUME_MRSD_BB_AMOUNT,
    GRP_CUME_CUR_LTSA_AMOUNT,
    BACKLOG_COMPLIANCE_QUANTITY_PCT,
    BACKLOG_COMPLIANCE_QUANTITY_GAP,
    SUPPLY_COMPLIANCE_QUANTITY_PCT,
    SUPPLY_COMPLIANCE_QUANTITY_GAP,
    BACKLOG_COMPLIANCE_AMOUNT_PCT,
    BACKLOG_COMPLIANCE_AMOUNT_GAP,
    SUPPLY_COMPLIANCE_AMOUNT_PCT,
    SUPPLY_COMPLIANCE_AMOUNT_GAP,
    GRP_BACKLOG_COMPLIANCE_QUANTITY_PCT,
    GRP_BACKLOG_COMPLIANCE_QUANTITY_GAP,
    GRP_SUPPLY_COMPLIANCE_QUANTITY_PCT,
    GRP_SUPPLY_COMPLIANCE_QUANTITY_GAP,
    GRP_BACKLOG_COMPLIANCE_AMOUNT_PCT,
    GRP_BACKLOG_COMPLIANCE_AMOUNT_GAP,
    GRP_SUPPLY_COMPLIANCE_AMOUNT_PCT,
    GRP_SUPPLY_COMPLIANCE_AMOUNT_GAP,
    KPI_QUANTITY,
    KPI_AMOUNT,
    GRP_KPI_QUANTITY,
    GRP_KPI_AMOUNT,
    SUPPLY_ENTITLEMENT_QUANTITY,
    SUPPLY_ENTITLEMENT_AMOUNT,
    GRP_SUPPLY_ENTITLEMENT_QUANTITY,
    GRP_SUPPLY_ENTITLEMENT_AMOUNT,
    BIW_INS_DTTM,
    BIW_UPD_DTTM,
    BIW_BATCH_ID,
    BIW_MD5_KEY
FROM 
{{ref('ETL_MART_SALES_LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT_HISTORY_ONE_TIME_LOAD')}}
WHERE SNAPSHOT_WEEK_KEY NOT IN (SELECT SNAPSHOT_WEEK_KEY FROM  {{ref('ETL_MART_SALES_LTSA_DEMAND_MANAGEMENT_WEEKLY_FACT')}} )
    {% if is_incremental() %}
    AND SNAPSHOT_WEEK_KEY  NOT IN ( SELECT SNAPSHOT_WEEK_KEY FROM {{ this }})
    {% endif %}
{%endif%}