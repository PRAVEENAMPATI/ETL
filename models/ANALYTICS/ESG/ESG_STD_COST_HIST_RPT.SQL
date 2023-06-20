{{ 
config
(
  description = 'EMISSIONS ALLOCATION REPORT VIEW ', 
  materialized = 'view', 
  schema = 'ESG', 
  tags = ['MART_ESG'],
  alias = 'STD_COST_HIST_RPT'
) 
}}

SELECT 
    STD_COST_HIST_KEY,
    PERIOD,
    SOURCE_TABLE,
    SOURCE_YM,
    PART_ID,
    PRODLINE_ID,
    TOTAL_STD_RTE,
    MATERIAL_STD_RTE,
    LABOR_STD_RTE,
    BURDEN_STD_RTE,
    OVERHEAD_STD_RTE,
    STG_OF_COMPLTN_CD,
    PAL4,
    PAL3,
    VALUED_FLG,
    BIW_INS_DTTM ,
    BIW_UPD_DTTM ,
    BIW_BATCH_ID,
    BIW_MD5_KEY
FROM 
    {{ref ('MART_ESG_STD_COST_HIST_RPT')}}