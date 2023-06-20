{{ config(
  description = 'RPT_GL_GHG_EMISSIONS_ALLOC_COS view for Emissions Allocation', 
  materialized = 'view', 
  schema = 'ESG', 
  tags = ['MART_ESG'],
  alias = 'RPT_GL_GHG_EMISSIONS_ALLOC_COS'
) }} 

SELECT 
    PERIOD,
    BU,
    COS,
	BIW_INS_DTTM,
	BIW_UPD_DTTM,
	BIW_BATCH_ID
FROM {{source('STG_EBS_RPT_OWNER','RPT_GL_GHG_EMISSIONS_ALLOC_COS')}}