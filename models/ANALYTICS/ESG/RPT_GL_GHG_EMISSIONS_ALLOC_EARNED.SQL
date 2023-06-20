{{ config(
  description = 'RPT_GL_GHG_EMISSIONS_ALLOC_EARNED view for Emissions Allocation', 
  materialized = 'view', 
  schema = 'ESG', 
  tags = ['MART_ESG'],
  alias = 'RPT_GL_GHG_EMISSIONS_ALLOC_EARNED'
) }} 

SELECT 
    PERIOD,
    MFG_PAL,
    SITE_GROUP,
    BU,
    EARNED,
	BIW_INS_DTTM,
	BIW_UPD_DTTM,
	BIW_BATCH_ID
FROM {{source('STG_EBS_RPT_OWNER','RPT_GL_GHG_EMISSIONS_ALLOC_EARNED')}}