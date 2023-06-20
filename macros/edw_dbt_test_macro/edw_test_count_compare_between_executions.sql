/*---------------------------------------------------------------------------
Below macro is build:
To compare the count of rows impacted from prior run to current
1. call macro edw_capture_count_base to capture execution rows impacted
2. get avg count between prior 7 execution and current .
Note: defualt is 7 you can change that using param_compare_against
Version     Date            Author          Description
-------     --------        -----------     ----------------------------------
1.0         Apr-03-2023      Kali D         Initial Version
---------------------------------------------------------------------------*/

{% test edw_test_count_compare_between_executions (model,param_compare_against="7")  %}
 {## NO NEED TO RUN BATCH PROCESS FOR DOC GENERATE##}
 {%- set v_table_name = var('V_EDL_DEFAULT_DB')+env_var('DBT_DEP_ENV')+'.'+'UTILITY_DBT.DBT_BIW_MODEL_EXECUTION_LOG' -%}

--step 1: call macro edw_capture_count_base to capture execution rows impacted
    {{edw_capture_count_base (model)}}
--step 2: get avg count between prior 7 days and compare with current day
 WITH PRIOR_EXECUTIONS AS 
            (select 
                    TABLE_DATABASE||'.'||TABLE_SCHEMA||'.'||TABLE_NAME AS MODEL_NAME,
                    ROW_IMPACTED
            from 
                {{v_table_name}}
            where 
                TABLE_DATABASE||'.'||TABLE_SCHEMA||'.'||TABLE_NAME= '{{model}}'
            qualify( ROW_NUMBER() OVER (PARTITION BY TABLE_DATABASE||'.'||TABLE_SCHEMA||'.'||TABLE_NAME ORDER BY MODEL_EXECUTION_DTTM DESC) BETWEEN 2 AND 1 + {{param_compare_against}} )
            )
        ,PRIOR_AVG AS 
            (select 
                    MODEL_NAME,
                    AVG(ROW_IMPACTED) AVG_COUNT
            from 
               PRIOR_EXECUTIONS
            GROUP BY 1
           )
        ,CURR_EXECUTIONS AS 
            (select 
                    TABLE_DATABASE||'.'||TABLE_SCHEMA||'.'||TABLE_NAME AS MODEL_NAME,
                    ROW_IMPACTED
            from 
                {{v_table_name}}
            where 
                TABLE_DATABASE||'.'||TABLE_SCHEMA||'.'||TABLE_NAME= '{{model}}'
               AND MODEL_EXECUTION_DTTM =   ( SELECT MAX(MODEL_EXECUTION_DTTM)
                                                FROM 
                                                {{v_table_name}}
                                                WHERE TABLE_DATABASE||'.'||TABLE_SCHEMA||'.'||TABLE_NAME= '{{model}}'
                                            )
            )
-- COMPARE WITH CURRENT
    SELECT 
        CURR.MODEL_NAME
    FROM CURR_EXECUTIONS CURR
    LEFT JOIN PRIOR_AVG BASE
        ON CURR.MODEL_NAME= BASE.MODEL_NAME
    {% if var('is_skip_count_test') %}
        where 1<>1 --skipping testing
    {% else %}
        where 1=1
    {% endif %}
    HAVING BASE.AVG_COUNT > ROW_IMPACTED 

{% endtest %}