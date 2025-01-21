{{ config(
    pre_hook="ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = true"
) }}

{{ config(
    tags=[var('TAG_DIMENSION')]
) }}

SELECT 
* 
FROM PROD.DBT_RAW.CTS
WHERE ROLE IN ('ACR','DRR')
