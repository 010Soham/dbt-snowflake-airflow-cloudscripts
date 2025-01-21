{{ config(
    pre_hook="ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = true",
    post_hook="GRANT SELECT ON TABLE {{ this }} TO ROLE TEST_DBT_ROLE"  
) }}

{{ config(
    tags=[var('TAG_DIMENSION')]
) }}

SELECT
ID
,ADB_ID
,ADB_SCORE
,ADB_VOTES
,ADB_POPULARITY
,TADB_SCORE
FROM
{{ source('nsx1', 'TITLES') }}
