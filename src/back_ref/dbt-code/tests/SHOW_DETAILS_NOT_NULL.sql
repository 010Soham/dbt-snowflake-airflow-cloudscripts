{{ config(
    tags=[var('TAG_TEST')]
) }}

SELECT * FROM {{ ref('MOS_SERIES_SHARE') }} WHERE CATEGORIES IS NULL
