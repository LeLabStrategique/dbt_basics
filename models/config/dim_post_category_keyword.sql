{{
    config(
        materialized='table',
        schema='CONFIG_linkedin_company_pages' 
    )
}}

SELECT
    keyword,
    'HR Posts' AS category
FROM
(
    SELECT 'next generation' AS keyword UNION ALL
    SELECT 'retail talent' AS keyword UNION ALL
    SELECT 'celebrate our talent' AS keyword UNION ALL
    SELECT 'our committed journey' AS keyword UNION ALL
    SELECT 'digital craftsmanship' AS keyword UNION ALL
    SELECT 'client care' AS keyword UNION ALL
    SELECT 'shape the future of design' AS keyword UNION ALL
    SELECT 'celebrating our talents' AS keyword UNION ALL
    SELECT 'louis vuitton celebrates' AS keyword UNION ALL
    SELECT 'celebrates talent' AS keyword UNION ALL
    SELECT 'metiers' AS keyword UNION ALL
    SELECT 'people for wildlife' AS keyword UNION ALL
    SELECT 'odyssee des savoir-faire' AS keyword UNION ALL
    SELECT 'you&me 2024' AS keyword UNION ALL
    SELECT 'celebrating excellence in operations' AS keyword UNION ALL
    SELECT 'clash of titans' AS keyword UNION ALL
    SELECT 'skyline of shanghai' AS keyword
)