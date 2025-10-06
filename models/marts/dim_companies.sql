{{ config(
    materialized='table',
    unique_key='organization_id',
    cluster_by=['organization_id'],
    schema='analytics', 
    tags=['marts', 'dimensions']
) }}

WITH organizations AS (
    SELECT
        organization_id, 
        organization_name AS company_name -- SEULE COLONNE CONSERVÉE ET RENOMMÉE
        -- Toutes les autres colonnes ont été supprimées car elles n'existent pas dans le staging (vanity_name, url, industry_code, etc.)
    FROM
        {{ ref('stg_linkedin_pages__organization') }}
    WHERE
        organization_id IS NOT NULL 
)

SELECT * FROM organizations