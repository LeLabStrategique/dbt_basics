{{ config(
    materialized='table', 
    unique_key='post_id', 
    cluster_by=['date_day', 'organization_id'],
    schema='analytics',  
    tags=['marts', 'facts']
) }}

WITH posts_data AS (
    SELECT
        post_url AS post_id, 
        organization_id,
        
        DATE(created_timestamp) AS date_day, 
        
        -- CORRECTION DES NOMS DES MÉTRIQUES : suppression de 'total_'
        share_count,
        like_count,
        comment_count,
        click_count,
        impression_count
    FROM
        {{ ref('linkedin_pages__posts') }}
),

final AS (
    SELECT
        -- Clés
        post_id,
        organization_id,
        date_day,
        
        -- Mesures agrégées (Renommage propre)
        share_count,
        like_count,
        comment_count,
        click_count,
        impression_count,

        -- Calcul de l'engagement (Mesure dérivée)
        (share_count + like_count + comment_count) * 1.0 / NULLIF(impression_count, 0) AS engagement_rate

    FROM posts_data
    WHERE organization_id IS NOT NULL 
)

SELECT * FROM final