{{ config(
    materialized='incremental',
    unique_key=['date_day', 'post_id'],
    cluster_by=['date_day', 'organization_id'],
    schema='analytics',  
    tags=['marts', 'performance']
) }}

WITH share_statistics AS (
    -- Ce CTE doit récupérer les données du Mart fact_share_statistics, ou directement de la table de staging si vous n'avez pas de Mart intermédiaire.
    -- Basé sur votre structure, fact_share_statistics semble être le bon Mart.
    SELECT
        date_day,
        ugc_post_id AS post_id, -- J'utilise ugc_post_id comme clé de lien
        organization_id,
        impression_count,
        like_count,
        comment_count,
        share_count,
        click_count,
        engagement_rate
    FROM
        {{ ref('fact_share_statistics') }} -- Remplacez par le nom de votre modèle de performance quotidienne (Fact)
    {% if is_incremental() %}
    -- Filtre incrémental (si fact_share_statistics est aussi incrémental)
    WHERE date_day >= date_sub(current_date(), interval 7 day)
    {% endif %}
)

SELECT * FROM share_statistics