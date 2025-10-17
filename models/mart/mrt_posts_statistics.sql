{{
    config(
        materialized='table',
        schema='MRT_linkedin_company_pages' 
    )
}}

WITH keyword_list AS (
  -- 1. Récupère la liste de tous les mots-clés de la table de dimension
  -- Utilise REF() car c'est maintenant un modèle DBT (dans models/config/dim_post_category_keyword.sql)
  SELECT
    STRING_AGG(keyword, '|') AS regex_keywords
  FROM
    {{ ref('dim_post_category_keyword') }} -- <-- CORRECTION ICI : ref() au lieu de source()
),

base_posts AS (
  -- 2. Récupère les données de base de la table de staging
  SELECT
    t1.day,
    t1.dim_post_id,
    t1.row_id,
    t1.title,
    LOWER(t1.title) AS post_content_lower,
    t1.ugc_post_urn,
    t1.share_statistic_id,
    t1.lifecycle_state,
    t1.visibility,
    t1.created_time,
    t1.last_modified_time,
    t1.first_published_at,
    t1.engagement_count,
    t1.share_count,
    t1.click_count,
    t1.like_count,
    t1.impression_count,
    t1.comment_count,
    t1._organization_entity_urn
  FROM
    {{ ref('stg_ugc_post_statistic') }} AS t1 -- Référence à la table de staging
  WHERE
    t1.title IS NOT NULL
),

classified_posts AS (
  -- 3. Classification, calcul de l'âge et jointure croisée
  SELECT
    t1.* EXCEPT(post_content_lower),
    -- Calcul de post_age
    DATE_DIFF(CURRENT_DATE(), DATE(t1.first_published_at), DAY) AS post_age,
    
    CASE 
        WHEN REGEXP_CONTAINS(t1.post_content_lower, kl.regex_keywords) THEN 'hr posts'
        ELSE 'brand posts'
    END AS post_category
  FROM
    base_posts AS t1
  CROSS JOIN
    keyword_list AS kl 
)

SELECT
    -- ************************************************************
    -- ** 1. CLÉS ET DIMENSIONS **
    -- ************************************************************
    t1.day AS stat_day_time,
    t1.dim_post_id AS dim_split_post_id,
    t1.row_id,
    
    --------------------------------------------------------------------------------------------------------------------
    -- Post_Intro
    --------------------------------------------------------------------------------------------------------------------
    SUBSTR(
        TRIM(
            COALESCE(
                REGEXP_EXTRACT(TRIM(t1.title), r'^(.*?[.?!,;:])'),
                TRIM(t1.title)
            )
        ),
        1,
        40
    ) AS post_intro,

    --------------------------------------------------------------------------------------------------------------------
    -- Post_Category
    --------------------------------------------------------------------------------------------------------------------
    t1.post_category, 

    --------------------------------------------------------------------------------------------------------------------
    -- Post_URL (URL Cliquable)
    --------------------------------------------------------------------------------------------------------------------
    CONCAT("https://www.linkedin.com/feed/update/", t1.ugc_post_urn) AS post_url,

    -- ************************************************************
    -- ** 2. SÉLECTION DES COLONNES RESTANTES & MÉTRIQUES **
    -- ************************************************************
    t1.title AS post_commentary,
    t1.share_statistic_id,
    t1.visibility,
    t1.lifecycle_state,
    t1.created_time,
    t1.last_modified_time,
    t1.first_published_at,

    -- MÉTRIQUES
    t1.post_age, 
    t1.engagement_count AS engagement, 
    t1.share_count,
    t1.click_count,
    t1.like_count,
    t1.impression_count,
    t1.comment_count,

    -- URNs
    t1._organization_entity_urn,
    t1.ugc_post_urn AS _share_entity_urn, 
    t1.ugc_post_urn

FROM
    classified_posts AS t1
ORDER BY
    t1.day DESC