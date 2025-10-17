{{
    config(
        materialized='table',
        schema='MRT_linkedin_company_pages' 
    )
}}

-- Le modèle mrt_post_category_intro_summary doit dépendre du Mart principal (mrt_posts_statistics)
-- S'il dépendait d'une source, c'est que la logique du Mart principal avait été copiée.

-- **HYPOTHÈSE DE CORRECTION** : Ce modèle doit agréger le Mart principal.
-- Si le Mart principal a été renommé, c'est 'mrt_posts_statistics'.

SELECT
    t1.post_intro,
    t1.post_category,
    
    -- Compte le nombre d'identifiants de publication uniques
    COUNT(DISTINCT t1.ugc_post_urn) AS unique_post_count

FROM
    -- **CORRECTION CLÉ** : Référence au Mart principal, pas à la source de dimension.
    {{ ref('mrt_posts_statistics') }} AS t1 

GROUP BY
    t1.post_intro,
    t1.post_category
ORDER BY
    unique_post_count DESC