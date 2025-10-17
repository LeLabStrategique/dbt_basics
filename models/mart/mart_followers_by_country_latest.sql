{{
    config(
        materialized='table',
        unique_key=['country'], 
        sort=['country'],
        tags=['mart', 'linkedin', 'followers', 'latest']
    )
}}

-- Définition des schémas et tables
{% set staging_schema = 'STG_linkedin_company_pages' %}
{% set config_schema = 'CONFIG_linkedin_company_pages' %}
{% set project_id = target.database %}

WITH follower_countries AS (
    -- 1. Sélectionne les données de followers par pays
    SELECT
        _organization_entity_urn,
        day,
        dim_geo_country AS country, -- Pays de la table de staging (clé de jointure)
        follower_counts_organic_follower_count AS organic_followers,
        follower_counts_paid_follower_count AS paid_followers
    FROM
        -- Référence directe à la table de staging (pour éviter les problèmes de source.yml)
        `{{ project_id }}`.{{ staging_schema }}.stg_followers_by_geo_country
),

ranked_followers AS (
    -- 2. Identifie la ligne la plus récente pour chaque pays
    SELECT
        *,
        -- Utilise ROW_NUMBER pour classer les lignes. Le plus petit numéro est la date MAX (dernière date).
        ROW_NUMBER() OVER (
            PARTITION BY country
            ORDER BY day DESC
        ) AS rn
    FROM
        follower_countries
),

latest_data AS (
    -- 3. Filtre pour ne conserver que la ligne la plus récente (rn = 1)
    SELECT
        country,
        day AS latest_day,
        SUM(organic_followers) AS latest_organic_followers,
        SUM(paid_followers) AS latest_paid_followers
    FROM
        ranked_followers
    WHERE
        rn = 1
    GROUP BY 1, 2 -- Agrégation si plusieurs lignes ont la même date max (peu probable ici, mais bonne pratique)
),

geo_dim AS (
    -- 4. Importe la table de dimension géographique
    SELECT
        country,
        continent,
        Zone_LV,
        Region_LV,
        country_LV
    FROM
        -- Référence directe à la table de dimension (pour éviter les problèmes de source.yml)
        `{{ project_id }}`.{{ config_schema }}.dim_country_region_LV
)

-- 5. Jointure finale
SELECT
    t1.latest_day,
    t1.country,
    t2.continent,
    t2.Zone_LV,
    t2.Region_LV,
    t2.country_LV,
    t1.latest_organic_followers,
    t1.latest_paid_followers
FROM
    latest_data t1
INNER JOIN
    geo_dim t2
    -- Le mapping est : stg_followers_by_geo_country.dim_geo_country = dim_country_region_LV.country
    ON t1.country = t2.country