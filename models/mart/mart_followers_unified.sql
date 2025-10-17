{{
    config(
        materialized='table',
        unique_key=['row_id_composite', 'dimension'], 
        sort=['day', 'dimension', 'dimension_value'],
        tags=['mart', 'linkedin', 'followers']
    )
}}

-- Le nom du Project ID est rendu dynamique par la macro {{ target.database }}
{% set schema_name = 'STG_linkedin_company_pages' %}

{% set follower_sources = [
    {'suffix': 'association_type', 'dimension_name': 'association type', 'dimension_column': 'dim_association_type'},
    {'suffix': 'function', 'dimension_name': 'function', 'dimension_column': 'dim_function'},
    {'suffix': 'geo', 'dimension_name': 'geo', 'dimension_column': 'dim_geo'},
    {'suffix': 'geo_country', 'dimension_name': 'geo country', 'dimension_column': 'dim_geo_country'},
    {'suffix': 'industry', 'dimension_name': 'industry', 'dimension_column': 'dim_industry'},
    {'suffix': 'seniority', 'dimension_name': 'seniority', 'dimension_column': 'dim_seniority'},
    {'suffix': 'staff_count_range', 'dimension_name': 'staff count range', 'dimension_column': 'dim_staff_count_range'}
] %}

WITH unions AS (

{% for s in follower_sources %}
    
    SELECT
        _organization_entity_urn,
        extract_timestamp,
        day,

        -- 1. Clé composite
        CONCAT(
            'followers_',
            '{{ s.suffix }}',
            '_',
            TO_HEX(MD5(_organization_entity_urn || day))
        ) AS row_id_composite,

        -- 2. Valeur forcée pour la dimension (ex: 'function')
        '{{ s.dimension_name }}' AS dimension,

        -- 3. Sélection de la colonne de dimension spécifique (ex: dim_function)
        {{ s.dimension_column }} AS dimension_value,

        -- 4. Métriques
        follower_counts_organic_follower_count AS organic_followers,
        follower_counts_paid_follower_count AS paid_followers,
        
        -- 5. Colonne pour la traçabilité
        'stg_followers_by_{{ s.suffix }}' AS source_table_name

    FROM
        -- Référence dynamique : utilise le Project ID de la connexion actuelle et le nom de schéma fixe
        `{{ target.database }}`.{{ schema_name }}.stg_followers_by_{{ s.suffix }}

    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %}

)

SELECT
    _organization_entity_urn,
    day,
    dimension,
    dimension_value,
    organic_followers,
    paid_followers
FROM
    unions