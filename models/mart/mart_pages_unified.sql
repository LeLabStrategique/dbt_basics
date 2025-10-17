{{
    config(
        materialized='table',
        unique_key=['row_id_composite', 'dimension'], 
        sort=['day', 'dimension', 'dimension_value'],
        tags=['mart', 'linkedin', 'pages', 'statistics']
    )
}}

-- Le Project ID est rendu dynamique par la macro {{ target.database }}
{% set schema_name = 'STG_linkedin_company_pages' %}
{% set prefix = 'stg_page_statistic_by_' %}

{% set page_sources = [
    {'suffix': 'function', 'dimension_name': 'function', 'dimension_column': 'dim_function'},
    {'suffix': 'geo', 'dimension_name': 'geo', 'dimension_column': 'dim_geo'},
    {'suffix': 'geo_country', 'dimension_name': 'geo country', 'dimension_column': 'dim_geo_country'},
    {'suffix': 'seniority', 'dimension_name': 'seniority', 'dimension_column': 'dim_seniority'},
    {'suffix': 'staff_count_range', 'dimension_name': 'staff count range', 'dimension_column': 'dim_staff_count_range'}
] %}

WITH unions AS (

{% for s in page_sources %}
    
    SELECT
        _organization_entity_urn,
        day,


        -- 2. Valeur forcée pour la dimension (ex: 'function')
        '{{ s.dimension_name }}' AS dimension,

        -- 3. Sélection de la colonne de dimension spécifique
        {{ s.dimension_column }} AS dimension_value,

        -- 4. Métriques de Page View (Liste complète)
        all_desktop_page_views,
        all_mobile_page_views,
        all_page_views,
        about_page_views,
        careers_page_views,
        products_page_views,
        jobs_page_views,
        people_page_views,
        overview_page_views,
        life_at_page_views,
        insights_page_views,
        mobile_careers_page_views,
        mobile_overview_page_views,
        mobile_jobs_page_views,
        mobile_life_at_page_views,
        mobile_insights_page_views,
        mobile_products_page_views,
        mobile_about_page_views,
        mobile_people_page_views,
        desktop_insights_page_views,
        desktop_careers_page_views,
        desktop_life_at_page_views,
        desktop_jobs_page_views,
        desktop_people_page_views,
        desktop_about_page_views,
        desktop_overview_page_views,
        desktop_products_page_views

    FROM
        -- Référence directe à la table BigQuery (Project ID dynamique)
        `{{ target.database }}`.{{ schema_name }}.{{ prefix }}{{ s.suffix }}

    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %}

)

SELECT *
FROM unions