{{ config(
    materialized='table', 
    cluster_by=['organization_id', 'days_since_publication'],
    schema='analytics',  
    tags=['marts', 'analysis']
) }}

WITH daily_performance AS (
    SELECT
        post_id,
        organization_id,
        date_day,
        engagement_rate,
        click_count,
        comment_count
    FROM 
        {{ ref('mart_daily_post_performance') }} 
),

post_metadata AS (
    SELECT
        ugc_post_id,
        created_timestamp
    FROM
        {{ ref('linkedin_pages__posts') }} 
),

joined_analysis AS (
    SELECT
        t1.organization_id,
        t1.post_id,
        t1.date_day,
        
        -- Calcul du Jour après publication (âge du post)
        DATE_DIFF(t1.date_day, DATE(t2.created_timestamp), DAY) AS days_since_publication,
        
        t1.engagement_rate,
        t1.click_count,
        t1.comment_count
    FROM 
        daily_performance t1
    INNER JOIN 
        post_metadata t2 
        ON t1.post_id = t2.ugc_post_id 
    WHERE 
        -- Assure que nous analysons l'engagement A PARTIR du jour après la publication
        DATE_DIFF(t1.date_day, DATE(t2.created_timestamp), DAY) > 0
),

final_aggregation AS (
    SELECT
        organization_id,
        days_since_publication,
        
        -- Calcul des moyennes d'engagement pour chaque "âge" de publication
        AVG(engagement_rate) AS avg_engagement_rate,
        AVG(click_count) AS avg_daily_clicks,
        AVG(comment_count) AS avg_daily_comments,
        
        COUNT(DISTINCT post_id) AS total_posts_in_period
    FROM
        joined_analysis
    GROUP BY 1, 2
)

SELECT * FROM final_aggregation
ORDER BY organization_id, days_since_publication