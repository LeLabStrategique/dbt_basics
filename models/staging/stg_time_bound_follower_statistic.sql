{{
    config(
        materialized='incremental',
        unique_key='day', 
        incremental_strategy='merge', 
        on_schema_change='fail'
    )
}}

WITH source_data AS (

    SELECT
        -- Colonnes de la source f (explicites)
        f.organization_entity, -- URN brut
        f.day AS day_raw_timestamp, 
        f._fivetran_synced,
        
        -- Colonnes de FAITS (GAINS)
        f.follower_gains_organic_follower_gain, 
        f.follower_gains_paid_follower_gain,
        
        -- Colonnes temporelles calculées
        TIMESTAMP(f._fivetran_synced) AS extract_timestamp_temp, 
        CAST(f.day AS DATE) AS day_format,
        CAST(DATE(f.day) AS STRING FORMAT 'YYYY-MM-DD') AS day_format_string
        
    FROM 
        {{ source('fivetran_linkedin', 'time_bound_follower_statistic') }} AS f

    {% if is_incremental() %}
        WHERE TIMESTAMP(f._fivetran_synced) > (SELECT MAX(extract_timestamp) FROM {{ this }})
    {% endif %}

),

deduplication AS (
    SELECT
        -- Sélection explicite des colonnes nécessaires pour le SELECT final
        s.organization_entity, -- *** INCLUS MAINTENANT ***
        s.day_raw_timestamp,
        s.extract_timestamp_temp,
        s.day_format,
        s.day_format_string,
        s.follower_gains_organic_follower_gain, 
        s.follower_gains_paid_follower_gain,
        
        -- Construction de la clé analytique (row_id)
        CONCAT(
            CAST(DATE(s.extract_timestamp_temp) AS STRING FORMAT 'YYYY-MM-DD'), 
            '_', 
            s.day_format_string
        ) AS row_id,

        ROW_NUMBER() OVER (
            PARTITION BY 
                s.day_format
            ORDER BY s.extract_timestamp_temp DESC
        ) AS rank_by_recency

    FROM 
        source_data AS s
)

SELECT
    
    -- ************************************************************
    -- ** 1. STRUCTURE DE COLONNES DEMANDÉE **
    -- ************************************************************
    
    -- 1. URN (Renommage final)
    d.organization_entity AS _organization_entity_urn,
    
    -- 2. ROW_ID
    d.row_id,
    
    -- 3. TIMESTAMP de la synchro
    d.extract_timestamp_temp AS extract_timestamp,
    
    -- 4. DIM_DAY (TIMESTAMP brut du jour d'agrégation)
    d.day_raw_timestamp AS dim_day, 
    
    -- 5. DIM_DAY_ID
    CAST(NULL AS STRING) AS dim_day_id, 
    
    -- 6. DAY (Date d'agrégation au format YYYY-MM-DD)
    d.day_format AS day, 
    
    -- ************************************************************
    -- ** 2. COLONNES DE FAITS (Métriques de GAINS) **
    -- ************************************************************
    
    d.follower_gains_organic_follower_gain,
    d.follower_gains_paid_follower_gain
    
FROM 
    deduplication AS d
WHERE
    d.rank_by_recency = 1