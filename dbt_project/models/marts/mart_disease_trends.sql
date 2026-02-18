-- Final analytical mart: County health profiles for reporting and visualization

{{ config(
    materialized='table'
) }}

WITH ranked_data AS (
    SELECT * FROM {{ ref('int_weekly_covid_aggregates') }}
),
state_context AS (
    SELECT * FROM {{ ref('int_state_covid_trends') }}
),
enriched_counties AS (
    SELECT
        r.state_code,
        r.state_name,
        r.county_name,
        r.county_fips,
        r.total_population,
        
        -- All health measures
        r.pct_uninsured,
        r.pct_diabetes,
        r.pct_current_smoker,
        r.pct_obesity,
        r.pct_arthritis,
        r.pct_heart_disease,
        r.pct_stroke,
        
        -- Relative to state average
        r.state_avg_diabetes,
        r.state_avg_obesity,
        r.state_avg_smoking,
        
        -- Health burden ranking within state (1 = healthiest, n = least healthy)
        r.diabetes_rank,
        r.obesity_rank,
        r.smoking_rank,
        
        -- Get state context
        s.num_counties,
        s.state_total_population,
        s.avg_pct_diabetes as state_avg_diabetes_agg,
        s.avg_pct_obesity as state_avg_obesity_agg,
        s.avg_pct_current_smoker as state_avg_smoking_agg,
        s.diabetes_disparity_range as state_diabetes_range,
        s.obesity_disparity_range as state_obesity_range,
        
        -- Flag counties with concerning health metrics
        CASE
            WHEN r.pct_diabetes > r.state_avg_diabetes + (s.diabetes_disparity_range * 0.1)
            THEN TRUE
            ELSE FALSE
        END as flag_high_diabetes,
        
        CASE
            WHEN r.pct_obesity > r.state_avg_obesity + (s.obesity_disparity_range * 0.1)
            THEN TRUE
            ELSE FALSE
        END as flag_high_obesity,
        
        -- Combined health burden score (simple: sum of deviations from state avg)
        ROUND(
            ABS(r.pct_diabetes - r.state_avg_diabetes) +
            ABS(r.pct_obesity - r.state_avg_obesity) +
            ABS(r.pct_current_smoker - r.state_avg_smoking),
            2
        ) as health_burden_score,
        
        -- Quality indicator
        CASE
            WHEN r.pct_diabetes IS NULL OR r.pct_obesity IS NULL OR r.pct_current_smoker IS NULL
            THEN 'Incomplete Data'
            WHEN r.total_population < 10000
            THEN 'Small Population'
            ELSE 'Valid'
        END as data_quality_status
    FROM ranked_data r
    LEFT JOIN state_context s
        ON r.state_code = s.state_code
)

SELECT
    state_code,
    state_name,
    county_name,
    county_fips,
    total_population,
    
    pct_uninsured,
    pct_diabetes,
    pct_current_smoker,
    pct_obesity,
    pct_arthritis,
    pct_heart_disease,
    pct_stroke,
    
    state_avg_diabetes,
    state_avg_obesity,
    state_avg_smoking,
    
    diabetes_rank,
    obesity_rank,
    smoking_rank,
    
    state_avg_diabetes_agg,
    state_avg_obesity_agg,
    state_avg_smoking_agg,
    state_diabetes_range,
    state_obesity_range,
    
    flag_high_diabetes,
    flag_high_obesity,
    
    health_burden_score,
    data_quality_status,
    
    CURRENT_TIMESTAMP as dbt_created_at,
    CURRENT_TIMESTAMP as dbt_updated_at
FROM enriched_counties
ORDER BY state_code, health_burden_score DESC, county_name
