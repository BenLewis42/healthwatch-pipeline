-- Intermediate model: State-level health profile comparison

{{ config(
    materialized='table'
) }}

WITH state_county_stats AS (
    SELECT
        state_code,
        state_name,
        county_fips,
        county_name,
        total_population,
        pct_uninsured,
        pct_diabetes,
        pct_obesity,
        pct_current_smoker,
        pct_heart_disease,
        pct_stroke,
        pct_arthritis,
        QUANTILE_CONT(pct_diabetes, 0.5) OVER (PARTITION BY state_code) as median_diabetes,
        QUANTILE_CONT(pct_obesity, 0.5) OVER (PARTITION BY state_code) as median_obesity
    FROM {{ ref('stg_covid_surveillance') }}
)

SELECT
    state_code,
    state_name,
    
    -- Count of counties per state
    COUNT(DISTINCT county_fips) as num_counties,
    
    -- Population stats
    SUM(total_population) as state_total_population,
    
    -- Average health metrics across all counties
    ROUND(AVG(pct_uninsured), 2) as avg_pct_uninsured,
    ROUND(AVG(pct_diabetes), 2) as avg_pct_diabetes,
    ROUND(AVG(pct_obesity), 2) as avg_pct_obesity,
    ROUND(AVG(pct_current_smoker), 2) as avg_pct_current_smoker,
    ROUND(AVG(pct_heart_disease), 2) as avg_pct_heart_disease,
    ROUND(AVG(pct_stroke), 2) as avg_pct_stroke,
    ROUND(AVG(pct_arthritis), 2) as avg_pct_arthritis,
    
    -- Min and max prevalence (health disparities)
    ROUND(MIN(pct_diabetes), 2) as min_pct_diabetes,
    ROUND(MAX(pct_diabetes), 2) as max_pct_diabetes,
    ROUND(MAX(pct_diabetes) - MIN(pct_diabetes), 2) as diabetes_disparity_range,
    
    ROUND(MIN(pct_obesity), 2) as min_pct_obesity,
    ROUND(MAX(pct_obesity), 2) as max_pct_obesity,
    ROUND(MAX(pct_obesity) - MIN(pct_obesity), 2) as obesity_disparity_range,
    
    ROUND(MIN(pct_current_smoker), 2) as min_pct_smoking,
    ROUND(MAX(pct_current_smoker), 2) as max_pct_smoking,
    ROUND(MAX(pct_current_smoker) - MIN(pct_current_smoker), 2) as smoking_disparity_range,
    
    -- Count counties with high disease burden
    SUM(CASE WHEN pct_diabetes >= median_diabetes THEN 1 ELSE 0 END) as counties_high_diabetes,
    SUM(CASE WHEN pct_obesity >= median_obesity THEN 1 ELSE 0 END) as counties_high_obesity,
    
    CURRENT_TIMESTAMP as dbt_created_at
FROM state_county_stats
GROUP BY state_code, state_name
ORDER BY state_code
