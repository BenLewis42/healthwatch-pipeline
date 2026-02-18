-- Intermediate model: County health rankings within each state

{{ config(
    materialized='table'
) }}

WITH ranked_counties AS (
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
        
        -- Calculate state average for each measure
        AVG(pct_diabetes) OVER (PARTITION BY state_code) as state_avg_diabetes,
        AVG(pct_obesity) OVER (PARTITION BY state_code) as state_avg_obesity,
        AVG(pct_current_smoker) OVER (PARTITION BY state_code) as state_avg_smoking,
        
        -- Rank counties within state by disease prevalence
        ROW_NUMBER() OVER (
            PARTITION BY state_code
            ORDER BY pct_diabetes DESC
        ) as diabetes_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY state_code
            ORDER BY pct_obesity DESC
        ) as obesity_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY state_code
            ORDER BY pct_current_smoker DESC
        ) as smoking_rank
    FROM {{ ref('stg_covid_surveillance') }}
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
    
    -- Deviation from state average (positive = higher than average)
    ROUND(pct_diabetes - state_avg_diabetes, 2) as diabetes_deviation_from_state_avg,
    ROUND(pct_obesity - state_avg_obesity, 2) as obesity_deviation_from_state_avg,
    ROUND(pct_current_smoker - state_avg_smoking, 2) as smoking_deviation_from_state_avg,
    
    diabetes_rank,
    obesity_rank,
    smoking_rank,
    
    CURRENT_TIMESTAMP as dbt_created_at
FROM ranked_counties
ORDER BY state_code, county_name
