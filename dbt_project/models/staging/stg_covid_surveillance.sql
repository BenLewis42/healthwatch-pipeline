-- Staging model to clean CDC PLACES county health data
-- Standardize column names, cast data types, and handle nulls

{{ config(
    materialized='view'
) }}

SELECT
    stateabbr as state_code,
    statedesc as state_name,
    countyname as county_name,
    countyfips as county_fips,
    CAST(totalpopulation AS INTEGER) as total_population,
    CAST(totalpop18plus AS INTEGER) as adult_population,
    
    -- Health Insurance Access & Care
    CAST(access2_crudeprev AS DECIMAL(5,2)) as pct_uninsured,
    CAST(checkup_crudeprev AS DECIMAL(5,2)) as pct_healthcare_checkup,
    
    -- Chronic Disease Prevalence
    CAST(arthritis_crudeprev AS DECIMAL(5,2)) as pct_arthritis,
    CAST(cancer_crudeprev AS DECIMAL(5,2)) as pct_cancer,
    CAST(copd_crudeprev AS DECIMAL(5,2)) as pct_copd,
    CAST(diabetes_crudeprev AS DECIMAL(5,2)) as pct_diabetes,
    CAST(bphigh_crudeprev AS DECIMAL(5,2)) as pct_hypertension,
    CAST(stroke_crudeprev AS DECIMAL(5,2)) as pct_stroke,
    CAST(chd_crudeprev AS DECIMAL(5,2)) as pct_heart_disease,
    CAST(casthma_crudeprev AS DECIMAL(5,2)) as pct_asthma,
    CAST(depression_crudeprev AS DECIMAL(5,2)) as pct_depression,
    
    -- Health Behaviors & Risk Factors
    CAST(binge_crudeprev AS DECIMAL(5,2)) as pct_binge_drinking,
    CAST(csmoking_crudeprev AS DECIMAL(5,2)) as pct_current_smoker,
    CAST(obesity_crudeprev AS DECIMAL(5,2)) as pct_obesity,
    CAST(lpa_crudeprev AS DECIMAL(5,2)) as pct_low_physical_activity,
    
    -- Mental & Physical Health
    CAST(mhlth_crudeprev AS DECIMAL(5,2)) as pct_mental_health_days,
    CAST(phlth_crudeprev AS DECIMAL(5,2)) as pct_physical_health_days,
    CAST(ghlth_crudeprev AS DECIMAL(5,2)) as pct_general_health_fair_poor,
    
    -- Social Factors
    CAST(isolation_crudeprev AS DECIMAL(5,2)) as pct_social_isolation,
    CAST(foodinsecu_crudeprev AS DECIMAL(5,2)) as pct_food_insecurity,
    CAST(housinsecu_crudeprev AS DECIMAL(5,2)) as pct_housing_insecurity,
    CAST(disability_crudeprev AS DECIMAL(5,2)) as pct_any_disability,
    
    -- Metadata
    loaded_at as data_loaded_timestamp,
    source_file as source_filename,
    extracted_at,
    source,
    dataset_id,
    CURRENT_TIMESTAMP as dbt_created_at
FROM {{ source('raw', 'places_county') }}
WHERE stateabbr IS NOT NULL
    AND countyname IS NOT NULL
ORDER BY state_code, county_name
