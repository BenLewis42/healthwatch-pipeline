-- Staging model for detailed disease breakdown
-- Unpivots health measures for analysis by specific indicators

{{ config(
    materialized='view'
) }}

SELECT
    state_code,
    state_name,
    county_name,
    county_fips,
    total_population,
    'Uninsured' as health_measure,
    pct_uninsured as prevalence_percent
FROM {{ ref('stg_covid_surveillance') }}
WHERE pct_uninsured IS NOT NULL

UNION ALL

SELECT
    state_code,
    state_name,
    county_name,
    county_fips,
    total_population,
    'Diabetes' as health_measure,
    pct_diabetes as prevalence_percent
FROM {{ ref('stg_covid_surveillance') }}
WHERE pct_diabetes IS NOT NULL

UNION ALL

SELECT
    state_code,
    state_name,
    county_name,
    county_fips,
    total_population,
    'Current Smoker' as health_measure,
    pct_current_smoker as prevalence_percent
FROM {{ ref('stg_covid_surveillance') }}
WHERE pct_current_smoker IS NOT NULL

UNION ALL

SELECT
    state_code,
    state_name,
    county_name,
    county_fips,
    total_population,
    'Obesity' as health_measure,
    pct_obesity as prevalence_percent
FROM {{ ref('stg_covid_surveillance') }}
WHERE pct_obesity IS NOT NULL
