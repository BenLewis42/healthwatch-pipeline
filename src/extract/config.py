"""Configuration for CDC PLACES API.

CDC PLACES (Population Level Analysis and Community Estimates) provides
model-based health estimates at county level for 36+ health measures.
"""

import os
from dotenv import load_dotenv
from typing import Optional, List

load_dotenv()

# CDC PLACES API Configuration
CDC_BASE_URL = "https://data.cdc.gov/resource"

# Socrata Dataset IDs
# County-level GIS format: All health measures in one row per county
COUNTY_GIS_DATASET = "d3i6-k6z5"

# County-level Open Data format: One measure per row (more rows, easier filtering)
COUNTY_OPENDATA_DATASET = "swc5-untb"

# Authentication (optional but recommended for better rate limits)
# Get token from: https://data.cdc.gov/ → Profile → Developer Settings
APP_TOKEN: Optional[str] = os.getenv("CDC_APP_TOKEN", None)

# API Configuration
DEFAULT_LIMIT = 50000  # Max records per API request
DEFAULT_TIMEOUT = 30   # Request timeout in seconds
MAX_RETRIES = 3
RETRY_DELAY = 2  # Seconds between retries

# Data Configuration
RAW_DATA_PATH = "data/raw"

# Health measures of interest (customize as needed)
PRIORITY_MEASURES = [
    "ACCESS2",      # No health insurance
    "ARTHRITIS",    # Arthritis prevalence
    "BINGE",        # Binge drinking
    "COPD",         # COPD prevalence
    "CSMOKING",     # Current smoking
    "DIABETES",     # Diabetes prevalence
    "OBESITY",      # Obesity prevalence
    "STROKE",       # Stroke prevalence
    "CHD",          # Coronary heart disease
]

# Geographic focus (None = all states, or specify list like ["WI", "MN", "IL"])
TARGET_STATES: Optional[List[str]] = None  # All states

# Data year to fetch (None = latest available)
TARGET_YEAR: Optional[int] = 2023
