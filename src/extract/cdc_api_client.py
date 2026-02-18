"""
CDC PLACES API client for extracting county-level health data.

Provides access to CDC PLACES (Population Level Analysis and Community Estimates)
data via Socrata SODA API for building health surveillance pipelines.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import config
from ..utils.logging_config import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


class PLACESClient:
    """
    Client for CDC PLACES API (Socrata SODA endpoint).

    Fetches county-level health data with 36+ health measures including
    prevention, health outcomes, behaviors, and health status.
    """

    def __init__(self, app_token: Optional[str] = None):
        """
        Initialize PLACES API client.

        Args:
            app_token: Socrata app token for better rate limits (optional)
        """
        self.base_url = config.CDC_BASE_URL
        self.app_token = app_token or config.APP_TOKEN
        self.session = self._create_session()
        
        if self.app_token:
            logger.info("Using CDC PLACES app token for authentication")
        else:
            logger.warning(
                "No app token provided. Rate limited to 1,000 requests/hour. "
                "Get token at: https://data.cdc.gov/ → Profile → Developer Settings"
            )

    def _create_session(self) -> requests.Session:
        """
        Create requests session with retry strategy.

        Returns:
            requests.Session: Configured session
        """
        session = requests.Session()
        
        # Retry strategy for failed requests
        retry_strategy = Retry(
            total=config.MAX_RETRIES,
            backoff_factor=config.RETRY_DELAY,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers
        session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json"
        })
        
        if self.app_token:
            session.headers.update({"X-App-Token": self.app_token})
        
        return session

    def fetch_county_data(
        self,
        states: Optional[List[str]] = None,
        year: Optional[int] = None,
        limit: int = config.DEFAULT_LIMIT
    ) -> pd.DataFrame:
        """
        Fetch county-level GIS data (all measures, one row per county).

        Args:
            states: List of state abbreviations. None = all states
            year: Year filter (e.g., 2023). None = latest
            limit: Max records per API request

        Returns:
            pd.DataFrame: County health data with all measures

        Raises:
            requests.RequestException: If API request fails
        """
        endpoint = f"{self.base_url}/{config.COUNTY_GIS_DATASET}.json"
        logger.info(f"Fetching PLACES county data from {endpoint}")
        
        # Build SoQL (Socrata Query Language) filter
        where_clause = []
        
        if states:
            state_filter = " OR ".join([f"stateabbr='{s}'"] for s in states)
            where_clause.append(f"({state_filter})")
            logger.info(f"Filtering by states: {states}")
        
        # Note: Year filtering is skipped - PLACES data is usually single year
        # If filtering is needed, it should be done in transformation layer
        
        params = {"$limit": limit, "$offset": 0}
        if where_clause:
            params["$where"] = " AND ".join(where_clause)
        
        all_records = []
        try:
            while True:
                response = self.session.get(
                    endpoint, params=params, timeout=config.DEFAULT_TIMEOUT
                )
                response.raise_for_status()
                
                records = response.json()
                if not records:
                    break
                
                all_records.extend(records)
                logger.info(
                    f"Fetched {len(records)} records (total: {len(all_records)})"
                )
                
                # Check if last page
                if len(records) < limit:
                    break
                
                params["$offset"] += limit
        
        except requests.RequestException as e:
            logger.error(f"Failed to fetch PLACES data: {e}")
            raise
        
        logger.info(f"Successfully fetched {len(all_records)} county records")
        
        # Convert to DataFrame
        df = pd.DataFrame(all_records)
        
        # Add metadata
        df["extracted_at"] = datetime.utcnow()
        df["source"] = "CDC_PLACES"
        df["dataset_id"] = config.COUNTY_GIS_DATASET
        
        return df

    def save_raw_data(
        self, df: pd.DataFrame, timestamp: Optional[datetime] = None
    ) -> Path:
        """
        Save raw data to JSON file.

        Args:
            df: DataFrame with raw data
            timestamp: Custom timestamp for filename (defaults to current time)

        Returns:
            Path: Path to saved file

        Raises:
            IOError: If file cannot be written
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        raw_dir = Path(config.RAW_DATA_PATH)
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"places_county_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = raw_dir / filename
        
        try:
            df.to_json(filepath, orient="records", indent=2)
            logger.info(f"Saved {len(df)} records to {filepath}")
            return filepath
        
        except IOError as e:
            logger.error(f"Failed to save data to {filepath}: {e}")
            raise

    def run_extraction(self) -> Path:
        """
        Run complete extraction pipeline.

        Returns:
            Path: Path to saved raw data file
        """
        logger.info("Starting CDC PLACES extraction")
        
        try:
            df = self.fetch_county_data(
                states=config.TARGET_STATES,
                year=config.TARGET_YEAR
            )
            
            logger.info(
                f"Extracted {len(df)} counties with {len(df.columns)} columns"
            )
            logger.info(f"Columns: {', '.join(df.columns[:10])}...")
            
            filepath = self.save_raw_data(df)
            logger.info("Extraction complete")
            return filepath
        
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise


def main():
    """Entry point for running extraction as a script."""
    client = PLACESClient()
    client.run_extraction()


if __name__ == "__main__":
    main()
