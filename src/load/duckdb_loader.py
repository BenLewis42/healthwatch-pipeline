"""
DuckDB data warehouse loader for CDC surveillance data.

Handles database initialization, table creation, and incremental data loading.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from ..utils.logging_config import setup_logging, get_logger

logger = get_logger(__name__)
setup_logging()

DATABASE_PATH = "data/warehouse.duckdb"


class DuckDBLoader:
    """
    Loader for ingesting CDC data into DuckDB data warehouse.

    Handles schema creation, incremental loading with upsert logic,
    and metadata tracking.
    """

    def __init__(self, db_path: str = DATABASE_PATH):
        """
        Initialize DuckDB loader.

        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = db_path
        self.conn = None
        self._ensure_database()

    def _ensure_database(self) -> None:
        """Create database file and establish connection."""
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        self.conn = duckdb.connect(self.db_path)
        logger.info(f"Connected to DuckDB at {self.db_path}")

    def create_raw_schema(self) -> None:
        """
        Create raw data schema and tables.

        Creates tables for storing unmodified CDC PLACES data with metadata.
        """
        try:
            # Create raw schema
            self.conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

            # Note: Table will be created with auto-detected schema from JSON during load
            # This allows flexibility to handle all ~170 columns from PLACES API
            logger.info("Raw schema created (table will be auto-created on first load)")

        except Exception as e:
            logger.error(f"Failed to create raw schema: {e}")
            raise

    def load_json_file(self, filepath: str, source_type: str = "places") -> None:
        """
        Load JSON file from extraction into appropriate table.

        Args:
            filepath: Path to raw JSON file
            source_type: Type of data (default: "places" for CDC PLACES)

        Raises:
            IOError: If file cannot be read
        """
        filepath = Path(filepath)

        if not filepath.exists():
            raise IOError(f"File not found: {filepath}")

        logger.info(f"Loading PLACES county data from {filepath}")

        try:
            with open(filepath, "r") as f:
                raw_data = json.load(f)

            # Convert to DataFrame for easier processing
            if isinstance(raw_data, list):
                df = pd.DataFrame(raw_data)
            elif isinstance(raw_data, dict) and "data" in raw_data:
                df = pd.DataFrame(raw_data["data"])
            else:
                df = pd.DataFrame([raw_data])

            # Add metadata
            df["loaded_at"] = datetime.now()
            df["source_file"] = str(filepath)

            # Create table if it doesn't exist, or insert into existing table
            try:
                # Try to insert first (table might already exist)
                self.conn.execute(f"INSERT INTO raw.places_county SELECT * FROM df")
            except Exception as table_error:
                # If table doesn't exist, create it from the dataframe
                if "does not exist" in str(table_error).lower():
                    self.conn.execute("CREATE TABLE raw.places_county AS SELECT * FROM df")
                else:
                    raise
            
            logger.info(f"Loaded {len(df)} PLACES county records")

        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise

    def load_raw_directory(self) -> None:
        """
        Load all raw JSON files from data/raw directory.

        Automatically detects PLACES data files.
        """
        raw_dir = Path("data/raw")

        if not raw_dir.exists():
            logger.warning(f"Raw data directory not found: {raw_dir}")
            return

        json_files = list(raw_dir.glob("*.json"))
        logger.info(f"Found {len(json_files)} raw files to load")

        for filepath in json_files:
            try:
                # PLACES files follow naming convention: places_county_*.json
                if "places" in filepath.name:
                    self.load_json_file(filepath, "places")
                else:
                    logger.warning(f"Could not determine source for {filepath.name}")
                    continue

            except Exception as e:
                logger.error(f"Error loading {filepath}: {e}")
                continue

    def get_record_counts(self) -> dict:
        """
        Get record counts for all raw tables.

        Returns:
            dict: Table names and their record counts
        """
        counts = {}

        try:
            counts["places_county"] = self.conn.execute(
                "SELECT COUNT(*) FROM raw.places_county"
            ).fetchall()[0][0]

        except Exception as e:
            logger.error(f"Failed to get record counts: {e}")

        return counts

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


def main():
    """Entry point for running loader as a script."""
    loader = DuckDBLoader()
    loader.create_raw_schema()
    loader.load_raw_directory()

    counts = loader.get_record_counts()
    logger.info(f"Load complete. Record counts: {counts}")

    loader.close()


if __name__ == "__main__":
    main()
