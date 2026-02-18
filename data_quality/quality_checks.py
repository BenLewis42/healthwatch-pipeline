"""
Data quality checks for CDC surveillance pipeline.

Implements automated validation rules to ensure data consistency and accuracy.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys

import duckdb

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logging_config import setup_logging, get_logger

logger = get_logger(__name__)
setup_logging()


class DataQualityChecker:
    """
    Quality validation engine for surveillance data.

    Runs systematic checks for freshness, completeness, validity, and anomalies.
    """

    def __init__(self, db_path: str = "data/warehouse.duckdb"):
        """
        Initialize quality checker.

        Args:
            db_path: Path to DuckDB database
        """
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.checks_passed = 0
        self.checks_failed = 0
        self.results = {}

    def check_data_freshness(self, max_hours_old: int = 24) -> dict:
        """
        Verify data was loaded recently.

        Args:
            max_hours_old: Maximum acceptable age in hours

        Returns:
            dict: Check result with status and details
        """
        try:
            result = self.conn.execute(
                """
                SELECT MAX(loaded_at) as last_load FROM raw.places_county
                """
            ).fetchall()

            last_load = max(r[0] for r in result if r[0])

            if last_load is None:
                self.checks_failed += 1
                return {"name": "Data Freshness", "status": "FAIL", "reason": "No data loaded"}

            hours_old = (datetime.now() - last_load).total_seconds() / 3600

            if hours_old <= max_hours_old:
                self.checks_passed += 1
                return {
                    "name": "Data Freshness",
                    "status": "PASS",
                    "last_load": str(last_load),
                    "hours_old": round(hours_old, 2),
                }
            else:
                self.checks_failed += 1
                return {
                    "name": "Data Freshness",
                    "status": "FAIL",
                    "reason": f"Data is {round(hours_old, 2)} hours old (max: {max_hours_old})",
                }

        except Exception as e:
            self.checks_failed += 1
            logger.error(f"Freshness check failed: {e}")
            return {"name": "Data Freshness", "status": "ERROR", "error": str(e)}

    def check_record_counts(self) -> dict:
        """
        Verify minimum records exist.

        Returns:
            dict: Check result with record counts
        """
        try:
            places_count = self.conn.execute(
                "SELECT COUNT(*) FROM raw.places_county"
            ).fetchall()[0][0]

            if places_count > 0:
                self.checks_passed += 1
                return {
                    "name": "Record Counts",
                    "status": "PASS",
                    "places_records": places_count,
                    "total_records": places_count,
                }
            else:
                self.checks_failed += 1
                return {
                    "name": "Record Counts",
                    "status": "FAIL",
                    "reason": "No records found",
                }

        except Exception as e:
            self.checks_failed += 1
            logger.error(f"Record count check failed: {e}")
            return {"name": "Record Counts", "status": "ERROR", "error": str(e)}

    def check_null_values(self) -> dict:
        """
        Check critical columns for null values.

        Returns:
            dict: Check result with null value counts
        """
        try:
            # Check PLACES data for critical nulls
            places_nulls = self.conn.execute(
                """
                SELECT
                    SUM(CASE WHEN stateabbr IS NULL THEN 1 ELSE 0 END) as null_states,
                    SUM(CASE WHEN countyname IS NULL THEN 1 ELSE 0 END) as null_counties,
                    SUM(CASE WHEN diabetes_crudeprev IS NULL OR diabetes_crudeprev = '' THEN 1 ELSE 0 END) as null_diabetes
                FROM raw.places_county
                """
            ).fetchall()[0]

            if places_nulls[0] == 0 and places_nulls[1] == 0:
                self.checks_passed += 1
                return {
                    "name": "Null Values Check",
                    "status": "PASS",
                    "null_states": int(places_nulls[0]),
                    "null_counties": int(places_nulls[1]),
                    "null_diabetes": int(places_nulls[2]),
                }
            else:
                self.checks_failed += 1
                return {
                    "name": "Null Values Check",
                    "status": "FAIL",
                    "covid_null_states": int(covid_nulls[0]),
                    "covid_null_dates": int(covid_nulls[1]),
                }

        except Exception as e:
            self.checks_failed += 1
            logger.error(f"Null values check failed: {e}")
            return {"name": "Null Values Check", "status": "ERROR", "error": str(e)}

    def check_date_ranges(self) -> dict:
        """
        Verify load dates are recent.

        Returns:
            dict: Check result with date statistics
        """
        try:
            date_stats = self.conn.execute(
                """
                SELECT
                    MIN(loaded_at) as min_load,
                    MAX(loaded_at) as max_load,
                    COUNT(DISTINCT DATE(loaded_at)) as distinct_load_dates
                FROM raw.places_county
                WHERE loaded_at IS NOT NULL
                """
            ).fetchall()[0]

            min_date, max_date, distinct_dates = date_stats

            # Check if data was loaded recently
            if min_date and max_date:
                is_recent = (datetime.now() - max_date).days < 7
                if is_recent:
                    self.checks_passed += 1
                else:
                    self.checks_failed += 1

                return {
                    "name": "Date Range Check",
                    "status": "PASS" if is_recent else "FAIL",
                    "min_date": str(min_date),
                    "max_date": str(max_date),
                    "distinct_dates": int(distinct_dates),
                }
            else:
                self.checks_failed += 1
                return {"name": "Date Range Check", "status": "FAIL", "reason": "No date data"}

        except Exception as e:
            self.checks_failed += 1
            logger.error(f"Date range check failed: {e}")
            return {"name": "Date Range Check", "status": "ERROR", "error": str(e)}

    def run_all_checks(self) -> dict:
        """
        Execute all quality checks.

        Returns:
            dict: Summary of all check results
        """
        logger.info("Starting data quality checks...")

        checks = [
            self.check_data_freshness(),
            self.check_record_counts(),
            self.check_null_values(),
            self.check_date_ranges(),
        ]

        self.results = {
            "timestamp": datetime.now().isoformat(),
            "checks": checks,
            "summary": {
                "total_checks": len(checks),
                "passed": self.checks_passed,
                "failed": self.checks_failed,
                "status": "PASS" if self.checks_failed == 0 else "FAIL",
            },
        }

        return self.results

    def save_report(self, output_path: str = "data_quality/report.json") -> None:
        """
        Save quality check results to JSON file.

        Args:
            output_path: Path to save report
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            json.dump(self.results, f, indent=2, default=str)

        logger.info(f"Quality report saved to {output_path}")

    def print_report(self) -> None:
        """Print formatted quality check report."""
        if not self.results:
            logger.warning("No results to print. Run checks first.")
            return

        print("\n" + "=" * 60)
        print("DATA QUALITY REPORT")
        print("=" * 60)
        print(f"Timestamp: {self.results['timestamp']}")
        print(f"\nSummary: {self.results['summary']['status']}")
        print(f"  [+] Passed: {self.results['summary']['passed']}")
        print(f"  [-] Failed: {self.results['summary']['failed']}")
        print()

        for check in self.results["checks"]:
            status_symbol = "[+]" if check["status"] == "PASS" else "[-]"
            print(f"{status_symbol} {check['name']}: {check['status']}")
            for key, value in check.items():
                if key not in ["name", "status"]:
                    print(f"    {key}: {value}")

        print("=" * 60 + "\n")

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Entry point for running quality checks as a script."""
    checker = DataQualityChecker()
    results = checker.run_all_checks()
    checker.print_report()
    checker.save_report()
    checker.close()

    # Exit with failure code if any checks failed
    import sys

    sys.exit(0 if results["summary"]["status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
