import polars as pl
from pathlib import Path
import random
import logging
from typing import Dict, Any
import json
from datetime import date, datetime
from decimal import Decimal

logger = logging.getLogger(__name__)


# Custom JSON encoder to handle date and decimal objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class DataValidator:
    """Validates the data pipeline output by examining Parquet files across layers."""

    def __init__(self, base_dir: str = "data"):
        """
        Initialize the validator with the base data directory.

        Args:
            base_dir: Base directory containing the data layers
        """
        self.base_dir = Path(base_dir)
        self.bronze_dir = self.base_dir / "bronze"
        self.silver_dir = self.base_dir / "silver"
        self.gold_dir = self.base_dir / "gold"
        self.validation_results = {}
        self.summary = {
            "bronze": {"status": "not_validated", "tables": 0, "rows": 0, "issues": []},
            "silver": {"status": "not_validated", "tables": 0, "rows": 0, "issues": []},
            "gold": {"status": "not_validated", "tables": 0, "rows": 0, "issues": []},
        }

    def _examine_parquet_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Examine a Parquet file and return its metadata and sample data.

        Args:
            file_path: Path to the Parquet file

        Returns:
            Dictionary containing file metadata
        """
        # Load the Parquet file
        df = pl.read_parquet(file_path)

        # Collect basic stats
        info = {
            "file_path": str(file_path),
            "row_count": df.height,
            "column_count": df.width,
            "columns": list(df.columns),
            "schema": {col: str(dtype) for col, dtype in df.schema.items()},
            "sample_rows": min(2, df.height),
            "sample": df.head(2).to_dicts() if df.height > 0 else [],
            "status": "valid",
        }

        # Check for data quality issues
        issues = []

        # Check for empty dataframe
        if df.height == 0:
            issues.append("Empty dataframe")

        # Check for missing values in key columns
        if (
            "bene_id" in df.columns
            and df.filter(pl.col("bene_id").is_null()).height > 0
        ):
            issues.append("Missing values in bene_id column")

        if issues:
            info["status"] = "warning"
            info["issues"] = issues

        return info

    def _validate_layer(self, layer_dir: Path, layer_name: str) -> Dict[str, Any]:
        """
        Validate a single data layer by examining sample files from each table.

        Args:
            layer_dir: Directory containing the layer data
            layer_name: Name of the layer (bronze, silver, gold)

        Returns:
            Dictionary containing validation results
        """
        if not layer_dir.exists():
            return {
                "status": "missing",
                "error": f"Directory {layer_dir} does not exist",
            }

        results = {}
        total_rows = 0
        total_tables = 0
        issues = []

        # For each table/data type in this layer
        for table_dir in [d for d in layer_dir.iterdir() if d.is_dir()]:
            # Find Parquet files
            parquet_files = list(table_dir.glob("**/*.parquet"))

            if not parquet_files:
                issues.append(f"No Parquet files found in {table_dir}")
                continue

            # Sample a few files to examine
            sample_size = min(3, len(parquet_files))
            sample_files = random.sample(parquet_files, sample_size)

            table_results = []
            table_rows = 0

            for file_path in sample_files:
                file_info = self._examine_parquet_file(file_path)
                table_results.append(file_info)

                if file_info["status"] == "valid" and "row_count" in file_info:
                    table_rows += file_info["row_count"]

                if file_info["status"] == "error":
                    issues.append(f"Error in {file_path}: {file_info.get('error')}")
                elif file_info["status"] == "warning":
                    for issue in file_info.get("issues", []):
                        issues.append(f"Warning in {file_path}: {issue}")

            results[table_dir.name] = {
                "sampled_files": len(sample_files),
                "total_files": len(parquet_files),
                "row_count": table_rows,
                "file_samples": table_results,
            }

            total_rows += table_rows
            total_tables += 1

        layer_status = "valid" if total_tables > 0 and not issues else "error"

        # Update the summary
        self.summary[layer_name] = {
            "status": layer_status,
            "tables": total_tables,
            "rows": total_rows,
            "issues": issues,
        }

        return results

    def validate_all(self) -> Dict[str, Any]:
        """
        Validate all data layers (bronze, silver, gold).

        Returns:
            Dictionary containing validation results for all layers
        """
        logger.info("Starting data validation...")

        # Validate bronze layer
        logger.info("Validating bronze layer...")
        bronze_results = self._validate_layer(self.bronze_dir, "bronze")
        self.validation_results["bronze"] = bronze_results

        # Validate silver layer
        logger.info("Validating silver layer...")
        silver_results = self._validate_layer(self.silver_dir, "silver")
        self.validation_results["silver"] = silver_results

        # Validate gold layer
        logger.info("Validating gold layer...")
        gold_results = self._validate_layer(self.gold_dir, "gold")
        self.validation_results["gold"] = gold_results

        logger.info("Data validation complete")
        return self.validation_results

    def print_summary(self) -> None:
        """Print a summary of the validation results."""
        print("\n=== DATA VALIDATION SUMMARY ===\n")

        # Print overall status
        all_valid = all(info["status"] == "valid" for info in self.summary.values())
        overall_status = "PASSED" if all_valid else "FAILED"
        print(f"Overall validation: {overall_status}\n")

        # Print layer summaries
        for layer, info in self.summary.items():
            status_symbol = "✅" if info["status"] == "valid" else "❌"
            print(
                f"{status_symbol} {layer.upper()} layer: {info['tables']} tables, {info['rows']} rows"
            )

            if info["issues"]:
                print("  Issues:")
                for issue in info["issues"]:
                    print(f"  - {issue}")
            print()

    def save_results(self, output_file: str = "validation_results.json") -> None:
        """
        Save validation results to a JSON file.

        Args:
            output_file: Path to the output JSON file
        """
        output_path = Path(output_file)

        # Prepare the results with summary
        results = {"summary": self.summary, "details": self.validation_results}

        # Save to file with custom encoder for dates
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2, cls=DateTimeEncoder)

        logger.info(f"Validation results saved to {output_path}")


def main():
    """Run validation as a standalone script."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate the Medicare data pipeline output"
    )
    parser.add_argument(
        "--data-dir", default="data", help="Base directory containing data layers"
    )
    parser.add_argument(
        "--output",
        default="validation_results.json",
        help="Output file for validation results",
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Run validation
    validator = DataValidator(args.data_dir)
    validator.validate_all()
    validator.print_summary()
    validator.save_results(args.output)


if __name__ == "__main__":
    main()
