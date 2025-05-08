import argparse
import logging
from pathlib import Path
import sys
import time

from medicare_pipeline.download_data import DataDownloader
from medicare_pipeline.csv_to_parquet import CSVToParquetConverter
from medicare_pipeline.transform_data import DataTransformer
from medicare_pipeline.create_analytics import AnalyticsBuilder
from medicare_pipeline.validate_data import DataValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("medicare_pipeline.log"),  # TODO: make this work
    ],
)
logger = logging.getLogger(__name__)


def setup_directories(base_dir: str = ".") -> dict:
    """
    Set up directory structure for the pipeline.

    Args:
        base_dir: Base directory for all data

    Returns:
        Dictionary of directories
    """
    base_path = Path(base_dir)

    dirs = {
        "raw": base_path / "data" / "raw",
        "bronze": base_path / "data" / "bronze",
        "silver": base_path / "data" / "silver",
        "gold": base_path / "data" / "gold",
    }

    # Create directories
    for dir_path in dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {dir_path}")

    return dirs


def run_download(raw_dir: Path) -> bool:
    """
    Download raw data files.

    Args:
        raw_dir: Directory to store raw files

    Returns:
        True if successful
    """
    logger.info("=== Starting data download phase ===")

    start_time = time.time()
    downloader = DataDownloader(str(raw_dir))
    downloader.download_and_extract_all()
    end_time = time.time()

    logger.info(f"Data download completed in {end_time - start_time:.2f} seconds")


def run_conversion(raw_dir: Path, bronze_dir: Path) -> bool:
    """
    Convert raw CSV files to Parquet format (bronze layer).

    Args:
        raw_dir: Directory containing raw CSV files
        bronze_dir: Directory to store bronze layer files

    Returns:
        True if successful
    """
    logger.info("=== Starting CSV to Parquet conversion phase ===")

    start_time = time.time()
    converter = CSVToParquetConverter(str(raw_dir), str(bronze_dir))
    converter.process_directory()
    end_time = time.time()

    logger.info(f"Conversion completed in {end_time - start_time:.2f} seconds")


def run_transformation(bronze_dir: Path, silver_dir: Path) -> bool:
    """
    Transform bronze layer to silver layer (dimensional model).

    Args:
        bronze_dir: Directory containing bronze layer files
        silver_dir: Directory to store silver layer files

    Returns:
        True if successful
    """
    logger.info("=== Starting data transformation phase ===")

    start_time = time.time()
    transformer = DataTransformer(str(bronze_dir), str(silver_dir))
    transformer.transform_all()
    end_time = time.time()

    logger.info(f"Transformation completed in {end_time - start_time:.2f} seconds")


def run_analytics(silver_dir: Path, gold_dir: Path) -> bool:
    """
    Create analytics views from silver layer (gold layer).

    Args:
        silver_dir: Directory containing silver layer files
        gold_dir: Directory to store gold layer files

    Returns:
        True if successful
    """
    logger.info("=== Starting analytics creation phase ===")

    start_time = time.time()
    builder = AnalyticsBuilder(str(silver_dir), str(gold_dir))
    builder.create_all_analytics()
    end_time = time.time()

    logger.info(f"Analytics creation completed in {end_time - start_time:.2f} seconds")


def run_validation(base_dir: Path) -> bool:
    """
    Validate the data pipeline output.

    Args:
        base_dir: Base directory for all data
        output_file: Path to save validation results

    Returns:
        True if validation passes
    """
    logger.info("=== Starting data validation phase ===")

    start_time = time.time()
    validator = DataValidator(str(base_dir))
    validator.validate_all()
    validator.print_summary()
    validator.save_results()
    all_valid = all(info["status"] == "valid" for info in validator.summary.values())
    end_time = time.time()

    logger.info(f"Data validation completed in {end_time - start_time:.2f} seconds")

    if not all_valid:
        logger.warning("Validation FAILED - see validation_results.json for details")
        raise ValueError("Validation FAILED")

    logger.info("Validation PASSED")


def run_pipeline(base_dir: str, steps: list) -> bool:
    """
    Run the complete data pipeline or specific steps.

    Args:
        base_dir: Base directory for all data
        steps: List of steps to run ["download", "convert", "transform", "analytics", "validate"]
        skip_download_if_exists: Skip download if files already exist
        validation_output: Path to save validation results

    Returns:
        True if all steps were successful
    """
    logger.info(f"Starting Medicare data pipeline with steps: {steps}")

    # Set up directories
    dirs = setup_directories(base_dir)

    # Run selected steps
    if "download" in steps:
        run_download(dirs["raw"])
    if "convert" in steps:
        run_conversion(dirs["raw"], dirs["bronze"])
    if "transform" in steps:
        run_transformation(dirs["bronze"], dirs["silver"])
    if "analytics" in steps:
        run_analytics(dirs["silver"], dirs["gold"])
    if "validate" in steps:
        run_validation(Path(base_dir) / "data")

    logger.info("Pipeline completed successfully!")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Run the Medicare data pipeline")
    parser.add_argument("--base-dir", default=".", help="Base directory for all data")
    parser.add_argument(
        "--steps",
        default="all",
        help="Comma-separated list of steps to run: download,convert,transform,analytics,validate",
    )

    args = parser.parse_args()

    # Parse steps
    if args.steps == "all":
        steps = ["download", "convert", "transform", "analytics", "validate"]
    else:
        steps = args.steps.split(",")

    # Run pipeline
    success = run_pipeline(args.base_dir, steps)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
