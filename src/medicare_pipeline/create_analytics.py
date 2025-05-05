import polars as pl
from pathlib import Path
import logging
import glob
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AnalyticsBuilder:
    """
    Creates analytical views (gold layer) from the dimensional model (silver layer).

    These views provide the specific metrics required by the project:
    1. Annual Spend
        - Total allowed
        - Total paid
        - Inpatient stays count
        - Outpatient visits count
        - RX fills count
    2. Top Diagnoses
        - Top 5 diagnoses by paid amount for each member-year
    3. Provider Breadth
        - Count of distinct providers seen by each member in a year
    """

    def __init__(self, silver_dir: str, gold_dir: str):
        """
        Initialize the analytics builder.

        Args:
            silver_dir: Directory containing silver layer data
            gold_dir: Directory where gold layer will be written
        """
        self.silver_dir = Path(silver_dir)
        self.gold_dir = Path(gold_dir)
        self.gold_dir.mkdir(parents=True, exist_ok=True)

    def _get_silver_files(self, table_name: str) -> List[Path]:
        """Get all files for a specific table in the silver layer."""
        pattern = f"{self.silver_dir}/{table_name}/**/*.parquet"
        return [Path(f) for f in glob.glob(pattern, recursive=True)]

    def _get_gold_files(self, table_name: str) -> List[Path]:
        """Get all files for a specific table in the gold layer."""
        pattern = f"{self.gold_dir}/{table_name}/**/*.parquet"
        return [Path(f) for f in glob.glob(pattern, recursive=True)]

    def _read_complete_table(
        self, table_name: str, from_gold: bool = False
    ) -> Optional[pl.DataFrame]:
        """Read and combine all files for a specific table into a single dataframe."""
        if from_gold:
            files = self._get_gold_files(table_name)
        else:
            files = self._get_silver_files(table_name)

        if not files:
            logger.warning(f"No files found for {table_name}")
            return None

        dfs = []
        for file in files:
            logger.info(f"Reading {file}")
            try:
                df = pl.read_parquet(file)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")
                continue

        if not dfs:
            logger.error(f"No valid data found for {table_name}")
            return None

        return pl.concat(dfs)

    def create_member_year_metrics(self):
        """
        Create the member_year_metrics view.

        This view contains annual spend metrics for each member/year:
        - Total allowed
        - Total paid
        - Inpatient stays count
        - Outpatient visits count
        - RX fills count
        - Provider breadth (unique provider count)
        """
        logger.info("Creating member_year_metrics view")

        # Start with beneficiary data for demographics and total payments
        beneficiary_df = self._read_complete_table("dim_beneficiary")
        if beneficiary_df is None:
            return

        # Read claims data for utilization counts
        claims_df = self._read_complete_table("fact_claims")
        prescription_df = self._read_complete_table("fact_prescription")

        # Create base metrics from beneficiary data
        metrics_df = beneficiary_df.select(
            [
                pl.col("bene_id"),
                pl.col("year"),
                pl.col("total_allowed"),
                pl.col("total_paid"),
                pl.col("gender"),
                pl.col("race"),
                pl.col("state"),
            ]
        )

        # Count inpatient stays and outpatient visits
        if claims_df is not None:
            # Count distinct claims by type
            claim_counts = claims_df.group_by(["bene_id", "year", "claim_type"]).agg(
                [pl.n_unique("claim_id").alias("claim_count")]
            )

            # Pivot the results to get one column per claim type
            claim_counts_wide = claim_counts.pivot(
                index=["bene_id", "year"], columns="claim_type", values="claim_count"
            ).with_columns(
                [
                    pl.col("inpatient").fill_null(0).alias("inpatient_stays"),
                    pl.col("outpatient").fill_null(0).alias("outpatient_visits"),
                    pl.col("carrier").fill_null(0).alias("carrier_claims"),
                ]
            )

            # Count unique providers per member/year
            provider_counts = (
                claims_df.filter(
                    pl.col("provider_id").is_not_null() & (pl.col("provider_id") != "")
                )
                .group_by(["bene_id", "year"])
                .agg([pl.n_unique("provider_id").alias("unique_providers")])
            )

            # Join claim counts to the metrics
            metrics_df = metrics_df.join(
                claim_counts_wide.select(
                    [
                        "bene_id",
                        "year",
                        "inpatient_stays",
                        "outpatient_visits",
                        "carrier_claims",
                    ]
                ),
                on=["bene_id", "year"],
                how="left",
            )

            # Fill missing values with zeros
            metrics_df = metrics_df.with_columns(
                [
                    pl.col("inpatient_stays").fill_null(0),
                    pl.col("outpatient_visits").fill_null(0),
                    pl.col("carrier_claims").fill_null(0),
                ]
            )

            # Join provider counts
            metrics_df = metrics_df.join(
                provider_counts, on=["bene_id", "year"], how="left"
            ).with_columns([pl.col("unique_providers").fill_null(0)])
        else:
            # Add placeholder columns if claims data is missing
            metrics_df = metrics_df.with_columns(
                [
                    pl.lit(0).alias("inpatient_stays"),
                    pl.lit(0).alias("outpatient_visits"),
                    pl.lit(0).alias("carrier_claims"),
                    pl.lit(0).alias("unique_providers"),
                ]
            )

        # Count prescription fills
        if prescription_df is not None:
            rx_counts = prescription_df.group_by(["bene_id", "year"]).agg(
                [pl.n_unique("prescription_id").alias("rx_fills")]
            )

            # Join rx counts
            metrics_df = metrics_df.join(
                rx_counts, on=["bene_id", "year"], how="left"
            ).with_columns([pl.col("rx_fills").fill_null(0)])
        else:
            # Add placeholder column if prescription data is missing
            metrics_df = metrics_df.with_columns([pl.lit(0).alias("rx_fills")])

        # Add prescription provider counts to unique_providers if needed
        if prescription_df is not None and claims_df is not None:
            # Get prescription providers
            rx_providers = prescription_df.filter(
                pl.col("provider_id").is_not_null() & (pl.col("provider_id") != "")
            ).select(["bene_id", "year", "provider_id"])

            # Get claim providers
            claim_providers = claims_df.filter(
                pl.col("provider_id").is_not_null() & (pl.col("provider_id") != "")
            ).select(["bene_id", "year", "provider_id"])

            # Combine and count unique providers
            all_providers = pl.concat([rx_providers, claim_providers])
            all_provider_counts = all_providers.group_by(["bene_id", "year"]).agg(
                [pl.n_unique("provider_id").alias("all_unique_providers")]
            )

            # Update the metrics with combined provider counts
            metrics_df = (
                metrics_df.join(all_provider_counts, on=["bene_id", "year"], how="left")
                .with_columns(
                    [
                        pl.col("all_unique_providers")
                        .fill_null(0)
                        .alias("unique_providers")
                    ]
                )
                .drop("all_unique_providers")
            )

        # Write to parquet partitioned by year
        output_path = self.gold_dir / "member_year_metrics"
        output_path.mkdir(parents=True, exist_ok=True)

        # Partition and write by year
        for year, year_df in metrics_df.partition_by("year", as_dict=True).items():
            year_path = output_path / f"year={year}"
            year_path.mkdir(parents=True, exist_ok=True)

            year_df.write_parquet(
                year_path / "member_year_metrics.parquet",
                compression="zstd",
                statistics=True,
                use_pyarrow=True,
            )

        logger.info(
            f"Successfully created member_year_metrics with {len(metrics_df)} rows"
        )

    def create_top_diagnoses(self):
        """
        Create the top_diagnoses_by_member view.

        This view contains the top 5 diagnoses with the highest paid dollars
        for each member-year.
        """
        logger.info("Creating top_diagnoses_by_member view")

        # Read diagnosis data
        diagnosis_df = self._read_complete_table("fact_claim_diagnoses")
        if diagnosis_df is None:
            return

        # Aggregate diagnosis spend by member/year/diagnosis
        diagnosis_spend = diagnosis_df.group_by(
            ["bene_id", "year", "diagnosis_code", "diagnosis_description"]
        ).agg([pl.sum("payment").alias("diagnosis_payment")])

        # Rank diagnoses by payment within each member/year
        diagnosis_spend = diagnosis_spend.sort(
            ["bene_id", "year", pl.col("diagnosis_payment").reverse()]
        )

        # Add rank column using window function
        diagnosis_spend = diagnosis_spend.with_columns(
            [
                pl.col("diagnosis_payment")
                .rank(method="dense", descending=True)
                .over(["bene_id", "year"])
                .alias("diagnosis_rank")
            ]
        )

        # Filter to top 5 diagnoses per member/year
        top_diagnoses = diagnosis_spend.filter(pl.col("diagnosis_rank") <= 5)

        # Write to parquet partitioned by year
        output_path = self.gold_dir / "top_diagnoses_by_member"
        output_path.mkdir(parents=True, exist_ok=True)

        # Partition and write by year
        for year, year_df in top_diagnoses.partition_by("year", as_dict=True).items():
            year_path = output_path / f"year={year}"
            year_path.mkdir(parents=True, exist_ok=True)

            year_df.write_parquet(
                year_path / "top_diagnoses_by_member.parquet",
                compression="zstd",
                statistics=True,
                use_pyarrow=True,
            )

        logger.info(
            f"Successfully created top_diagnoses_by_member with {len(top_diagnoses)} rows"
        )

    def create_patient_api_view(self):
        """
        Create a combined view optimized for the patient API endpoint.

        This view joins member metrics with top diagnoses for efficient API responses.
        """
        logger.info("Creating patient_api_view")

        # Read metrics and diagnoses from the gold layer
        metrics_files = self._get_gold_files("member_year_metrics")
        diagnoses_files = self._get_gold_files("top_diagnoses_by_member")

        if not metrics_files or not diagnoses_files:
            logger.error("Missing required data for patient_api_view")
            return

        # Read metrics and diagnoses from the gold layer
        metrics_df = self._read_complete_table("member_year_metrics", from_gold=True)
        diagnoses_df = self._read_complete_table(
            "top_diagnoses_by_member", from_gold=True
        )

        if metrics_df is None or diagnoses_df is None:
            return

        # Select only needed columns from metrics
        metrics_slim = metrics_df.select(
            [
                "bene_id",
                "year",
                "total_allowed",
                "total_paid",
                "inpatient_stays",
                "outpatient_visits",
                "rx_fills",
                "unique_providers",
            ]
        )

        # Write to parquet partitioned by year and bene_id (for faster lookups)
        output_path = self.gold_dir / "patient_api_view"
        output_path.mkdir(parents=True, exist_ok=True)

        # Create patient metrics view
        for year, year_df in metrics_slim.partition_by("year", as_dict=True).items():
            year_path = output_path / f"year={year}"
            year_path.mkdir(parents=True, exist_ok=True)

            year_df.write_parquet(
                year_path / "patient_metrics.parquet",
                compression="zstd",
                statistics=True,
                use_pyarrow=True,
            )

        # Create patient diagnoses view
        for year, year_df in diagnoses_df.partition_by("year", as_dict=True).items():
            year_path = output_path / f"year={year}"
            year_path.mkdir(parents=True, exist_ok=True)

            year_df.write_parquet(
                year_path / "patient_diagnoses.parquet",
                compression="zstd",
                statistics=True,
                use_pyarrow=True,
            )

        logger.info("Successfully created patient_api_view")

    def create_all_analytics(self):
        """
        Execute the complete analytics creation process.
        """
        logger.info("Starting analytics creation process")

        self.create_member_year_metrics()
        self.create_top_diagnoses()
        self.create_patient_api_view()

        logger.info("Analytics creation complete")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Create analytics views from silver layer data"
    )
    parser.add_argument(
        "--silver-dir", required=True, help="Directory containing silver layer data"
    )
    parser.add_argument(
        "--gold-dir", required=True, help="Output directory for gold layer"
    )

    args = parser.parse_args()

    builder = AnalyticsBuilder(args.silver_dir, args.gold_dir)
    builder.create_all_analytics()


if __name__ == "__main__":
    main()
