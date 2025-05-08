import polars as pl
from pathlib import Path
import logging
import glob
from typing import List
from dataclasses import dataclass
from icd9cms import search

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

COLUMN_GROUPS = {
    "LINE_PMT_COLS": [f"LINE_NCH_PMT_AMT_{i}" for i in range(1, 14)],
    "LINE_PRVDR_PMT_COLS": [f"LINE_BENE_PRMRY_PYR_PD_AMT_{i}" for i in range(1, 14)],
    "PRF_PHYSN_COLS": [f"PRF_PHYSN_NPI_{i}" for i in range(1, 14)],
    "CARRIER_DGNS_COLS": [f"ICD9_DGNS_CD_{i}" for i in range(1, 9)],
    "INPATIENT_OUTPATIENT_DGNS_COLS": [f"ICD9_DGNS_CD_{i}" for i in range(1, 11)],
}


@dataclass
class DataTransformer:
    """
    Transforms raw Medicare data from the bronze layer into a dimensional model (silver layer).

    The dimensional model consists of:
    - dim_beneficiary: Beneficiary demographics and annual spending
    - fact_claims: Unified claims table with claim details
    - fact_claim_diagnoses: Normalized diagnosis codes (long format)
    - fact_prescription: Prescription drug events
    - dim_provider: Provider information
    """

    bronze_dir: str
    silver_dir: str

    def __post_init__(self):
        """Initialize directories after creation."""
        self.bronze_dir = Path(self.bronze_dir)
        self.silver_dir = Path(self.silver_dir)
        self.silver_dir.mkdir(parents=True, exist_ok=True)

    def _get_icd9_description(self, code: str) -> str:
        """Get description for an ICD-9 code using the icd9cms library."""
        if not code or code.strip() == "":
            return "Unknown"
        description = search(code)
        return description.long_desc if description else "Unknown"

    def _get_files_by_type(self, file_type: str) -> List[Path]:
        """Get all Parquet files for a specific file type."""
        pattern = f"{self.bronze_dir}/{file_type}/**/*.parquet"
        files = [Path(f) for f in glob.glob(pattern, recursive=True)]
        logger.info(f"Found {len(files)} files for {file_type}")
        return files

    def _write_partitioned(self, df: pl.DataFrame, subdir: str):
        """Write a dataframe partitioned by year and bene_id_prefix."""
        # Partition by year
        for year_val, year_df in df.partition_by("year", as_dict=True).items():
            # Further partition by beneficiary ID prefix
            for prefix, prefix_df in year_df.partition_by(
                "bene_id_prefix", as_dict=True
            ).items():
                output_path = (
                    self.silver_dir
                    / subdir
                    / f"year={year_val}"
                    / f"bene_id_prefix={prefix}"
                )
                output_path.mkdir(parents=True, exist_ok=True)

                prefix_df.write_parquet(
                    output_path / f"{subdir}.parquet",
                    compression="zstd",
                    statistics=True,
                    use_pyarrow=True,
                )

    def create_dim_beneficiary(self):
        """Create the beneficiary dimension table."""
        logger.info("Creating dim_beneficiary table")

        # Read and combine all beneficiary files
        dfs = [pl.read_parquet(file) for file in self._get_files_by_type("beneficiary")]
        df = pl.concat(dfs)

        # Standardize column names
        rename_mapping = {
            "DESYNPUF_ID": "bene_id",
            "SEX": "sex",
            "RACE": "race",
            "STATE_CODE": "state",
            "BENE_BIRTH_DT": "birth_date",
            "BENE_DEATH_DT": "death_date",
            "MEDREIMB_IP": "ip_medicare_payment",
            "BENRES_IP": "ip_beneficiary_payment",
            "PPPYMT_IP": "ip_third_party_payment",
            "MEDREIMB_OP": "op_medicare_payment",
            "BENRES_OP": "op_beneficiary_payment",
            "PPPYMT_OP": "op_third_party_payment",
            "MEDREIMB_CAR": "car_medicare_payment",
            "BENRES_CAR": "car_beneficiary_payment",
            "PPPYMT_CAR": "car_third_party_payment",
        }
        df = df.rename(rename_mapping)

        # Add summary columns in a single operation
        df = df.with_columns(
            [
                # Total medicare payments
                (
                    pl.col("ip_medicare_payment")
                    + pl.col("op_medicare_payment")
                    + pl.col("car_medicare_payment")
                ).alias("total_medicare_payment"),
                # Total beneficiary payments
                (
                    pl.col("ip_beneficiary_payment")
                    + pl.col("op_beneficiary_payment")
                    + pl.col("car_beneficiary_payment")
                ).alias("total_beneficiary_payment"),
                # Total third-party payments
                (
                    pl.col("ip_third_party_payment")
                    + pl.col("op_third_party_payment")
                    + pl.col("car_third_party_payment")
                ).alias("total_third_party_payment"),
            ]
        )

        # Add total allowed and paid columns
        df = df.with_columns(
            [
                (
                    pl.col("total_medicare_payment")
                    + pl.col("total_beneficiary_payment")
                    + pl.col("total_third_party_payment")
                ).alias("total_allowed"),
                # Total paid (simply just the medicare payment)
                pl.col("total_medicare_payment").alias("total_paid"),
            ]
        )

        # Write the output
        output_path = self.silver_dir / "dim_beneficiary"
        output_path.mkdir(parents=True, exist_ok=True)
        df.write_parquet(
            output_path / "dim_beneficiary.parquet",
            compression="zstd",
            statistics=True,
            use_pyarrow=True,
        )

        logger.info(f"Successfully created dim_beneficiary with {len(df)} rows")

    def _process_claims(self, claim_type: str, files: List[Path]) -> pl.DataFrame:
        """Process claims of a specific type into a standardized format."""
        claim_dfs = []

        for file in files:
            logger.info(f"Reading {claim_type} claim: {file}")
            df = pl.read_parquet(file)

            if claim_type in ["inpatient", "outpatient"]:
                # Standard processing for inpatient and outpatient claims
                df = df.select(
                    [
                        pl.col("DESYNPUF_ID").alias("bene_id"),
                        pl.col("CLM_ID").alias("claim_id"),
                        pl.lit(claim_type).alias("claim_type"),
                        pl.col("CLM_FROM_DT").alias("claim_from_date"),
                        pl.col("CLM_THRU_DT").alias("claim_thru_date"),
                        pl.col("PRVDR_NUM").alias("provider_id"),
                        pl.col("CLM_PMT_AMT").alias("medicare_payment"),
                        pl.col("NCH_PRMRY_PYR_CLM_PD_AMT").alias("third_party_payment"),
                        pl.lit(0.0).alias("patient_payment"),
                        pl.col("year"),
                        pl.col("bene_id_prefix"),
                    ]
                )
            else:  # carrier claims
                # Find provider column
                provider_col = next(
                    (
                        col
                        for col in COLUMN_GROUPS["PRF_PHYSN_COLS"]
                        if col in df.columns
                    ),
                    None,
                )
                provider_expr = (
                    pl.col(provider_col).alias("provider_id")
                    if provider_col
                    else pl.lit("Unknown").alias("provider_id")
                )

                # Payment calculation
                if "CLM_PMT_AMT" in df.columns:
                    payment_col = pl.col("CLM_PMT_AMT").alias("medicare_payment")
                else:
                    payment_cols = [
                        col
                        for col in COLUMN_GROUPS["LINE_PMT_COLS"]
                        if col in df.columns
                    ]
                    payment_col = (
                        pl.sum_horizontal([pl.col(col) for col in payment_cols]).alias(
                            "medicare_payment"
                        )
                        if payment_cols
                        else pl.lit(0.0).alias("medicare_payment")
                    )

                # Third-party payment
                if "CLM_OP_PRVDR_PMT_AMT" in df.columns:
                    third_party_col = pl.col("CLM_OP_PRVDR_PMT_AMT").alias(
                        "third_party_payment"
                    )
                else:
                    third_party_cols = [
                        col
                        for col in COLUMN_GROUPS["LINE_PRVDR_PMT_COLS"]
                        if col in df.columns
                    ]
                    third_party_col = (
                        pl.sum_horizontal(
                            [pl.col(col) for col in third_party_cols]
                        ).alias("third_party_payment")
                        if third_party_cols
                        else pl.lit(0.0).alias("third_party_payment")
                    )

                df = df.select(
                    [
                        pl.col("DESYNPUF_ID").alias("bene_id"),
                        pl.col("CLM_ID").alias("claim_id"),
                        pl.lit(claim_type).alias("claim_type"),
                        pl.col("CLM_FROM_DT").alias("claim_from_date"),
                        pl.col("CLM_THRU_DT").alias("claim_thru_date"),
                        provider_expr,
                        payment_col,
                        third_party_col,
                        pl.lit(0.0).alias("patient_payment"),
                        pl.col("year"),
                        pl.col("bene_id_prefix"),
                    ]
                )

            claim_dfs.append(df)

        return pl.concat(claim_dfs) if claim_dfs else pl.DataFrame()

    def create_fact_claims(self):
        """Create the claims fact table unifying inpatient, outpatient, and carrier claims."""
        logger.info("Creating fact_claims table")

        # Process each claim type
        claim_types = {
            "inpatient": self._get_files_by_type("inpatient"),
            "outpatient": self._get_files_by_type("outpatient"),
            "carrier": self._get_files_by_type("carrier"),
        }

        claim_dfs = []
        for claim_type, files in claim_types.items():
            if files:
                claim_dfs.append(self._process_claims(claim_type, files))

        # Combine all claim types
        combined_claims = pl.concat(claim_dfs)

        # Add total payment column
        combined_claims = combined_claims.with_columns(
            [
                (
                    pl.col("medicare_payment")
                    + pl.col("third_party_payment")
                    + pl.col("patient_payment")
                ).alias("total_payment")
            ]
        )

        # Write partitioned output
        self._write_partitioned(combined_claims, "fact_claims")

        logger.info(
            f"Successfully created fact_claims with {len(combined_claims)} rows"
        )

    def create_fact_claim_diagnoses(self):
        """Create normalized diagnosis codes table with one row per diagnosis per claim."""
        logger.info("Creating fact_claim_diagnoses table")

        diagnosis_dfs = []

        # Process each claim type
        for claim_type in ["inpatient", "outpatient", "carrier"]:
            for file in self._get_files_by_type(claim_type):
                logger.info(f"Reading {claim_type} diagnoses: {file}")
                df = pl.read_parquet(file)

                # Determine which diagnosis columns to use
                diag_col_group = (
                    "CARRIER_DGNS_COLS"
                    if claim_type == "carrier"
                    else "INPATIENT_OUTPATIENT_DGNS_COLS"
                )
                diag_cols = [
                    col for col in COLUMN_GROUPS[diag_col_group] if col in df.columns
                ]

                # For carrier claims, ensure payment amount is calculated
                if claim_type == "carrier" and "CLM_PMT_AMT" not in df.columns:
                    payment_cols = [
                        col
                        for col in COLUMN_GROUPS["LINE_PMT_COLS"]
                        if col in df.columns
                    ]
                    if payment_cols:
                        df = df.with_columns(
                            [
                                pl.sum_horizontal(
                                    [pl.col(col) for col in payment_cols]
                                ).alias("CLM_PMT_AMT")
                            ]
                        )
                    else:
                        df = df.with_columns([pl.lit(0.0).alias("CLM_PMT_AMT")])

                # Process each diagnosis column
                for i, diag_col in enumerate(diag_cols):
                    pos_df = df.select(
                        [
                            pl.col("DESYNPUF_ID").alias("bene_id"),
                            pl.col("CLM_ID").alias("claim_id"),
                            pl.col(diag_col).alias("diagnosis_code"),
                            pl.lit(i + 1).alias("diagnosis_position"),
                            pl.col("CLM_PMT_AMT").alias("payment"),
                            pl.lit(claim_type).alias("claim_type"),
                            pl.col("year"),
                            pl.col("bene_id_prefix"),
                        ]
                    )

                    # Filter out empty diagnosis codes
                    pos_df = pos_df.filter(
                        (pl.col("diagnosis_code").is_not_null())
                        & (pl.col("diagnosis_code") != "")
                    )

                    # Add diagnosis description
                    pos_df = pos_df.with_columns(
                        [
                            pl.col("diagnosis_code")
                            .map_elements(
                                self._get_icd9_description, return_dtype=pl.Utf8
                            )
                            .alias("diagnosis_description")
                        ]
                    )

                    diagnosis_dfs.append(pos_df)

        # Combine all diagnosis dataframes
        combined_diagnoses = pl.concat(diagnosis_dfs)

        # Write partitioned output
        self._write_partitioned(combined_diagnoses, "fact_claim_diagnoses")

        logger.info(
            f"Successfully created fact_claim_diagnoses with {len(combined_diagnoses)} rows"
        )

    def create_fact_prescription(self):
        """Create the prescription fact table from PDE (Prescription Drug Event) files."""
        logger.info("Creating fact_prescription table")

        # Column mapping for PDE files
        column_mapping = {
            "DESYNPUF_ID": "bene_id",
            "PDE_ID": "prescription_id",
            "SRVC_DT": "service_date",
            "PROD_SRVC_ID": "product_id",
            "QTY_DSPNSD_NUM": "quantity_dispensed",
            "DAYS_SUPLY_NUM": "days_supply",
            "PTNT_PAY_AMT": "patient_payment",
            "TOT_RX_CST_AMT": "total_cost",
            "year": "year",
            "bene_id_prefix": "bene_id_prefix",
        }

        # Read and transform PDE files
        pde_dfs = []
        for file in self._get_files_by_type("pde"):
            logger.info(f"Reading prescription data: {file}")
            df = pl.read_parquet(file)

            # Select and rename columns
            select_exprs = [
                pl.col(source_col).alias(target_name)
                for source_col, target_name in column_mapping.items()
                if source_col in df.columns
            ]

            # Add year and bene_id_prefix if not in column_mapping
            for col in ["year", "bene_id_prefix"]:
                if col not in column_mapping.values() and col in df.columns:
                    select_exprs.append(pl.col(col))

            pde_dfs.append(df.select(select_exprs))

        # Combine PDE dataframes
        combined_prescriptions = pl.concat(pde_dfs)

        # Calculate medicare payment (total cost - patient payment)
        combined_prescriptions = combined_prescriptions.with_columns(
            [
                (pl.col("total_cost") - pl.col("patient_payment")).alias(
                    "medicare_payment"
                )
            ]
        )

        # Write partitioned output
        self._write_partitioned(combined_prescriptions, "fact_prescription")

        logger.info(
            f"Successfully created fact_prescription with {len(combined_prescriptions)} rows"
        )

    def create_dim_provider(self):
        """Create the provider dimension table with deduplicated provider information."""
        logger.info("Creating dim_provider table")

        provider_dfs = []

        # Process inpatient and outpatient claims
        for claim_type in ["inpatient", "outpatient"]:
            for file in self._get_files_by_type(claim_type):
                df = pl.read_parquet(file)

                # Check all potential provider columns
                potential_cols = [
                    "AT_PHYSN_NPI",
                    "OP_PHYSN_NPI",
                    "OT_PHYSN_NPI",
                    "PRVDR_NUM",
                ]
                provider_cols = [col for col in potential_cols if col in df.columns]

                for provider_col in provider_cols:
                    provider_dfs.append(
                        df.select([pl.col(provider_col).alias("provider_id")]).filter(
                            (pl.col("provider_id").is_not_null())
                            & (pl.col("provider_id") != "")
                        )
                    )

        # Process carrier claims
        for file in self._get_files_by_type("carrier"):
            df = pl.read_parquet(file)

            # Process each performing physician NPI column
            for provider_col in COLUMN_GROUPS["PRF_PHYSN_COLS"]:
                if provider_col in df.columns:
                    provider_dfs.append(
                        df.select([pl.col(provider_col).alias("provider_id")]).filter(
                            (pl.col("provider_id").is_not_null())
                            & (pl.col("provider_id") != "")
                        )
                    )

        # Combine and deduplicate providers
        combined_providers = pl.concat(provider_dfs)
        unique_providers = combined_providers.unique()

        # Write output
        output_path = self.silver_dir / "dim_provider"
        output_path.mkdir(parents=True, exist_ok=True)
        unique_providers.write_parquet(
            output_path / "dim_provider.parquet",
            compression="zstd",
            statistics=True,
            use_pyarrow=True,
        )

        logger.info(
            f"Successfully created dim_provider with {len(unique_providers)} rows"
        )

    def transform_all(self):
        """Execute the complete transformation process."""
        logger.info("Starting data transformation process")

        self.create_dim_beneficiary()
        self.create_dim_provider()
        self.create_fact_claims()
        self.create_fact_claim_diagnoses()
        self.create_fact_prescription()

        logger.info("Data transformation complete")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Transform Medicare data from bronze to silver layer"
    )
    parser.add_argument(
        "--bronze-dir", required=True, help="Directory containing bronze layer data"
    )
    parser.add_argument(
        "--silver-dir", required=True, help="Output directory for silver layer"
    )

    args = parser.parse_args()

    transformer = DataTransformer(args.bronze_dir, args.silver_dir)
    transformer.transform_all()


if __name__ == "__main__":
    main()
