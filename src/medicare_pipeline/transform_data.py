import polars as pl
from pathlib import Path
import logging
from typing import List
import glob
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants for payment columns
LINE_PMT_COLS = [
    "LINE_NCH_PMT_AMT_1",
    "LINE_NCH_PMT_AMT_2",
    "LINE_NCH_PMT_AMT_3",
    "LINE_NCH_PMT_AMT_4",
    "LINE_NCH_PMT_AMT_5",
    "LINE_NCH_PMT_AMT_6",
    "LINE_NCH_PMT_AMT_7",
    "LINE_NCH_PMT_AMT_8",
    "LINE_NCH_PMT_AMT_9",
    "LINE_NCH_PMT_AMT_10",
    "LINE_NCH_PMT_AMT_11",
    "LINE_NCH_PMT_AMT_12",
    "LINE_NCH_PMT_AMT_13",
]

LINE_PRVDR_PMT_COLS = [
    "LINE_BENE_PRMRY_PYR_PD_AMT_1",
    "LINE_BENE_PRMRY_PYR_PD_AMT_2",
    "LINE_BENE_PRMRY_PYR_PD_AMT_3",
    "LINE_BENE_PRMRY_PYR_PD_AMT_4",
    "LINE_BENE_PRMRY_PYR_PD_AMT_5",
    "LINE_BENE_PRMRY_PYR_PD_AMT_6",
    "LINE_BENE_PRMRY_PYR_PD_AMT_7",
    "LINE_BENE_PRMRY_PYR_PD_AMT_8",
    "LINE_BENE_PRMRY_PYR_PD_AMT_9",
    "LINE_BENE_PRMRY_PYR_PD_AMT_10",
    "LINE_BENE_PRMRY_PYR_PD_AMT_11",
    "LINE_BENE_PRMRY_PYR_PD_AMT_12",
    "LINE_BENE_PRMRY_PYR_PD_AMT_13",
]


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

    def __init__(self, bronze_dir: str, silver_dir: str):
        """
        Initialize the data transformer.

        Args:
            bronze_dir: Directory containing bronze layer Parquet files
            silver_dir: Directory where silver layer will be written
        """
        self.bronze_dir = Path(bronze_dir)
        self.silver_dir = Path(silver_dir)
        self.silver_dir.mkdir(parents=True, exist_ok=True)

        # ICD-9 diagnosis code mappings (simplified example - would be more complete in production)
        self.icd9_mappings = {
            "250": "Diabetes mellitus",
            "401": "Essential hypertension",
            "272": "Disorders of lipoid metabolism",
            "414": "Other forms of chronic ischemic heart disease",
            "427": "Cardiac dysrhythmias",
            "428": "Heart failure",
            "496": "Chronic airway obstruction",
            "311": "Depressive disorder",
            "715": "Osteoarthrosis",
            "724": "Other and unspecified disorders of back",
        }

    def _get_icd9_description(self, code: str) -> str:
        """Get description for an ICD-9 code."""
        if not code or code.strip() == "":
            return "Unknown"

        # Try to match the first 3 digits
        if len(code) >= 3:
            prefix = code[:3]
            if prefix in self.icd9_mappings:
                return self.icd9_mappings[prefix]

        return "Other diagnosis"

    def _get_files_by_type(self, file_type: str) -> List[Path]:
        """Get all Parquet files for a specific file type."""
        pattern = f"{self.bronze_dir}/{file_type}/**/*.parquet"
        files = [Path(f) for f in glob.glob(pattern, recursive=True)]
        logger.info(f"Found {len(files)} files for {file_type}")
        return files

    def create_dim_beneficiary(self):
        """
        Create the beneficiary dimension table.

        This table contains one row per beneficiary per year with demographics
        and annual spending information.
        """
        logger.info("Creating dim_beneficiary table")

        # Get all beneficiary files
        files = self._get_files_by_type("beneficiary")
        if not files:
            logger.error("No beneficiary files found!")
            raise ValueError("No beneficiary files found!")

        # Read and combine all beneficiary files
        dfs = []
        for file in files:
            logger.info(f"Reading {file}")
            df = pl.read_parquet(file)
            dfs.append(df)

        # Combine all dataframes
        df = pl.concat(dfs)

        # Rename columns for consistency
        df = df.rename(
            {
                "DESYNPUF_ID": "bene_id",
                "BENE_SEX_IDENT_CD": "gender",
                "BENE_RACE_CD": "race",
                "SP_STATE_CODE": "state",
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
        )

        # Add total payment columns
        df = df.with_columns(
            [
                (
                    pl.col("ip_medicare_payment")
                    + pl.col("op_medicare_payment")
                    + pl.col("car_medicare_payment")
                ).alias("total_medicare_payment"),
                (
                    pl.col("ip_beneficiary_payment")
                    + pl.col("op_beneficiary_payment")
                    + pl.col("car_beneficiary_payment")
                ).alias("total_beneficiary_payment"),
                (
                    pl.col("ip_third_party_payment")
                    + pl.col("op_third_party_payment")
                    + pl.col("car_third_party_payment")
                ).alias("total_third_party_payment"),
            ]
        )

        # Add total allowed amount (all payments combined)
        df = df.with_columns(
            [
                (
                    pl.col("total_medicare_payment")
                    + pl.col("total_beneficiary_payment")
                    + pl.col("total_third_party_payment")
                ).alias("total_allowed")
            ]
        )

        # Add total paid (Medicare + beneficiary)
        df = df.with_columns(
            [
                (
                    pl.col("total_medicare_payment")
                    + pl.col("total_beneficiary_payment")
                ).alias("total_paid")
            ]
        )

        # Write to parquet partitioned by year
        output_path = self.silver_dir / "dim_beneficiary"
        output_path.mkdir(parents=True, exist_ok=True)

        df.write_parquet(
            output_path / "dim_beneficiary.parquet",
            compression="zstd",
            statistics=True,
            use_pyarrow=True,
        )

        logger.info(f"Successfully created dim_beneficiary with {len(df)} rows")

    def create_fact_claims(self):
        """
        Create the claims fact table.

        This table unifies inpatient, outpatient, and carrier claims into a single table
        with a claim type discriminator.
        """
        logger.info("Creating fact_claims table")

        claim_dfs = []

        # Process inpatient claims
        for file in self._get_files_by_type("inpatient"):
            logger.info(f"Reading inpatient claim: {file}")
            df = pl.read_parquet(file)

            # Select and rename columns
            df = df.select(
                [
                    pl.col("DESYNPUF_ID").alias("bene_id"),
                    pl.col("CLM_ID").alias("claim_id"),
                    pl.lit("inpatient").alias("claim_type"),
                    pl.col("CLM_FROM_DT").alias("claim_from_date"),
                    pl.col("CLM_THRU_DT").alias("claim_thru_date"),
                    pl.col("PRVDR_NUM").alias("provider_id"),
                    pl.col("CLM_PMT_AMT").alias("medicare_payment"),
                    pl.col("NCH_PRMRY_PYR_CLM_PD_AMT").alias("third_party_payment"),
                    pl.lit(0.0)
                    .cast(pl.Decimal(precision=10, scale=2))
                    .alias("patient_payment"),
                    pl.col("year"),
                    pl.col("bene_id_prefix"),
                ]
            )

            claim_dfs.append(df)

        # Process outpatient claims
        for file in self._get_files_by_type("outpatient"):
            logger.info(f"Reading outpatient claim: {file}")
            df = pl.read_parquet(file)

            # Select and rename columns
            df = df.select(
                [
                    pl.col("DESYNPUF_ID").alias("bene_id"),
                    pl.col("CLM_ID").alias("claim_id"),
                    pl.lit("outpatient").alias("claim_type"),
                    pl.col("CLM_FROM_DT").alias("claim_from_date"),
                    pl.col("CLM_THRU_DT").alias("claim_thru_date"),
                    pl.col("PRVDR_NUM").alias("provider_id"),
                    pl.col("CLM_PMT_AMT").alias("medicare_payment"),
                    pl.col("NCH_PRMRY_PYR_CLM_PD_AMT").alias("third_party_payment"),
                    pl.lit(0.0)
                    .cast(pl.Decimal(precision=10, scale=2))
                    .alias("patient_payment"),
                    pl.col("year"),
                    pl.col("bene_id_prefix"),
                ]
            )

            claim_dfs.append(df)

        # Process carrier claims
        for file in self._get_files_by_type("carrier"):
            logger.info(f"Reading carrier claim: {file}")
            df = pl.read_parquet(file)

            # Check which provider column exists and use appropriate one
            provider_col = None
            if "PRVDR_NUM" in df.columns:
                provider_col = pl.col("PRVDR_NUM").alias("provider_id")
            elif "PRVDR_NPI" in df.columns:
                provider_col = pl.col("PRVDR_NPI").alias("provider_id")
            else:
                # Use a placeholder if neither provider column exists
                provider_col = pl.lit("Unknown").alias("provider_id")

            # Check if CLM_PMT_AMT exists
            if "CLM_PMT_AMT" in df.columns:
                payment_col = (
                    pl.col("CLM_PMT_AMT")
                    .cast(pl.Decimal(10, 2))
                    .alias("medicare_payment")
                )
            else:
                # Calculate payment from line-level columns if needed
                logger.info(
                    f"Calculating claim payment amount from {len(LINE_PMT_COLS)} line-level payment columns"
                )

                # First create a sum of all available payment columns, casting to decimal
                payment_expr = None
                for col in LINE_PMT_COLS:
                    if col in df.columns:
                        if payment_expr is None:
                            payment_expr = pl.col(col).cast(pl.Decimal(10, 2))
                        else:
                            payment_expr = payment_expr + pl.col(col).cast(
                                pl.Decimal(10, 2)
                            )

                # If no payment columns exist or payment_expr is None, use 0
                if payment_expr is None:
                    payment_col = (
                        pl.lit(0).cast(pl.Decimal(10, 2)).alias("medicare_payment")
                    )
                else:
                    payment_col = payment_expr.alias("medicare_payment")

            # Third-party payment calculation with proper decimal casting
            if "CLM_OP_PRVDR_PMT_AMT" in df.columns:
                third_party_col = (
                    pl.col("CLM_OP_PRVDR_PMT_AMT")
                    .cast(pl.Decimal(10, 2))
                    .alias("third_party_payment")
                )
            else:
                # Calculate third-party payment from line level
                logger.info(
                    f"Calculating third party payment from {len(LINE_PRVDR_PMT_COLS)} line-level columns"
                )

                # Create a sum of all available third-party payment columns with decimal casting
                third_party_expr = None
                for col in LINE_PRVDR_PMT_COLS:
                    if col in df.columns:
                        if third_party_expr is None:
                            third_party_expr = pl.col(col).cast(pl.Decimal(10, 2))
                        else:
                            third_party_expr = third_party_expr + pl.col(col).cast(
                                pl.Decimal(10, 2)
                            )

                # If none of the third-party payment columns exist, use 0
                if third_party_expr is None:
                    third_party_col = (
                        pl.lit(0).cast(pl.Decimal(10, 2)).alias("third_party_payment")
                    )
                else:
                    third_party_col = third_party_expr.alias("third_party_payment")

            # Select and rename columns
            df = df.select(
                [
                    pl.col("DESYNPUF_ID").alias("bene_id"),
                    pl.col("CLM_ID").alias("claim_id"),
                    pl.lit("carrier").alias("claim_type"),
                    pl.col("CLM_FROM_DT").alias("claim_from_date"),
                    pl.col("CLM_THRU_DT").alias("claim_thru_date"),
                    provider_col,
                    payment_col,
                    third_party_col,
                    pl.lit(0).cast(pl.Decimal(10, 2)).alias("patient_payment"),
                    pl.col("year"),
                    pl.col("bene_id_prefix"),
                ]
            )

            claim_dfs.append(df)

        # Combine all claim types
        if claim_dfs:
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

            # Write to parquet partitioned by year and bene_id_prefix
            for year_val, year_df in combined_claims.partition_by(
                "year", as_dict=True
            ).items():
                for prefix, prefix_df in year_df.partition_by(
                    "bene_id_prefix", as_dict=True
                ).items():
                    output_path = (
                        self.silver_dir
                        / "fact_claims"
                        / f"year={year_val}"
                        / f"bene_id_prefix={prefix}"
                    )
                    output_path.mkdir(parents=True, exist_ok=True)

                    prefix_df.write_parquet(
                        output_path / "fact_claims.parquet",
                        compression="zstd",
                        statistics=True,
                        use_pyarrow=True,
                    )

            logger.info(
                f"Successfully created fact_claims with {len(combined_claims)} rows"
            )
        else:
            logger.error("No data found for fact_claims")

    def create_fact_claim_diagnoses(self):
        """
        Create the claim diagnoses fact table.

        This table normalizes the wide format diagnosis codes into a long format with
        one row per diagnosis per claim.
        """
        logger.info("Creating fact_claim_diagnoses table")

        diagnosis_dfs = []

        # Process claims with diagnosis codes (inpatient, outpatient, carrier)
        for claim_type in ["inpatient", "outpatient", "carrier"]:
            for file in self._get_files_by_type(claim_type):
                logger.info(f"Reading {claim_type} diagnoses: {file}")
                df = pl.read_parquet(file)

                # Debug: Print column names
                logger.info(f"Diagnosis columns in {claim_type} file: {df.columns}")

                # Find all diagnosis code columns - could be ICD9_DGNS_CD_{i} or DGNS_CD_{i}
                diag_cols = []
                for i in range(1, 20):  # Look for up to 20 diagnosis columns
                    for prefix in ["ICD9_DGNS_CD_", "DGNS_CD_"]:
                        col_name = f"{prefix}{i}"
                        if col_name in df.columns:
                            diag_cols.append(col_name)

                if not diag_cols:
                    logger.warning(f"No diagnosis columns found in {file}")
                    continue

                logger.info(f"Found diagnosis columns: {diag_cols}")

                # Base columns needed for each diagnosis
                base_cols = [
                    "DESYNPUF_ID",
                    "CLM_ID",
                    "CLM_PMT_AMT",
                    "year",
                    "bene_id_prefix",
                ]

                # Check that base columns exist
                missing_base_cols = [col for col in base_cols if col not in df.columns]
                if missing_base_cols:
                    logger.warning(
                        f"Missing required columns: {missing_base_cols} in {file}"
                    )
                    continue

                # Create a separate dataframe for each diagnosis position
                for i, diag_col in enumerate(diag_cols):
                    try:
                        # Create a dataframe for this diagnosis position
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
                                    self._get_icd9_description, return_dtype=pl.String
                                )
                                .alias("diagnosis_description")
                            ]
                        )

                        diagnosis_dfs.append(pos_df)
                    except Exception as e:
                        logger.error(
                            f"Error processing diagnosis column {diag_col}: {e}"
                        )

        if not diagnosis_dfs:
            logger.error("No data found for fact_claim_diagnoses")
            raise ValueError("No data found for fact_claim_diagnoses")

        # Combine all diagnosis dataframes
        try:
            logger.info(f"Combining {len(diagnosis_dfs)} diagnosis dataframes")
            combined_diagnoses = pl.concat(diagnosis_dfs)
            logger.info(
                f"Combined diagnosis dataframe shape: {combined_diagnoses.shape}"
            )

            # Write to parquet partitioned by year and bene_id_prefix
            for year_val, year_df in combined_diagnoses.partition_by(
                "year", as_dict=True
            ).items():
                for prefix, prefix_df in year_df.partition_by(
                    "bene_id_prefix", as_dict=True
                ).items():
                    output_path = (
                        self.silver_dir
                        / "fact_claim_diagnoses"
                        / f"year={year_val}"
                        / f"bene_id_prefix={prefix}"
                    )
                    output_path.mkdir(parents=True, exist_ok=True)

                    prefix_df.write_parquet(
                        output_path / "fact_claim_diagnoses.parquet",
                        compression="zstd",
                        statistics=True,
                        use_pyarrow=True,
                    )

            logger.info(
                f"Successfully created fact_claim_diagnoses with {len(combined_diagnoses)} rows"
            )
        except Exception as e:
            logger.error(f"Error creating fact_claim_diagnoses: {e}")
            raise e

    def create_fact_prescription(self):
        """
        Create the prescription fact table from PDE (Prescription Drug Event) files.
        """
        logger.info("Creating fact_prescription table")

        pde_dfs = []

        # Process prescription drug events
        for file in self._get_files_by_type("pde"):
            logger.info(f"Reading prescription data: {file}")

            try:
                df = pl.read_parquet(file)

                # Debug: Print column names
                logger.info(f"PDE file columns: {df.columns}")

                # Define required columns and their aliases
                column_mapping = {
                    "DESYNPUF_ID": "bene_id",
                    "CLM_ID": "prescription_id",
                    "PDE_ID": "prescription_id",  # Alternative column for prescription ID
                    "SRVC_DT": "service_date",
                    "QTY_DSPNSD_NUM": "quantity_dispensed",
                    "DAYS_SUPLY_NUM": "days_supply",
                    "PTNT_PAY_AMT": "patient_payment",
                    "TOT_RX_CST_AMT": "total_cost",
                }

                # Check for provider column - could be different names
                provider_col = None
                for col_name in ["PRVDR_ID", "PRSCRBR_ID", "PHRMCY_ID"]:
                    if col_name in df.columns:
                        provider_col = col_name
                        break

                if provider_col:
                    column_mapping[provider_col] = "provider_id"
                else:
                    logger.warning(f"No provider column found in {file}")
                    # Will use a placeholder

                # Check for product ID column - could be different names
                product_col = None
                for col_name in ["PROD_SRVC_ID", "PRDUCT_ID", "NDC"]:
                    if col_name in df.columns:
                        product_col = col_name
                        break

                if product_col:
                    column_mapping[product_col] = "product_id"
                else:
                    logger.warning(f"No product ID column found in {file}")
                    # Will use a placeholder

                # Check that required columns exist
                missing_cols = [
                    col
                    for col in ["DESYNPUF_ID", "SRVC_DT", "TOT_RX_CST_AMT"]
                    if col not in df.columns
                ]

                # We need either CLM_ID or PDE_ID
                if "CLM_ID" not in df.columns and "PDE_ID" not in df.columns:
                    missing_cols.append("CLM_ID/PDE_ID")

                if missing_cols:
                    logger.warning(
                        f"Missing required columns: {missing_cols} in {file}"
                    )
                    continue

                # Build select expressions
                select_exprs = []

                # Add mapped columns that exist
                for source_col, target_name in column_mapping.items():
                    if source_col in df.columns:
                        select_exprs.append(pl.col(source_col).alias(target_name))

                # Add placeholder for provider_id if not found
                if provider_col is None:
                    select_exprs.append(pl.lit("Unknown").alias("provider_id"))

                # Add placeholder for product_id if not found
                if product_col is None:
                    select_exprs.append(pl.lit("Unknown").alias("product_id"))

                # Add year and bene_id_prefix
                select_exprs.extend(
                    [
                        pl.col("year"),
                        pl.col("bene_id_prefix"),
                    ]
                )

                # Select and rename columns
                df = df.select(select_exprs)

                # Add default values for optional columns if missing
                if "quantity_dispensed" not in df.columns:
                    df = df.with_columns([pl.lit(1.0).alias("quantity_dispensed")])

                if "days_supply" not in df.columns:
                    df = df.with_columns([pl.lit(30).alias("days_supply")])

                if "patient_payment" not in df.columns:
                    df = df.with_columns([pl.lit(0.0).alias("patient_payment")])

                pde_dfs.append(df)

            except Exception as e:
                logger.error(f"Error processing PDE file {file}: {e}")
                raise e

        if not pde_dfs:
            logger.error("No data found for fact_prescription")
            raise ValueError("No data found for fact_prescription")

        # Combine all prescription dataframes
        try:
            logger.info(f"Combining {len(pde_dfs)} PDE dataframes")
            combined_prescriptions = pl.concat(pde_dfs)
            logger.info(f"Combined PDE dataframe shape: {combined_prescriptions.shape}")

            # Calculate medicare payment (total cost - patient payment)
            combined_prescriptions = combined_prescriptions.with_columns(
                [
                    (pl.col("total_cost") - pl.col("patient_payment")).alias(
                        "medicare_payment"
                    )
                ]
            )

            # Write to parquet partitioned by year and bene_id_prefix
            for year_val, year_df in combined_prescriptions.partition_by(
                "year", as_dict=True
            ).items():
                for prefix, prefix_df in year_df.partition_by(
                    "bene_id_prefix", as_dict=True
                ).items():
                    output_path = (
                        self.silver_dir
                        / "fact_prescription"
                        / f"year={year_val}"
                        / f"bene_id_prefix={prefix}"
                    )
                    output_path.mkdir(parents=True, exist_ok=True)

                    prefix_df.write_parquet(
                        output_path / "fact_prescription.parquet",
                        compression="zstd",
                        statistics=True,
                        use_pyarrow=True,
                    )

            logger.info(
                f"Successfully created fact_prescription with {len(combined_prescriptions)} rows"
            )
        except Exception as e:
            logger.error(f"Error creating fact_prescription: {e}")
            raise e

    def create_dim_provider(self):
        """
        Create the provider dimension table.

        This table extracts and deduplicates provider information from claims.
        """
        logger.info("Creating dim_provider table")

        provider_dfs = []

        # Process provider IDs from claims
        for claim_type in ["inpatient", "outpatient", "carrier"]:
            for file in self._get_files_by_type(claim_type):
                logger.info(f"Extracting providers from {claim_type}: {file}")
                df = pl.read_parquet(file)

                # Debug: Print column names
                logger.info(f"Columns in {claim_type} file: {df.columns}")

                # List of potential provider columns
                potential_provider_cols = [
                    "PRVDR_NUM",
                    "AT_PHYSN_NPI",
                    "OP_PHYSN_NPI",
                    "OT_PHYSN_NPI",
                    "PRVDR_NPI",
                ]

                # Find provider columns that actually exist in this file
                provider_cols = [
                    col for col in potential_provider_cols if col in df.columns
                ]

                if not provider_cols:
                    logger.warning(f"No provider columns found in {file}")
                    continue

                # Check if state column exists
                state_col = None
                for col_name in ["PRVDR_STATE_CD", "PRVDR_STATE"]:
                    if col_name in df.columns:
                        state_col = col_name
                        break

                # Create a dataframe for providers
                for provider_col in provider_cols:
                    # Extract provider info
                    if state_col:
                        provider_df = df.select(
                            [
                                pl.col(provider_col).alias("provider_id"),
                                pl.col(state_col)
                                .alias("state")
                                .fill_null("Unknown")
                                .fill_nan("Unknown"),
                            ]
                        )
                    else:
                        # If state column doesn't exist, use "Unknown"
                        provider_df = df.select(
                            [
                                pl.col(provider_col).alias("provider_id"),
                                pl.lit("Unknown").alias("state"),
                            ]
                        )

                    # Filter out empty provider IDs
                    provider_df = provider_df.filter(
                        (pl.col("provider_id").is_not_null())
                        & (pl.col("provider_id") != "")
                    )

                    provider_dfs.append(provider_df)

        # Process provider IDs from prescriptions
        for file in self._get_files_by_type("pde"):
            logger.info(f"Extracting providers from prescriptions: {file}")
            df = pl.read_parquet(file)

            # Debug: Print column names
            logger.info(f"Columns in pde file: {df.columns}")

            # Find provider column in PDE file
            pde_provider_col = None
            for col_name in ["PRVDR_ID", "PRSCRBR_ID"]:
                if col_name in df.columns:
                    pde_provider_col = col_name
                    break

            if pde_provider_col:
                # Extract provider info
                provider_df = df.select(
                    [
                        pl.col(pde_provider_col).alias("provider_id"),
                        pl.lit("Unknown").alias("state"),
                    ]
                )

                # Filter out empty provider IDs
                provider_df = provider_df.filter(
                    (pl.col("provider_id").is_not_null())
                    & (pl.col("provider_id") != "")
                )

                provider_dfs.append(provider_df)
            else:
                logger.warning(f"No provider column found in PDE file: {file}")

        # Combine and deduplicate provider information
        if not provider_dfs:
            logger.error("No data found for dim_provider")
            raise ValueError("No data found for dim_provider")

        combined_providers = pl.concat(provider_dfs)

        # Deduplicate providers and keep the most common state
        unique_providers = combined_providers.group_by("provider_id").agg(
            [pl.col("state").mode().first().alias("state")]
        )

        # Add provider type (placeholder - this would use additional reference data in production)
        unique_providers = unique_providers.with_columns(
            [pl.lit("Unknown").alias("provider_type")]
        )

        # Write to parquet
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
        """
        Execute the complete transformation process.
        """
        logger.info("Starting data transformation process")

        # Create dimension tables
        self.create_dim_beneficiary()
        self.create_dim_provider()

        # Create fact tables
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
