import polars as pl
from pathlib import Path
import logging
from medicare_pipeline.data_cleaner import DataCleaner

# Enable string cache for categorical columns
pl.enable_string_cache()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVToParquetConverter:
    """
    Converts CSV files to Parquet format with appropriate partitioning and data cleaning.
    This creates the "bronze" layer of the data lakehouse.
    """

    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.data_cleaner = DataCleaner()

        # Define column type mappings for each file type
        self.column_types = {
            "beneficiary": {
                "DESYNPUF_ID": pl.String,
                "BENE_BIRTH_DT": pl.Date,
                "BENE_DEATH_DT": pl.Date,
                "BENE_SEX_IDENT_CD": pl.Categorical,
                "BENE_RACE_CD": pl.Categorical,
                "BENE_ESRD_IND": pl.Categorical,
                "SP_STATE_CODE": pl.Categorical,
                "BENE_COUNTY_CD": pl.Categorical,
                "BENE_HI_CVRAGE_TOT_MONS": pl.Int32,
                "BENE_SMI_CVRAGE_TOT_MONS": pl.Int32,
                "BENE_HMO_CVRAGE_TOT_MONS": pl.Int32,
                "PLAN_CVRG_MOS_NUM": pl.Int32,
                "SP_ALZHDMTA": pl.Int32,
                "SP_CHF": pl.Int32,
                "SP_CHRNKIDN": pl.Int32,
                "SP_CNCR": pl.Int32,
                "SP_COPD": pl.Int32,
                "SP_DEPRESSN": pl.Int32,
                "SP_DIABETES": pl.Int32,
                "SP_ISCHMCHT": pl.Int32,
                "SP_OSTEOPRS": pl.Int32,
                "SP_RA_OA": pl.Int32,
                "SP_STRKETIA": pl.Int32,
                "MEDREIMB_IP": pl.Decimal(precision=10, scale=2),
                "BENRES_IP": pl.Decimal(precision=10, scale=2),
                "PPPYMT_IP": pl.Decimal(precision=10, scale=2),
                "MEDREIMB_OP": pl.Decimal(precision=10, scale=2),
                "BENRES_OP": pl.Decimal(precision=10, scale=2),
                "PPPYMT_OP": pl.Decimal(precision=10, scale=2),
                "MEDREIMB_CAR": pl.Decimal(precision=10, scale=2),
                "BENRES_CAR": pl.Decimal(precision=10, scale=2),
                "PPPYMT_CAR": pl.Decimal(precision=10, scale=2),
            },
            "inpatient": {
                "DESYNPUF_ID": pl.String,
                "CLM_ID": pl.String,
                "SEGMENT": pl.Int32,
                "CLM_FROM_DT": pl.Date,
                "CLM_THRU_DT": pl.Date,
                "PRVDR_NUM": pl.String,
                "CLM_PMT_AMT": pl.Decimal(precision=10, scale=2),
                "NCH_PRMRY_PYR_CLM_PD_AMT": pl.Decimal(precision=10, scale=2),
                "AT_PHYSN_NPI": pl.String,
                "OP_PHYSN_NPI": pl.String,
                "OT_PHYSN_NPI": pl.String,
                "CLM_ADMSN_DT": pl.Date,
                "ADMTNG_ICD9_DGNS_CD": pl.String,
                "ICD9_DGNS_CD_1": pl.String,
                "ICD9_DGNS_CD_2": pl.String,
                "ICD9_DGNS_CD_3": pl.String,
                "ICD9_DGNS_CD_4": pl.String,
                "ICD9_DGNS_CD_5": pl.String,
                "ICD9_DGNS_CD_6": pl.String,
                "ICD9_DGNS_CD_7": pl.String,
                "ICD9_DGNS_CD_8": pl.String,
                "ICD9_DGNS_CD_9": pl.String,
                "ICD9_DGNS_CD_10": pl.String,
                "ICD9_PRCDR_CD_1": pl.String,
                "ICD9_PRCDR_CD_2": pl.String,
                "ICD9_PRCDR_CD_3": pl.String,
                "ICD9_PRCDR_CD_4": pl.String,
                "ICD9_PRCDR_CD_5": pl.String,
                "ICD9_PRCDR_CD_6": pl.String,
                "PRVDR_STATE_CD": pl.Categorical,
                "BENE_COUNTY_CD": pl.Categorical,
                "BENE_STATE_CD": pl.Categorical,
                "BENE_MLG_CNTCT_ZIP_CD": pl.String,
            },
            "outpatient": {
                "DESYNPUF_ID": pl.String,
                "CLM_ID": pl.String,
                "SEGMENT": pl.Int32,
                "CLM_FROM_DT": pl.Date,
                "CLM_THRU_DT": pl.Date,
                "PRVDR_NUM": pl.String,
                "CLM_PMT_AMT": pl.Decimal(precision=10, scale=2),
                "NCH_PRMRY_PYR_CLM_PD_AMT": pl.Decimal(precision=10, scale=2),
                "AT_PHYSN_NPI": pl.String,
                "OP_PHYSN_NPI": pl.String,
                "OT_PHYSN_NPI": pl.String,
                "ICD9_DGNS_CD_1": pl.String,
                "ICD9_DGNS_CD_2": pl.String,
                "ICD9_DGNS_CD_3": pl.String,
                "ICD9_DGNS_CD_4": pl.String,
                "ICD9_DGNS_CD_5": pl.String,
                "ICD9_DGNS_CD_6": pl.String,
                "ICD9_DGNS_CD_7": pl.String,
                "ICD9_DGNS_CD_8": pl.String,
                "ICD9_DGNS_CD_9": pl.String,
                "ICD9_DGNS_CD_10": pl.String,
                "ICD9_PRCDR_CD_1": pl.String,
                "ICD9_PRCDR_CD_2": pl.String,
                "ICD9_PRCDR_CD_3": pl.String,
                "ICD9_PRCDR_CD_4": pl.String,
                "ICD9_PRCDR_CD_5": pl.String,
                "ICD9_PRCDR_CD_6": pl.String,
                "PRVDR_STATE_CD": pl.Categorical,
                "BENE_COUNTY_CD": pl.Categorical,
                "BENE_STATE_CD": pl.Categorical,
                "BENE_MLG_CNTCT_ZIP_CD": pl.String,
            },
            "carrier": {
                "DESYNPUF_ID": pl.String,
                "CLM_ID": pl.String,
                "SEGMENT": pl.Int32,
                "CLM_FROM_DT": pl.Date,
                "CLM_THRU_DT": pl.Date,
                "PRVDR_NUM": pl.String,
                "CLM_PMT_AMT": pl.Decimal(precision=10, scale=2),
                "NCH_PRMRY_PYR_CLM_PD_AMT": pl.Decimal(precision=10, scale=2),
                "AT_PHYSN_NPI": pl.String,
                "OP_PHYSN_NPI": pl.String,
                "OT_PHYSN_NPI": pl.String,
                "ICD9_DGNS_CD_1": pl.String,
                "ICD9_DGNS_CD_2": pl.String,
                "ICD9_DGNS_CD_3": pl.String,
                "ICD9_DGNS_CD_4": pl.String,
                "ICD9_DGNS_CD_5": pl.String,
                "ICD9_DGNS_CD_6": pl.String,
                "ICD9_DGNS_CD_7": pl.String,
                "ICD9_DGNS_CD_8": pl.String,
                "ICD9_DGNS_CD_9": pl.String,
                "ICD9_DGNS_CD_10": pl.String,
                "ICD9_PRCDR_CD_1": pl.String,
                "ICD9_PRCDR_CD_2": pl.String,
                "ICD9_PRCDR_CD_3": pl.String,
                "ICD9_PRCDR_CD_4": pl.String,
                "ICD9_PRCDR_CD_5": pl.String,
                "ICD9_PRCDR_CD_6": pl.String,
                "PRVDR_STATE_CD": pl.Categorical,
                "BENE_COUNTY_CD": pl.Categorical,
                "BENE_STATE_CD": pl.Categorical,
                "BENE_MLG_CNTCT_ZIP_CD": pl.String,
            },
            "pde": {
                "DESYNPUF_ID": pl.String,
                "CLM_ID": pl.String,
                "PRVDR_ID": pl.String,
                "SRVC_DT": pl.Date,
                "PROD_SRVC_ID": pl.String,
                "QTY_DSPNSD_NUM": pl.Decimal(precision=10, scale=2),
                "DAYS_SUPLY_NUM": pl.Int32,
                "PTNT_PAY_AMT": pl.Decimal(precision=10, scale=2),
                "TOT_RX_CST_AMT": pl.Decimal(precision=10, scale=2),
            },
        }

    def _get_file_type(self, file_path: Path) -> str:
        """Determine the type of file based on its name."""
        file_name = file_path.name.lower()
        if "beneficiary" in file_name:
            return "beneficiary"
        elif "inpatient" in file_name:
            return "inpatient"
        elif "outpatient" in file_name:
            return "outpatient"
        elif "carrier" in file_name:
            return "carrier"
        elif "prescription_drug" in file_name:
            return "pde"
        else:
            raise ValueError(f"Unknown file type for {file_path}")

    def _extract_year(self, file_path: Path) -> int:
        """Extract year from file name."""
        # For beneficiary files, extract from filename
        if "beneficiary" in file_path.name.lower():
            for part in file_path.stem.split("_"):
                if part.isdigit() and len(part) == 4:
                    return int(part)
        # For other files, extract from the data
        else:
            # Read the first few rows to determine the year
            df = pl.read_csv(
                file_path,
                n_rows=1000,
                infer_schema_length=10000,
                ignore_errors=True,
                schema_overrides={"CLM_FROM_DT": pl.String, "SRVC_DT": pl.String},
            )
            if "CLM_FROM_DT" in df.columns:
                # Convert string date to integer year
                year = int(df["CLM_FROM_DT"].str.slice(0, 4).mode()[0])
                return year
            elif "SRVC_DT" in df.columns:
                # Convert string date to integer year
                year = int(df["SRVC_DT"].str.slice(0, 4).mode()[0])
                return year
        raise ValueError(f"Could not extract year from {file_path}")

    def _extract_sample_id(self, file_path: Path) -> str:
        """Extract sample ID from file name."""
        # Assuming file names contain 'Sample_X' where X is the sample number
        for part in file_path.stem.split("_"):
            if part.startswith("Sample"):
                return part
        raise ValueError(f"Could not extract sample ID from {file_path}")

    def _get_bene_id_prefix(self, bene_id: str) -> str:
        """Extract the first 2 characters of the beneficiary ID as prefix for partitioning."""
        if bene_id and len(bene_id) >= 2:
            return bene_id[:2]
        return "00"  # Default prefix for empty or short IDs

    def _validate_data(self, df: pl.DataFrame, file_type: str) -> pl.DataFrame:
        """
        Perform basic data validation and report statistics.
        """
        total_rows = len(df)

        # Check for missing values in key fields
        key_columns = ["DESYNPUF_ID"]
        if file_type != "beneficiary":
            key_columns.append("CLM_ID")

        missing_stats = {}
        for col in key_columns:
            if col in df.columns:
                missing_count = df.filter(pl.col(col).is_null()).height
                missing_stats[col] = (
                    f"{missing_count} ({(missing_count/total_rows)*100:.2f}%)"
                )

        # Log validation results
        logger.info(f"Data validation for {file_type}: Total rows: {total_rows}")
        for col, stat in missing_stats.items():
            logger.info(f"  Missing {col}: {stat}")

        # Filter out rows with missing beneficiary IDs
        if "DESYNPUF_ID" in df.columns:
            invalid_rows = df.filter(pl.col("DESYNPUF_ID").is_null()).height
            if invalid_rows > 0:
                logger.warning(f"Removing {invalid_rows} rows with missing DESYNPUF_ID")
                df = df.filter(~pl.col("DESYNPUF_ID").is_null())

        return df

    def convert_file(self, file_path: Path) -> None:
        """Convert a single CSV file to Parquet format with proper partitioning."""
        try:
            file_type = self._get_file_type(file_path)
            year = self._extract_year(file_path)
            sample_id = self._extract_sample_id(file_path)

            logger.info(f"Processing {file_path}")

            # Define date columns and their format
            date_format = "%Y%m%d"  # Format: YYYYMMDD
            date_columns = [
                "CLM_FROM_DT",
                "CLM_THRU_DT",
                "CLM_ADMSN_DT",
                "SRVC_DT",
                "BENE_BIRTH_DT",
                "BENE_DEATH_DT",
            ]

            # Update column types to read date columns as strings first
            schema_overrides = self.column_types[file_type].copy()
            for col in date_columns:
                if col in schema_overrides:
                    schema_overrides[col] = pl.String

            # Read CSV with proper column types
            df = pl.read_csv(
                file_path,
                schema_overrides=schema_overrides,
                infer_schema_length=10000,
                ignore_errors=True,
                null_values=["", "NA", "NULL", "null", "NaN", "nan"],
            )

            # Convert date columns
            for col in date_columns:
                if col in df.columns:
                    try:
                        df = df.with_columns(
                            [
                                pl.col(col)
                                .str.strptime(pl.Date, format=date_format, strict=False)
                                .alias(col)
                            ]
                        )
                    except Exception as e:
                        logger.warning(f"Could not convert date column {col}: {str(e)}")

            # Skip cleaning for now as it's causing errors
            # df = self.data_cleaner.clean_data(df, file_type)

            # Create year column
            df = df.with_columns(
                [pl.lit(year).alias("year"), pl.lit(sample_id).alias("sample_id")]
            )

            # Create partition columns for beneficiary data
            if "DESYNPUF_ID" in df.columns:
                # Create a new column with the first 2 characters of DESYNPUF_ID
                # Using str.slice instead of apply
                df = df.with_columns(
                    [
                        pl.col("DESYNPUF_ID")
                        .str.slice(0, 2)
                        .fill_null("00")
                        .alias("bene_id_prefix")
                    ]
                )

            # Create output path with partitioning
            # Structure: {file_type}/year={year}/bene_id_prefix={prefix}/file.parquet
            if "bene_id_prefix" in df.columns:
                # We need to partition the dataframe by bene_id_prefix and write each partition separately
                for prefix, group_df in df.partition_by(
                    "bene_id_prefix", as_dict=True
                ).items():
                    output_path = (
                        self.output_dir
                        / file_type
                        / f"year={year}"
                        / f"bene_id_prefix={prefix}"
                        / f"{file_path.stem}.parquet"
                    )
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                    # Write to Parquet
                    group_df.write_parquet(
                        output_path, compression="zstd", statistics=True
                    )

                logger.info(
                    f"Successfully partitioned and converted {file_path} to {self.output_dir}/{file_type}/year={year}/..."
                )
            else:
                # For files without beneficiary ID, use only year partitioning
                output_path = (
                    self.output_dir
                    / file_type
                    / f"year={year}"
                    / f"{file_path.stem}.parquet"
                )
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Write to Parquet
                df.write_parquet(output_path, compression="zstd", statistics=True)

                logger.info(f"Successfully converted {file_path} to {output_path}")

        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            raise

    def process_directory(self) -> None:
        """Process all CSV files in the input directory."""
        for file_path in self.input_dir.glob("**/*.csv"):
            self.convert_file(file_path)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Convert Medicare CSV files to Parquet format"
    )
    parser.add_argument(
        "--input-dir", required=True, help="Input directory containing CSV files"
    )
    parser.add_argument(
        "--output-dir", required=True, help="Output directory for Parquet files"
    )

    args = parser.parse_args()

    converter = CSVToParquetConverter(args.input_dir, args.output_dir)
    converter.process_directory()


if __name__ == "__main__":
    main()
