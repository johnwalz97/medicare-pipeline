import polars as pl
from pathlib import Path
import logging

# Enable string cache for categorical columns
pl.enable_string_cache()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVToParquetConverter:
    """
    Converts CSV files to Parquet format with appropriate partitioning and data cleaning.
    This creates the "bronze" layer of the data lakehouse.

    Also handles data normalization tasks for Medicare claims data, including:
    - Handling missing values
    - Normalizing categorical values
    - Type conversions
    - Data quality checks
    """

    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Explicitly define column types for each file type (don't have to specify all columns but its a nice reference)
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
                "CLM_PASS_THRU_PER_DIEM_AMT": pl.Decimal(precision=10, scale=2),
                "NCH_BENE_IP_DDCTBL_AMT": pl.Decimal(precision=10, scale=2),
                "NCH_BENE_PTA_COINSRNC_LBLTY_AM": pl.Decimal(precision=10, scale=2),
                "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM": pl.Decimal(precision=10, scale=2),
                "CLM_UTLZTN_DAY_CNT": pl.Int32,
                "NCH_BENE_DSCHRG_DT": pl.Date,
                "CLM_DRG_CD": pl.String,
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
                "HCPCS_CD_1": pl.String,
                "HCPCS_CD_2": pl.String,
                "HCPCS_CD_3": pl.String,
                "HCPCS_CD_4": pl.String,
                "HCPCS_CD_5": pl.String,
                "HCPCS_CD_6": pl.String,
                "HCPCS_CD_7": pl.String,
                "HCPCS_CD_8": pl.String,
                "HCPCS_CD_9": pl.String,
                "HCPCS_CD_10": pl.String,
                "HCPCS_CD_11": pl.String,
                "HCPCS_CD_12": pl.String,
                "HCPCS_CD_13": pl.String,
                "HCPCS_CD_14": pl.String,
                "HCPCS_CD_15": pl.String,
                "HCPCS_CD_16": pl.String,
                "HCPCS_CD_17": pl.String,
                "HCPCS_CD_18": pl.String,
                "HCPCS_CD_19": pl.String,
                "HCPCS_CD_20": pl.String,
                "HCPCS_CD_21": pl.String,
                "HCPCS_CD_22": pl.String,
                "HCPCS_CD_23": pl.String,
                "HCPCS_CD_24": pl.String,
                "HCPCS_CD_25": pl.String,
                "HCPCS_CD_26": pl.String,
                "HCPCS_CD_27": pl.String,
                "HCPCS_CD_28": pl.String,
                "HCPCS_CD_29": pl.String,
                "HCPCS_CD_30": pl.String,
                "HCPCS_CD_31": pl.String,
                "HCPCS_CD_32": pl.String,
                "HCPCS_CD_33": pl.String,
                "HCPCS_CD_34": pl.String,
                "HCPCS_CD_35": pl.String,
                "HCPCS_CD_36": pl.String,
                "HCPCS_CD_37": pl.String,
                "HCPCS_CD_38": pl.String,
                "HCPCS_CD_39": pl.String,
                "HCPCS_CD_40": pl.String,
                "HCPCS_CD_41": pl.String,
                "HCPCS_CD_42": pl.String,
                "HCPCS_CD_43": pl.String,
                "HCPCS_CD_44": pl.String,
                "HCPCS_CD_45": pl.String,
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
                "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM": pl.Decimal(precision=10, scale=2),
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
                "NCH_BENE_PTB_DDCTBL_AMT": pl.Decimal(precision=10, scale=2),
                "NCH_BENE_PTB_COINSRNC_AMT": pl.Decimal(precision=10, scale=2),
                "ADMTNG_ICD9_DGNS_CD": pl.String,
                "HCPCS_CD_1": pl.String,
                "HCPCS_CD_2": pl.String,
                "HCPCS_CD_3": pl.String,
                "HCPCS_CD_4": pl.String,
                "HCPCS_CD_5": pl.String,
                "HCPCS_CD_6": pl.String,
                "HCPCS_CD_7": pl.String,
                "HCPCS_CD_8": pl.String,
                "HCPCS_CD_9": pl.String,
                "HCPCS_CD_10": pl.String,
                "HCPCS_CD_11": pl.String,
                "HCPCS_CD_12": pl.String,
                "HCPCS_CD_13": pl.String,
                "HCPCS_CD_14": pl.String,
                "HCPCS_CD_15": pl.String,
                "HCPCS_CD_16": pl.String,
                "HCPCS_CD_17": pl.String,
                "HCPCS_CD_18": pl.String,
                "HCPCS_CD_19": pl.String,
                "HCPCS_CD_20": pl.String,
                "HCPCS_CD_21": pl.String,
                "HCPCS_CD_22": pl.String,
                "HCPCS_CD_23": pl.String,
                "HCPCS_CD_24": pl.String,
                "HCPCS_CD_25": pl.String,
                "HCPCS_CD_26": pl.String,
                "HCPCS_CD_27": pl.String,
                "HCPCS_CD_28": pl.String,
                "HCPCS_CD_29": pl.String,
                "HCPCS_CD_30": pl.String,
                "HCPCS_CD_31": pl.String,
                "HCPCS_CD_32": pl.String,
                "HCPCS_CD_33": pl.String,
                "HCPCS_CD_34": pl.String,
                "HCPCS_CD_35": pl.String,
                "HCPCS_CD_36": pl.String,
                "HCPCS_CD_37": pl.String,
                "HCPCS_CD_38": pl.String,
                "HCPCS_CD_39": pl.String,
                "HCPCS_CD_40": pl.String,
                "HCPCS_CD_41": pl.String,
                "HCPCS_CD_42": pl.String,
                "HCPCS_CD_43": pl.String,
                "HCPCS_CD_44": pl.String,
                "HCPCS_CD_45": pl.String,
            },
            "carrier": {
                "DESYNPUF_ID": pl.String,
                "CLM_ID": pl.String,
                "CLM_FROM_DT": pl.Date,
                "CLM_THRU_DT": pl.Date,
                "ICD9_DGNS_CD_1": pl.String,
                "ICD9_DGNS_CD_2": pl.String,
                "ICD9_DGNS_CD_3": pl.String,
                "ICD9_DGNS_CD_4": pl.String,
                "ICD9_DGNS_CD_5": pl.String,
                "ICD9_DGNS_CD_6": pl.String,
                "ICD9_DGNS_CD_7": pl.String,
                "ICD9_DGNS_CD_8": pl.String,
                "PRF_PHYSN_NPI_1": pl.String,
                "PRF_PHYSN_NPI_2": pl.String,
                "PRF_PHYSN_NPI_3": pl.String,
                "PRF_PHYSN_NPI_4": pl.String,
                "PRF_PHYSN_NPI_5": pl.String,
                "PRF_PHYSN_NPI_6": pl.String,
                "PRF_PHYSN_NPI_7": pl.String,
                "PRF_PHYSN_NPI_8": pl.String,
                "PRF_PHYSN_NPI_9": pl.String,
                "PRF_PHYSN_NPI_10": pl.String,
                "PRF_PHYSN_NPI_11": pl.String,
                "PRF_PHYSN_NPI_12": pl.String,
                "PRF_PHYSN_NPI_13": pl.String,
                "TAX_NUM_1": pl.String,
                "TAX_NUM_2": pl.String,
                "TAX_NUM_3": pl.String,
                "TAX_NUM_4": pl.String,
                "TAX_NUM_5": pl.String,
                "TAX_NUM_6": pl.String,
                "TAX_NUM_7": pl.String,
                "TAX_NUM_8": pl.String,
                "TAX_NUM_9": pl.String,
                "TAX_NUM_10": pl.String,
                "TAX_NUM_11": pl.String,
                "TAX_NUM_12": pl.String,
                "TAX_NUM_13": pl.String,
                "HCPCS_CD_1": pl.String,
                "HCPCS_CD_2": pl.String,
                "HCPCS_CD_3": pl.String,
                "HCPCS_CD_4": pl.String,
                "HCPCS_CD_5": pl.String,
                "HCPCS_CD_6": pl.String,
                "HCPCS_CD_7": pl.String,
                "HCPCS_CD_8": pl.String,
                "HCPCS_CD_9": pl.String,
                "HCPCS_CD_10": pl.String,
                "HCPCS_CD_11": pl.String,
                "HCPCS_CD_12": pl.String,
                "HCPCS_CD_13": pl.String,
                "LINE_NCH_PMT_AMT_1": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_2": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_3": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_4": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_5": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_6": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_7": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_8": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_9": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_10": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_11": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_12": pl.Decimal(precision=10, scale=2),
                "LINE_NCH_PMT_AMT_13": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_1": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_2": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_3": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_4": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_5": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_6": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_7": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_8": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_9": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_10": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_11": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_12": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PTB_DDCTBL_AMT_13": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_1": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_2": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_3": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_4": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_5": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_6": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_7": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_8": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_9": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_10": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_11": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_12": pl.Decimal(precision=10, scale=2),
                "LINE_BENE_PRMRY_PYR_PD_AMT_13": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_1": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_2": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_3": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_4": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_5": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_6": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_7": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_8": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_9": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_10": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_11": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_12": pl.Decimal(precision=10, scale=2),
                "LINE_COINSRNC_AMT_13": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_1": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_2": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_3": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_4": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_5": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_6": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_7": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_8": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_9": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_10": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_11": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_12": pl.Decimal(precision=10, scale=2),
                "LINE_ALOWD_CHRG_AMT_13": pl.Decimal(precision=10, scale=2),
                "LINE_PRCSG_IND_CD_1": pl.String,
                "LINE_PRCSG_IND_CD_2": pl.String,
                "LINE_PRCSG_IND_CD_3": pl.String,
                "LINE_PRCSG_IND_CD_4": pl.String,
                "LINE_PRCSG_IND_CD_5": pl.String,
                "LINE_PRCSG_IND_CD_6": pl.String,
                "LINE_PRCSG_IND_CD_7": pl.String,
                "LINE_PRCSG_IND_CD_8": pl.String,
                "LINE_PRCSG_IND_CD_9": pl.String,
                "LINE_PRCSG_IND_CD_10": pl.String,
                "LINE_PRCSG_IND_CD_11": pl.String,
                "LINE_PRCSG_IND_CD_12": pl.String,
                "LINE_PRCSG_IND_CD_13": pl.String,
                "LINE_ICD9_DGNS_CD_1": pl.String,
                "LINE_ICD9_DGNS_CD_2": pl.String,
                "LINE_ICD9_DGNS_CD_3": pl.String,
                "LINE_ICD9_DGNS_CD_4": pl.String,
                "LINE_ICD9_DGNS_CD_5": pl.String,
                "LINE_ICD9_DGNS_CD_6": pl.String,
                "LINE_ICD9_DGNS_CD_7": pl.String,
                "LINE_ICD9_DGNS_CD_8": pl.String,
                "LINE_ICD9_DGNS_CD_9": pl.String,
                "LINE_ICD9_DGNS_CD_10": pl.String,
                "LINE_ICD9_DGNS_CD_11": pl.String,
                "LINE_ICD9_DGNS_CD_12": pl.String,
                "LINE_ICD9_DGNS_CD_13": pl.String,
            },
            "pde": {
                "DESYNPUF_ID": pl.String,
                "PDE_ID": pl.String,
                "SRVC_DT": pl.Date,
                "PROD_SRVC_ID": pl.String,
                "QTY_DSPNSD_NUM": pl.Decimal(precision=10, scale=2),
                "DAYS_SUPLY_NUM": pl.Int32,
                "PTNT_PAY_AMT": pl.Decimal(precision=10, scale=2),
                "TOT_RX_CST_AMT": pl.Decimal(precision=10, scale=2),
            },
        }

        # Define mappings for categorical columns
        self.sex_mapping = {"1": "Male", "2": "Female"}

        self.race_mapping = {
            "1": "White",
            "2": "Black",
            "3": "Others",
            "5": "Hispanic",
        }

        self.state_mapping = {
            "01": "AL",  # Alabama
            "02": "AK",  # Alaska
            "03": "AZ",  # Arizona
            "04": "AR",  # Arkansas
            "05": "CA",  # California
            "06": "CO",  # Colorado
            "07": "CT",  # Connecticut
            "08": "DE",  # Delaware
            "09": "DC",  # District of Columbia
            "10": "FL",  # Florida
            "11": "GA",  # Georgia
            "12": "HI",  # Hawaii
            "13": "ID",  # Idaho
            "14": "IL",  # Illinois
            "15": "IN",  # Indiana
            "16": "IA",  # Iowa
            "17": "KS",  # Kansas
            "18": "KY",  # Kentucky
            "19": "LA",  # Louisiana
            "20": "ME",  # Maine
            "21": "MD",  # Maryland
            "22": "MA",  # Massachusetts
            "23": "MI",  # Michigan
            "24": "MN",  # Minnesota
            "25": "MS",  # Mississippi
            "26": "MO",  # Missouri
            "27": "MT",  # Montana
            "28": "NE",  # Nebraska
            "29": "NV",  # Nevada
            "30": "NH",  # New Hampshire
            "31": "NJ",  # New Jersey
            "32": "NM",  # New Mexico
            "33": "NY",  # New York
            "34": "NC",  # North Carolina
            "35": "ND",  # North Dakota
            "36": "OH",  # Ohio
            "37": "OK",  # Oklahoma
            "38": "OR",  # Oregon
            "39": "PA",  # Pennsylvania
            "40": "RI",  # Rhode Island
            "41": "SC",  # South Carolina
            "42": "SD",  # South Dakota
            "43": "TN",  # Tennessee
            "44": "TX",  # Texas
            "45": "UT",  # Utah
            "46": "VT",  # Vermont
            "47": "VA",  # Virginia
            "48": "WA",  # Washington
            "49": "WV",  # West Virginia
            "50": "WI",  # Wisconsin
            "51": "WY",  # Wyoming
            "52": "PR",  # Puerto Rico
            "53": "VI",  # Virgin Islands
            "54": "GU",  # Guam
            "55": "AS",  # American Samoa
            "56": "MP",  # Northern Mariana Islands
            "99": "Unknown",  # Unknown or missing
        }

    def normalize_data(self, df: pl.DataFrame, file_type: str) -> pl.DataFrame:
        """
        Normalize data based on file type.

        Args:
            df: Input DataFrame to normalize
            file_type: Type of file (beneficiary, inpatient, outpatient, carrier, pde)

        Returns:
            Normalized DataFrame
        """
        logger.info(
            f"Starting normalization for {file_type} data with {df.height} rows and {df.width} columns."
        )

        # Filter out rows where all values are null
        original_row_count = df.height
        df = df.filter(~pl.all_horizontal(pl.all().is_null()))
        removed_rows = original_row_count - df.height
        if removed_rows > 0:
            logger.info(f"Removed {removed_rows} completely empty rows.")

        if file_type == "beneficiary":
            df = self._normalize_beneficiary_data(df)

        logger.info(
            f"Finished normalization for {file_type} data. Resulting table has {df.height} rows and {df.width} columns."
        )

        return df

    def _normalize_beneficiary_data(self, df: pl.DataFrame) -> pl.DataFrame:
        logger.info("Normalizing beneficiary data...")
        expressions = []
        new_column_expressions = []
        columns_to_drop_after_new_added = []

        if "BENE_SEX_IDENT_CD" in df.columns:
            new_column_expressions.append(
                pl.col("BENE_SEX_IDENT_CD")
                .replace(self.sex_mapping, default=None)
                .cast(pl.Categorical)
                .alias("SEX")
            )
            columns_to_drop_after_new_added.append("BENE_SEX_IDENT_CD")

        if "BENE_RACE_CD" in df.columns:
            new_column_expressions.append(
                pl.col("BENE_RACE_CD")
                .replace(self.race_mapping, default=None)
                .cast(pl.Categorical)
                .alias("RACE")
            )
            columns_to_drop_after_new_added.append("BENE_RACE_CD")

        if "SP_STATE_CODE" in df.columns:
            new_column_expressions.append(
                pl.col("SP_STATE_CODE")
                .replace(self.state_mapping, default=None)
                .cast(pl.Categorical)
                .alias("STATE_CODE")
            )
            columns_to_drop_after_new_added.append("SP_STATE_CODE")

        chronic_condition_cols = [
            "SP_ALZHDMTA",
            "SP_CHF",
            "SP_CHRNKIDN",
            "SP_CNCR",
            "SP_COPD",
            "SP_DEPRESSN",
            "SP_DIABETES",
            "SP_ISCHMCHT",
            "SP_OSTEOPRS",
            "SP_RA_OA",
            "SP_STRKETIA",
        ]
        for col_name in chronic_condition_cols:
            if col_name in df.columns:
                expressions.append(
                    pl.when(pl.col(col_name).cast(pl.Utf8) == "2")
                    .then(True)
                    .when(pl.col(col_name).cast(pl.Utf8) == "1")
                    .then(False)
                    .otherwise(pl.lit(None, dtype=pl.Boolean))
                    .alias(col_name)
                )

        if "BENE_ESRD_IND" in df.columns:
            expressions.append(
                pl.when(pl.col("BENE_ESRD_IND").cast(pl.Utf8).str.to_uppercase() == "Y")
                .then(True)
                .otherwise(False)
                .alias("BENE_ESRD_IND")
            )

        if expressions:
            df = df.with_columns(expressions)

        if new_column_expressions:
            df = df.with_columns(new_column_expressions)
            actual_columns_to_drop = [
                col for col in columns_to_drop_after_new_added if col in df.columns
            ]
            if actual_columns_to_drop:
                df = df.drop(actual_columns_to_drop)

        logger.info("Finished normalizing beneficiary data.")
        return df

    def _get_file_type(self, file_path: Path) -> str:
        """Determine the type of file based on its name."""
        file_name = file_path.name.lower()

        file_types = {
            "beneficiary": "beneficiary",
            "inpatient": "inpatient",
            "outpatient": "outpatient",
            "carrier": "carrier",
            "prescription_drug": "pde",
        }

        for key, value in file_types.items():
            if key in file_name:
                return value

        raise ValueError(f"Unknown file type for {file_path}")

    def _extract_year(self, file_path: Path) -> int:
        """Extract year from file name."""
        # For beneficiary files, extract from filename
        if "beneficiary" in file_path.name.lower():
            for part in file_path.stem.split("_"):
                if part.isdigit() and len(part) == 4:
                    return int(part)

        # For other files, extract from the data
        df = pl.read_csv(
            file_path,
            n_rows=1000,
            infer_schema_length=10000,
            ignore_errors=True,
            schema_overrides={"CLM_FROM_DT": pl.String, "SRVC_DT": pl.String},
        )
        if "CLM_FROM_DT" in df.columns:
            year = int(df["CLM_FROM_DT"].str.slice(0, 4).mode()[0])
            return year

        elif "SRVC_DT" in df.columns:
            year = int(df["SRVC_DT"].str.slice(0, 4).mode()[0])
            return year

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
                    f"{missing_count} ({(missing_count / total_rows) * 100:.2f}%)"
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
        file_type = self._get_file_type(file_path)
        year = self._extract_year(file_path)
        sample_id = self._extract_sample_id(file_path)

        logger.info(f"Processing {file_path}")

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
                df = df.with_columns(
                    [
                        pl.col(col)
                        .str.strptime(pl.Date, format=date_format, strict=False)
                        .alias(col)
                    ]
                )

        df = self.normalize_data(df, file_type)

        # Create year column
        df = df.with_columns(
            [pl.lit(year).alias("year"), pl.lit(sample_id).alias("sample_id")]
        )

        # Create partition columns for beneficiary data
        if "DESYNPUF_ID" in df.columns:
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

                group_df.write_parquet(output_path, compression="zstd", statistics=True)

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

            df.write_parquet(output_path, compression="zstd", statistics=True)
            logger.info(f"Successfully converted {file_path} to {output_path}")

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
