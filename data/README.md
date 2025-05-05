# Medicare Data Pipeline Documentation

This document details the Medicare data pipeline, covering its structure, transformations, and data characteristics across the Bronze, Silver, and Gold layers.

## Pipeline Overview

The pipeline processes Medicare claims data through three standard layers: Bronze, Silver, and Gold. There is also a `raw` layer that just stores the raw data as it is downloaded from the CMS server.

1. **Bronze Layer**: Stores the raw ingested data in Parquet format with minimal alterations. Data is partitioned by `year` and `bene_id_prefix`.
2. **Silver Layer**: Contains cleaned and dimensionally modeled data derived from the Bronze layer. This layer provides a structured and reliable source for analytics, featuring dimension tables (`dim_beneficiary`, `dim_provider`) and fact tables (`fact_claims`, `fact_claim_diagnoses`, `fact_prescription`). Data is partitioned for query optimization.
3. **Gold Layer**: Consists of aggregated analytical views built upon the Silver layer, designed for specific business intelligence and reporting requirements.

---

## Bronze Layer

The Bronze layer holds the the data that is converted from the raw CSVs that are downloaded from CMS.

**Total Tables**: 5
**Total Rows**: 134,929

### Tables

#### 1. `inpatient`

* **Description**: Raw inpatient claims data.
* **Partitioning**: `year`, `bene_id_prefix`
* **Schema**: (84 columns)
  * `DESYNPUF_ID`: String (Beneficiary ID)
  * `CLM_ID`: String (Claim ID)
  * `SEGMENT`: Int32
  * `CLM_FROM_DT`: Date
  * `CLM_THRU_DT`: Date
  * `PRVDR_NUM`: String (Provider Number)
  * `CLM_PMT_AMT`: Decimal(10, 2) (Claim Payment Amount)
  * `NCH_PRMRY_PYR_CLM_PD_AMT`: Decimal(10, 2) (Primary Payer Paid Amount)
  * `AT_PHYSN_NPI`: String (Attending Physician NPI)
  * `OP_PHYSN_NPI`: String (Operating Physician NPI)
  * `OT_PHYSN_NPI`: String (Other Physician NPI)
  * `CLM_ADMSN_DT`: Date (Admission Date)
  * `ADMTNG_ICD9_DGNS_CD`: String (Admitting Diagnosis Code)
  * `CLM_PASS_THRU_PER_DIEM_AMT`: Float64
  * `NCH_BENE_IP_DDCTBL_AMT`: Float64 (Inpatient Deductible Amount)
  * `NCH_BENE_PTA_COINSRNC_LBLTY_AM`: Float64 (Coinsurance Liability)
  * `NCH_BENE_BLOOD_DDCTBL_LBLTY_AM`: Float64 (Blood Deductible Liability)
  * `CLM_UTLZTN_DAY_CNT`: Int64 (Utilization Day Count)
  * `NCH_BENE_DSCHRG_DT`: Int64 (Discharge Date)
  * `CLM_DRG_CD`: String (Diagnosis Related Group Code)
  * `ICD9_DGNS_CD_1` to `ICD9_DGNS_CD_10`: String (Diagnosis Codes)
  * `ICD9_PRCDR_CD_1` to `ICD9_PRCDR_CD_6`: String (Procedure Codes)
  * `HCPCS_CD_1` to `HCPCS_CD_45`: String (HCPCS Codes)
  * `year`: Int32
  * `sample_id`: String
  * `bene_id_prefix`: String
* **Sample Row**:

    ```json
    {
      "DESYNPUF_ID": "430456A8342E3D40", "CLM_ID": "196621177017319", "SEGMENT": 1, "CLM_FROM_DT": "2010-07-20", "CLM_THRU_DT": "2010-07-27", "PRVDR_NUM": "3100ZR", "CLM_PMT_AMT": 38000.0, "NCH_PRMRY_PYR_CLM_PD_AMT": 0.0, "AT_PHYSN_NPI": "3649405387", "OP_PHYSN_NPI": null, "OT_PHYSN_NPI": null, "CLM_ADMSN_DT": "2010-07-20", "ADMTNG_ICD9_DGNS_CD": "486", "CLM_PASS_THRU_PER_DIEM_AMT": 0.0, "NCH_BENE_IP_DDCTBL_AMT": 1100.0, "NCH_BENE_PTA_COINSRNC_LBLTY_AM": 0.0, "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM": 0.0, "CLM_UTLZTN_DAY_CNT": 7, "NCH_BENE_DSCHRG_DT": 20100727, "CLM_DRG_CD": "192", "ICD9_DGNS_CD_1": "4821", ..., "year": 2008, "sample_id": "Sample", "bene_id_prefix": "43"
    }
    ```

#### 2. `outpatient`

* **Description**: Raw outpatient claims data.
* **Partitioning**: `year`, `bene_id_prefix`
* **Schema**: (79 columns)
  * `DESYNPUF_ID`: String
  * `CLM_ID`: String
  * `SEGMENT`: Int32
  * `CLM_FROM_DT`: Date
  * `CLM_THRU_DT`: Date
  * `PRVDR_NUM`: String
  * `CLM_PMT_AMT`: Decimal(10, 2)
  * `NCH_PRMRY_PYR_CLM_PD_AMT`: Decimal(10, 2)
  * `AT_PHYSN_NPI`: String
  * `OP_PHYSN_NPI`: String
  * `OT_PHYSN_NPI`: String
  * `NCH_BENE_BLOOD_DDCTBL_LBLTY_AM`: Float64
  * `ICD9_DGNS_CD_1` to `ICD9_DGNS_CD_10`: String
  * `ICD9_PRCDR_CD_1` to `ICD9_PRCDR_CD_6`: String
  * `NCH_BENE_PTB_DDCTBL_AMT`: Float64 (Part B Deductible Amount)
  * `NCH_BENE_PTB_COINSRNC_AMT`: Float64 (Part B Coinsurance Amount)
  * `ADMTNG_ICD9_DGNS_CD`: String
  * `HCPCS_CD_1` to `HCPCS_CD_45`: String
  * `year`: Int32
  * `sample_id`: String
  * `bene_id_prefix`: String
* **Sample Row**:

    ```json
    {
      "DESYNPUF_ID": "FE012326DB854308", "CLM_ID": "542342281548636", "SEGMENT": 1, "CLM_FROM_DT": "2008-10-11", "CLM_THRU_DT": "2008-10-11", "PRVDR_NUM": "1101YQ", "CLM_PMT_AMT": 60.0, "NCH_PRMRY_PYR_CLM_PD_AMT": 0.0, "AT_PHYSN_NPI": "7024131648", "OP_PHYSN_NPI": null, "OT_PHYSN_NPI": null, "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM": 0.0, "ICD9_DGNS_CD_1": "45384", ..., "year": 2008, "sample_id": "Sample", "bene_id_prefix": "FE"
    }
    ```

#### 3. `beneficiary`

* **Description**: Raw beneficiary summary data, containing demographics and annual payment summaries.
* **Partitioning**: `year`, `bene_id_prefix`
* **Schema**: (35 columns)
  * `DESYNPUF_ID`: String
  * `BENE_BIRTH_DT`: Date
  * `BENE_DEATH_DT`: Date
  * `BENE_SEX_IDENT_CD`: Categorical (Gender)
  * `BENE_RACE_CD`: Categorical (Race)
  * `BENE_ESRD_IND`: Categorical (End-Stage Renal Disease Indicator)
  * `SP_STATE_CODE`: Categorical (State Code)
  * `BENE_COUNTY_CD`: Categorical (County Code)
  * `BENE_HI_CVRAGE_TOT_MONS`: Int32 (Months of HI Coverage)
  * `BENE_SMI_CVRAGE_TOT_MONS`: Int32 (Months of SMI Coverage)
  * `BENE_HMO_CVRAGE_TOT_MONS`: Int32 (Months of HMO Coverage)
  * `PLAN_CVRG_MOS_NUM`: Int32 (Months of Part D Coverage)
  * `SP_ALZHDMTA` to `SP_STRKETIA`: Int32 (Chronic Condition Flags)
  * `MEDREIMB_IP`, `BENRES_IP`, `PPPYMT_IP`: Decimal(10, 2) (Inpatient Payments: Medicare, Beneficiary, Primary Payer)
  * `MEDREIMB_OP`, `BENRES_OP`, `PPPYMT_OP`: Decimal(10, 2) (Outpatient Payments)
  * `MEDREIMB_CAR`, `BENRES_CAR`, `PPPYMT_CAR`: Decimal(10, 2) (Carrier Payments)
  * `year`: Int32
  * `sample_id`: String
  * `bene_id_prefix`: String
* **Sample Row**:

    ```json
    {
      "DESYNPUF_ID": "A901E4CBC9E3EA0B", "BENE_BIRTH_DT": "1944-12-01", "BENE_DEATH_DT": null, "BENE_SEX_IDENT_CD": "1", "BENE_RACE_CD": "2", "BENE_ESRD_IND": "Y", "SP_STATE_CODE": "33", "BENE_COUNTY_CD": "050", "BENE_HI_CVRAGE_TOT_MONS": 12, "BENE_SMI_CVRAGE_TOT_MONS": 12, "BENE_HMO_CVRAGE_TOT_MONS": 0, "PLAN_CVRG_MOS_NUM": 12, "SP_ALZHDMTA": 2, ..., "year": 2010, "sample_id": "Sample", "bene_id_prefix": "A9"
    }
    ```

#### 4. `carrier`

* **Description**: Raw carrier (physician/supplier Part B) claims data, including line-level details.
* **Partitioning**: `year`, `bene_id_prefix`
* **Schema**: (145 columns)
  * `DESYNPUF_ID`: String
  * `CLM_ID`: String
  * `CLM_FROM_DT`: Date
  * `CLM_THRU_DT`: Date
  * `ICD9_DGNS_CD_1` to `ICD9_DGNS_CD_8`: String (Claim Level Diagnosis Codes)
  * `PRF_PHYSN_NPI_1` to `PRF_PHYSN_NPI_13`: Int64 (Performing Physician NPIs per line)
  * `TAX_NUM_1` to `TAX_NUM_13`: Int64 (Tax Numbers per line)
  * `HCPCS_CD_1` to `HCPCS_CD_13`: String (HCPCS Codes per line)
  * `LINE_NCH_PMT_AMT_1` to `LINE_NCH_PMT_AMT_13`: Float64 (Medicare Payment per line)
  * `LINE_BENE_PTB_DDCTBL_AMT_1` to `LINE_BENE_PTB_DDCTBL_AMT_13`: Float64 (Deductible per line)
  * `LINE_BENE_PRMRY_PYR_PD_AMT_1` to `LINE_BENE_PRMRY_PYR_PD_AMT_13`: Float64 (Primary Payer Payment per line)
  * `LINE_COINSRNC_AMT_1` to `LINE_COINSRNC_AMT_13`: Float64 (Coinsurance per line)
  * `LINE_ALOWD_CHRG_AMT_1` to `LINE_ALOWD_CHRG_AMT_13`: Float64 (Allowed Charge per line)
  * `LINE_PRCSG_IND_CD_1` to `LINE_PRCSG_IND_CD_13`: String (Processing Indicator per line)
  * `LINE_ICD9_DGNS_CD_1` to `LINE_ICD9_DGNS_CD_13`: String (Diagnosis Code per line)
  * `year`: Int32
  * `sample_id`: String
  * `bene_id_prefix`: String
* **Sample Row**:

    ```json
    {
      "DESYNPUF_ID": "7E00D9D78DAA008D", "CLM_ID": "887063387103756", "CLM_FROM_DT": "2008-01-04", "CLM_THRU_DT": "2008-01-04", "ICD9_DGNS_CD_1": "4830", ..., "LINE_NCH_PMT_AMT_1": 130.0, ..., "year": 2008, "sample_id": "Sample", "bene_id_prefix": "7E"
    }
    ```

#### 5. `pde`

* **Description**: Raw Prescription Drug Event (PDE) data (Part D).
* **Partitioning**: `year`, `bene_id_prefix`
* **Schema**: (11 columns)
  * `DESYNPUF_ID`: String
  * `PDE_ID`: Int64 (Prescription Drug Event ID)
  * `SRVC_DT`: Date (Service Date)
  * `PROD_SRVC_ID`: String (Product Service ID / NDC Code)
  * `QTY_DSPNSD_NUM`: Decimal(10, 2) (Quantity Dispensed)
  * `DAYS_SUPLY_NUM`: Int32 (Days Supply)
  * `PTNT_PAY_AMT`: Decimal(10, 2) (Patient Pay Amount)
  * `TOT_RX_CST_AMT`: Decimal(10, 2) (Total RX Cost)
  * `year`: Int32
  * `sample_id`: String
  * `bene_id_prefix`: String
* **Sample Row**:

    ```json
    {
      "DESYNPUF_ID": "F300281FCFFD1B3E", "PDE_ID": 233604491701758, "SRVC_DT": "2009-11-20", "PROD_SRVC_ID": "16590019790", "QTY_DSPNSD_NUM": 150.0, "DAYS_SUPLY_NUM": 20, "PTNT_PAY_AMT": 0.0, "TOT_RX_CST_AMT": 20.0, "year": 2009, "sample_id": "Sample", "bene_id_prefix": "F3"
    }
    ```

---

## Silver Layer

The Silver layer provides a cleaned, integrated, and dimensionally modeled view of the Medicare data.

**Total Tables**: 5
**Total Rows**: 592,952

### Tables

#### 1. `dim_beneficiary`

* **Description**: Dimension table storing beneficiary demographics and calculated annual spending totals. It serves as the central dimension for member-related analysis.
* **Transformation Source**: Bronze `beneficiary`.
* **Transformation Logic**: Renames source columns for clarity (e.g., `DESYNPUF_ID` to `bene_id`). Calculates aggregate payment fields (`total_medicare_payment`, `total_beneficiary_payment`, `total_third_party_payment`, `total_allowed`, `total_paid`) by summing the corresponding IP, OP, and Carrier payment columns from the source.
* **Partitioning**: Not partitioned (written as a single file).
* **Schema**: (40 columns)
  * `bene_id`: String
  * `birth_date`: Date
  * `death_date`: Date
  * `gender`: Categorical
  * `race`: Categorical
  * `BENE_ESRD_IND`: Categorical
  * `state`: Categorical
  * `BENE_COUNTY_CD`: Categorical
  * `BENE_HI_CVRAGE_TOT_MONS`: Int32
  * `BENE_SMI_CVRAGE_TOT_MONS`: Int32
  * `BENE_HMO_CVRAGE_TOT_MONS`: Int32
  * `PLAN_CVRG_MOS_NUM`: Int32
  * `SP_ALZHDMTA` to `SP_STRKETIA`: Int32
  * `ip_medicare_payment`: Decimal(10, 2)
  * `ip_beneficiary_payment`: Decimal(10, 2)
  * `ip_third_party_payment`: Decimal(10, 2)
  * `op_medicare_payment`: Decimal(10, 2)
  * `op_beneficiary_payment`: Decimal(10, 2)
  * `op_third_party_payment`: Decimal(10, 2)
  * `car_medicare_payment`: Decimal(10, 2)
  * `car_beneficiary_payment`: Decimal(10, 2)
  * `car_third_party_payment`: Decimal(10, 2)
  * `year`: Int32
  * `sample_id`: String
  * `bene_id_prefix`: String
  * `total_medicare_payment`: Decimal(38, 2) (Calculated)
  * `total_beneficiary_payment`: Decimal(38, 2) (Calculated)
  * `total_third_party_payment`: Decimal(38, 2) (Calculated)
  * `total_allowed`: Decimal(38, 2) (Calculated: Sum of all payments)
  * `total_paid`: Decimal(38, 2) (Calculated: Medicare + Beneficiary Payment)
* **Sample Row**:

    ```json
    {
      "bene_id": "F900242EC7459BD5", "birth_date": "1938-06-01", "death_date": null, "gender": "2", "race": "1", "BENE_ESRD_IND": "0", "state": "33", ..., "total_allowed": 1880.0, "total_paid": 1880.0
    }
    ```

#### 2. `dim_provider`

* **Description**: Dimension table containing unique provider identifiers.
* **Transformation Source**: Bronze `inpatient`, `outpatient`, `carrier`, `pde`.
* **Transformation Logic**: Extracts provider IDs from various columns across source tables (`PRVDR_NUM`, NPI columns, `PRVDR_ID`, `PRSCRBR_ID`). Deduplicates based on `provider_id`. State information is associated where available; otherwise, it's 'Unknown'. Provider type is currently 'Unknown'.
* **Partitioning**: Not partitioned (written as a single file).
* **Schema**: (3 columns)
  * `provider_id`: String
  * `state`: String
  * `provider_type`: String (Currently "Unknown")
* **Sample Row**:

    ```json
    { "provider_id": "2765173173", "state": "Unknown", "provider_type": "Unknown" }
    ```

#### 3. `fact_claims`

* **Description**: Unified fact table containing key information for all claim types.
* **Transformation Source**: Bronze `inpatient`, `outpatient`, `carrier`.
* **Transformation Logic**: Selects and renames columns from source tables to create a consistent structure. Assigns `claim_type` ('inpatient', 'outpatient', 'carrier'). Calculates `total_payment` by summing `medicare_payment`, `third_party_payment`, and `patient_payment`. Carrier claim payments (`medicare_payment`, `third_party_payment`) are summed from line-level columns if the claim-level amount (`CLM_PMT_AMT`) is unavailable.
* **Partitioning**: `year`, `bene_id_prefix`
* **Schema**: (12 columns)
  * `bene_id`: String
  * `claim_id`: String
  * `claim_type`: String
  * `claim_from_date`: Date
  * `claim_thru_date`: Date
  * `provider_id`: String
  * `medicare_payment`: Decimal(10, 2)
  * `third_party_payment`: Decimal(10, 2)
  * `patient_payment`: Decimal(10, 2)
  * `year`: Int32
  * `bene_id_prefix`: String
  * `total_payment`: Decimal(38, 2) (Calculated)
* **Sample Row**:

    ```json
    {
      "bene_id": "F101EACC41590D23", "claim_id": "196461177019712", "claim_type": "inpatient", "claim_from_date": "2010-03-21", "claim_thru_date": "2010-03-22", "provider_id": "18T0RJ", "medicare_payment": 4000.0, "third_party_payment": 0.0, "patient_payment": 0.0, "year": 2008, "bene_id_prefix": "F1", "total_payment": 4000.0
    }
    ```

#### 4. `fact_claim_diagnoses`

* **Description**: Normalized fact table detailing diagnoses associated with claims.
* **Transformation Source**: Bronze `inpatient`, `outpatient`, `carrier`.
* **Transformation Logic**: Unpivots the multiple `ICD9_DGNS_CD_*` columns from source tables into a long format with one row per diagnosis. Creates `diagnosis_code` and `diagnosis_position`. Associates the claim-level payment (`CLM_PMT_AMT`) with each diagnosis row. Adds a simplified `diagnosis_description` based on a mapping of ICD-9 prefixes.
* **Partitioning**: `year`, `bene_id_prefix`
* **Schema**: (9 columns)
  * `bene_id`: String
  * `claim_id`: String
  * `diagnosis_code`: String
  * `diagnosis_position`: Int32
  * `payment`: Decimal(10, 2) (Claim-level payment)
  * `claim_type`: String
  * `year`: Int32
  * `bene_id_prefix`: String
  * `diagnosis_description`: String (Mapped)
* **Sample Row**:

    ```json
    {
      "bene_id": "3A005DE6FF742D89", "claim_id": "196851177007719", "diagnosis_code": "1970", "diagnosis_position": 1, "payment": 22000.0, "claim_type": "inpatient", "year": 2008, "bene_id_prefix": "3A", "diagnosis_description": "Other diagnosis"
    }
    ```

#### 5. `fact_prescription`

* **Description**: Fact table detailing prescription drug events.
* **Transformation Source**: Bronze `pde`.
* **Transformation Logic**: Selects and renames columns from the source PDE table. Identifies `product_id` (NDC) and `provider_id` (prescriber/pharmacy). Calculates `medicare_payment` as `total_cost` minus `patient_payment`.
* **Partitioning**: `year`, `bene_id_prefix`
* **Schema**: (12 columns)
  * `bene_id`: String
  * `prescription_id`: Int64
  * `service_date`: Date
  * `quantity_dispensed`: Decimal(10, 2)
  * `days_supply`: Int32
  * `patient_payment`: Decimal(10, 2)
  * `total_cost`: Decimal(10, 2)
  * `product_id`: String
  * `provider_id`: String
  * `year`: Int32
  * `bene_id_prefix`: String
  * `medicare_payment`: Decimal(38, 2) (Calculated)
* **Sample Row**:

    ```json
    {
      "bene_id": "0C00409846257AEB", "prescription_id": 233024493846551, "service_date": "2008-01-04", "quantity_dispensed": 20.0, "days_supply": 10, "patient_payment": 0.0, "total_cost": 30.0, "product_id": "46036000156", "provider_id": "Unknown", "year": 2009, "bene_id_prefix": "0C", "medicare_payment": 30.0
    }
    ```

---

## Gold Layer

The Gold layer provides pre-aggregated, business-ready analytical views.

**Total Tables**: 3
**Total Rows**: 2,693,072

### Tables

#### 1. `member_year_metrics`

* **Description**: Provides annual aggregated metrics for each beneficiary, including costs and utilization counts.
* **Transformation Source**: Silver `dim_beneficiary`, `fact_claims`, `fact_prescription`.
* **Transformation Logic**: Starts with `dim_beneficiary` for demographics and total payments. Joins aggregated counts from `fact_claims` (distinct `claim_id` per `claim_type` yields `inpatient_stays`, `outpatient_visits`, `carrier_claims`; distinct `provider_id` yields `unique_providers`) and `fact_prescription` (distinct `prescription_id` yields `rx_fills`).
* **Partitioning**: `year`
* **Schema**: (12 columns)
  * `bene_id`: String
  * `year`: Int32
  * `total_allowed`: Decimal(38, 2)
  * `total_paid`: Decimal(38, 2)
  * `gender`: Categorical
  * `race`: Categorical
  * `state`: Categorical
  * `inpatient_stays`: UInt32 (Aggregated)
  * `outpatient_visits`: UInt32 (Aggregated)
  * `carrier_claims`: UInt32 (Aggregated)
  * `unique_providers`: UInt32 (Aggregated)
  * `rx_fills`: UInt32 (Aggregated)
* **Sample Row**:

    ```json
    {
      "bene_id": "F900242EC7459BD5", "year": 2009, "total_allowed": 2650.0, "total_paid": 2580.0, "gender": "2", "race": "1", "state": "33", "inpatient_stays": 0, "outpatient_visits": 0, "carrier_claims": 61, "unique_providers": 1, "rx_fills": 0
    }
    ```

#### 2. `top_diagnoses_by_member`

* **Description**: Ranks diagnoses for each beneficiary annually based on associated claim payments.
* **Transformation Source**: Silver `fact_claim_diagnoses`.
* **Transformation Logic**: Aggregates `fact_claim_diagnoses` by `bene_id`, `year`, `diagnosis_code`, and `diagnosis_description`, summing the claim-level `payment`. Ranks the results within each `bene_id` and `year` partition based on the summed `diagnosis_payment` in descending order.
* **Partitioning**: `year`
* **Schema**: (6 columns)
  * `bene_id`: String
  * `year`: Int32
  * `diagnosis_code`: String
  * `diagnosis_description`: String
  * `diagnosis_payment`: Decimal(10, 2) (Aggregated payment for this diagnosis)
  * `diagnosis_rank`: UInt32 (Rank based on payment)
* **Sample Row**:

    ```json
    {
      "bene_id": "00013D2EFD8E45D1", "year": 2008, "diagnosis_code": "73300", "diagnosis_description": "Other diagnosis", "diagnosis_payment": 4000.0, "diagnosis_rank": 1
    }
    ```

#### 3. `patient_api_view`

* **Description**: A denormalized view optimized for patient-centric API access, combining annual metrics and top diagnoses.
* **Transformation Source**: Gold `member_year_metrics`, `top_diagnoses_by_member`.
* **Transformation Logic**: Combines data from `member_year_metrics` and `top_diagnoses_by_member`, potentially filtering or selecting specific columns needed for the API endpoint. The structure suggests separate components for metrics and diagnoses are persisted.
* **Partitioning**: `year` (observed for constituent files `patient_metrics.parquet` and `patient_diagnoses.parquet`)
* **Schema**: (Composed of metrics and diagnoses components)
  * **Metrics Component**:
    * `bene_id`: String
    * `year`: Int32
    * `total_allowed`: Decimal(38, 2)
    * `total_paid`: Decimal(38, 2)
    * `inpatient_stays`: UInt32
    * `outpatient_visits`: UInt32
    * `rx_fills`: UInt32
    * `unique_providers`: UInt32
  * **Diagnoses Component**:
    * `bene_id`: String
    * `year`: Int32
    * `diagnosis_code`: String
    * `diagnosis_description`: String
    * `diagnosis_payment`: Decimal(10, 2)
    * `diagnosis_rank`: UInt32
* **Sample Row (Metrics Component)**:

    ```json
    {
      "bene_id": "F900242EC7459BD5", "year": 2008, "total_allowed": 2970.0, "total_paid": 2970.0, "inpatient_stays": 0, "outpatient_visits": 1, "rx_fills": 0, "unique_providers": 1
    }
    ```

* **Sample Row (Diagnoses Component)**:

    ```json
    {
      "bene_id": "00013D2EFD8E45D1", "year": 2008, "diagnosis_code": "73300", "diagnosis_description": "Other diagnosis", "diagnosis_payment": 4000.0, "diagnosis_rank": 1
    }
    ```
