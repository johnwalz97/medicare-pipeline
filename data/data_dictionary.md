# Data Dictionary

## CMS Beneficiary Summary DE-SynPUF

| # | Variable names             | Labels                                                                                      |
|---|----------------------------|---------------------------------------------------------------------------------------------|
| 1 | DESYNPUF_ID                | Beneficiary Code                                                                  |
| 2 | BENE_BIRTH_DT              | Date of birth                                                                     |
| 3 | BENE_DEATH_DT              | Date of death                                                                     |
| 4 | BENE_SEX_IDENT_CD          | Sex                                                                               |
| 5 | BENE_RACE_CD               | Beneficiary Race Code                                                             |
| 6 | BENE_ESRD_IND              | End stage renal disease Indicator                                                 |
| 7 | SP_STATE_CODE              | State Code                                                                        |
| 8 | BENE_COUNTY_CD             | County Code                                                                       |
| 9 | BENE_HI_CVRAGE_TOT_MONS    | Total number of months of part A coverage for the beneficiary.                    |
| 10| BENE_SMI_CVRAGE_TOT_MONS   | Total number of months of part B coverage for the beneficiary.                    |
| 11| BENE_HMO_CVRAGE_TOT_MONS   | Total number of months of HMO coverage for the beneficiary.                       |
| 12| PLAN_CVRG_MOS_NUM          | Total number of months of part D plan coverage for the beneficiary.               |
| 13| SP_ALZHDMTA                | Chronic Condition: Alzheimer or related disorders or senile                     |
| 14| SP_CHF                     | Chronic Condition: Heart Failure                                                  |
| 15| SP_CHRNKIDN                | Chronic Condition: Chronic Kidney Disease                                         |
| 16| SP_CNCR                    | Chronic Condition: Cancer                                                         |
| 17| SP_COPD                    | Chronic Condition: Chronic Obstructive Pulmonary Disease                        |
| 18| SP_DEPRESSN                | Chronic Condition: Depression                                                     |
| 19| SP_DIABETES                | Chronic Condition: Diabetes                                                       |
| 20| SP_ISCHMCHT                | Chronic Condition: Ischemic Heart Disease                                         |
| 21| SP_OSTEOPRS                | Chronic Condition: Osteoporosis                                                   |
| 22| SP_RA_OA                   | Chronic Condition: rheumatoid arthritis and osteoarthritis (RA/OA)              |
| 23| SP_STRKETIA                | Chronic Condition: Stroke/transient Ischemic Attack                             |
| 24| MEDREIMB_IP                | Inpatient annual Medicare reimbursement amount                                    |
| 25| BENRES_IP                  | Inpatient annual beneficiary responsibility amount                                |
| 26| PPPYMT_IP                  | Inpatient annual primary payer reimbursement amount                               |
| 27| MEDREIMB_OP                | Outpatient Institutional annual Medicare reimbursement amount                       |
| 28| BENRES_OP                  | Outpatient Institutional annual beneficiary responsibility amount                 |
| 29| PPPYMT_OP                  | Outpatient Institutional annual primary payer reimbursement amount                |
| 30| MEDREIMB_CAR               | Carrier annual Medicare reimbursement amount                                      |
| 31| BENRES_CAR                 | Carrier annual beneficiary responsibility amount                                  |
| 32| PPPYMT_CAR                 | Carrier annual primary payer reimbursement amount                                 |

## CMS Inpatient Claims DE-SynPUF

| #       | Variable names                        | Labels                                                                                                         |
|---------|---------------------------------------|----------------------------------------------------------------------------------------------------------------|
| 1       | DESYNPUF_ID                           | Beneficiary Code                                                                                     |
| 2       | CLM_ID                                | Claim ID                                                                                             |
| 3       | SEGMENT                               | Claim Line Segment                                                                                   |
| 4       | CLM_FROM_DT                           | Claims start date                                                                                    |
| 5       | CLM_THRU_DT                           | Claims end date                                                                                      |
| 6       | PRVDR_NUM                             | Provider Institution                                                                                 |
| 7       | CLM_PMT_AMT                           | Claim Payment Amount                                                                                 |
| 8       | NCH_PRMRY_PYR_CLM_PD_AMT              | NCH Primary Payer Claim Paid Amount                                                                  |
| 9       | AT_PHYSN_NPI                          | Attending Physician – National Provider Identifier Number                                            |
| 10      | OP_PHYSN_NPI                          | Operating Physician – National Provider Identifier Number                                          |
| 11      | OT_PHYSN_NPI                          | Other Physician – National Provider Identifier Number                                              |
| 12      | CLM_ADMSN_DT                          | Inpatient admission date                                                                             |
| 13      | ADMTNG_ICD9_DGNS_CD                   | Claim Admitting Diagnosis Code                                                                       |
| 14      | CLM_PASS_THRU_PER_DIEM_AMT            | Claim Pass Thru Per Diem Amount                                                                      |
| 15      | NCH_BENE_IP_DDCTBL_AMT                | NCH Beneficiary Inpatient Deductible Amount                                                          |
| 16      | NCH_BENE_PTA_COINSRNC_LBLTY_AM        | NCH Beneficiary Part A Coinsurance Liability Amount                                                  |
| 17      | NCH_BENE_BLOOD_DDCTBL_LBLTY_AM        | NCH Beneficiary Blood Deductible Liability Amount                                                    |
| 18      | CLM_UTLZTN_DAY_CNT                    | Claim Utilization Day Count                                                                          |
| 19      | NCH_BENE_DSCHRG_DT                    | Inpatient discharged date                                                                            |
| 20      | CLM_DRG_CD                            | Claim Diagnosis Related Group Code                                                                   |
| 21-30   | ICD9_DGNS_CD_1 – ICD9_DGNS_CD_10      | Claim Diagnosis Code 1 – Claim Diagnosis Code 10                                                     |
| 31-36   | ICD9_PRCDR_CD_1 – ICD9_PRCDR_CD_6     | Claim Procedure Code 1 – Claim Procedure Code 6                                                      |
| 37-81   | HCPCS_CD_1 – HCPCS_CD_45              | Revenue Center HCFA Common Procedure Coding System 1 – Revenue Center HCFA Common Procedure Coding System 45 |

## CMS Outpatient Claims DE-SynPUF

| #       | Variable names                        | Labels                                                                                                         |
|---------|---------------------------------------|----------------------------------------------------------------------------------------------------------------|
| 1       | DESYNPUF_ID                           | Beneficiary Code                                                                                     |
| 2       | CLM_ID                                | Claim ID                                                                                             |
| 3       | SEGMENT                               | Claim Line Segment                                                                                   |
| 4       | CLM_FROM_DT                           | Claims start date                                                                                    |
| 5       | CLM_THRU_DT                           | Claims end date                                                                                      |
| 6       | PRVDR_NUM                             | Provider Institution                                                                                 |
| 7       | CLM_PMT_AMT                           | Claim Payment Amount                                                                                 |
| 8       | NCH_PRMRY_PYR_CLM_PD_AMT              | NCH Primary Payer Claim Paid Amount                                                                  |
| 9       | AT_PHYSN_NPI                          | Attending Physician – National Provider Identifier Number                                            |
| 10      | OP_PHYSN_NPI                          | Operating Physician – National Provider Identifier Number                                          |
| 11      | OT_PHYSN_NPI                          | Other Physician – National Provider Identifier Number                                              |
| 12      | NCH_BENE_BLOOD_DDCTBL_LBLTY_AM        | NCH Beneficiary Blood Deductible Liability Amount                                                    |
| 13-22   | ICD9_DGNS_CD_1 – ICD9_DGNS_CD_10      | Claim Diagnosis Code 1 – Claim Diagnosis Code 10                                                     |
| 23-28   | ICD9_PRCDR_CD_1 – ICD9_PRCDR_CD_6     | Claim Procedure Code 1 – Claim Procedure Code 6                                                      |
| 29      | NCH_BENE_PTB_DDCTBL_AMT               | NCH Beneficiary Part B Deductible Amount                                                             |
| 30      | NCH_BENE_PTB_COINSRNC_AMT             | NCH Beneficiary Part B Coinsurance Amount                                                            |
| 31      | ADMTNG_ICD9_DGNS_CD                   | Claim Admitting Diagnosis Code                                                                       |
| 32-76   | HCPCS_CD_1 – HCPCS_CD_45              | Revenue Center HCFA Common Procedure Coding System 1 – Revenue Center HCFA Common Procedure Coding System 45 |

## CMS Carrier Claims DE-SynPUF

| #         | Variable names                                                              | Labels                                                                                                                               |
|-----------|-----------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| 1         | DESYNPUF_ID                                                                 | Beneficiary Code                                                                                                           |
| 2         | CLM_ID                                                                      | Claim ID                                                                                                                   |
| 3         | CLM_FROM_DT                                                                 | Claims start date                                                                                                          |
| 4         | CLM_THRU_DT                                                                 | Claims end date                                                                                                            |
| 5-12      | ICD9_DGNS_CD_1 – ICD9_DGNS_CD_8                                             | Claim Diagnosis Code 1 – Claim Diagnosis Code 8                                                                              |
| 13-25     | PRF_PHYSN_NPI_1 – PRF_PHYSN_NPI_13                                          | Provider Physician – National Provider Identifier Number                                                                 |
| 26-38     | TAX_NUM_1 – TAX_NUM_13                                                      | Provider Institution Tax Number                                                                                            |
| 39-51     | HCPCS_CD_1 – HCPCS_CD_13                                                    | Line HCFA Common Procedure Coding System 1 – Line HCFA Common Procedure Coding System 13                                     |
| 52-64     | LINE_NCH_PMT_AMT_1 – LINE_NCH_PMT_AMT_13                                    | Line NCH Payment Amount 1 – Line NCH Payment Amount 13                                                                       |
| 65-77     | LINE_BENE_PTB_DDCTBL_AMT_1 – LINE_BENE_PTB_DDCTBL_AMT_13                    | Line Beneficiary Part B Deductible Amount 1 – Line Beneficiary Part B Deductible Amount 13                                 |
| 78-90     | LINE_BENE_PRMRY_PYR_PD_AMT_1 – LINE_BENE_PRMRY_PYR_PD_AMT_13                | Line Beneficiary Primary Payer Paid Amount 1 – Line Beneficiary Primary Payer Paid Amount 13                             |
| 91-103    | LINE_COINSRNC_AMT_1 – LINE_COINSRNC_AMT_13                                  | Line Coinsurance Amount 1 – Line Coinsurance Amount 13                                                                     |
| 104-116   | LINE_ALOWD_CHRG_AMT_1 – LINE_ALOWD_CHRG_AMT_13                              | Line Allowed Charge Amount 1 – Line Allowed Charge Amount 13                                                               |
| 117-129   | LINE_PRCSG_IND_CD_1 – LINE_PRCSG_IND_CD_13                                  | Line Processing Indicator Code 1 – Line Processing Indicator Code13                                                        |
| 130-142   | LINE_ICD9_DGNS_CD_1 – LINE_ICD9_DGNS_CD_13                                  | Line Diagnosis Code 1 – Line Diagnosis Code 13                                                                               |

## CMS Prescription Drug Events (PDE) DE-SynPUF

| # | Variable names   | Labels                             |
|---|------------------|------------------------------------|
| 1 | DESYNPUF_ID      | Beneficiary Code           |
| 2 | PDE_ID           | CCW Part D Event Number  |
| 3 | SRVC_DT          | RX Service Date          |
| 4 | PROD_SRVC_ID     | Product Service ID       |
| 5 | QTY_DSPNSD_NUM   | Quantity Dispensed       |
| 6 | DAYS_SUPLY_NUM   | Days Supply              |
| 7 | PTNT_PAY_AMT     | Patient Pay Amount       |
| 8 | TOT_RX_CST_AMT   | Gross Drug Cost          |
