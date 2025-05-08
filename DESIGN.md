# Design Decisions

## Data Understanding

The CMS DE-SynPUF dataset provides synthetic Medicare claims across multiple files:

1. **Beneficiary Summary Files (2008-2010)**
   - Contains demographic information and annual spending summaries
   - One row per beneficiary per year (~60,000 beneficiaries)

2. **Claims Files (2008-2010)**
   - **Inpatient**: Hospital admissions (~16MB, relatively rare events)
   - **Outpatient**: Outpatient services (~150MB, moderate frequency)
   - **Carrier**: Professional services (~2.4GB, highest volume)
   - **Prescription Drug Events**: Medication dispensing (~400MB, recurring events)

Each file contains multiple diagnosis codes, procedure codes, and provider identifiers stored in a "wide" format with columns like ICD9_DGNS_CD_1 through ICD9_DGNS_CD_10.

## Raw Data Schema (Key Fields Only)

### Beneficiary

```plain
DESYNPUF_ID                            - Beneficiary ID (PK)
BENE_BIRTH_DT                          - Birth date
BENE_SEX_IDENT_CD                      - Sex (1=Male, 2=Female)
BENE_RACE_CD                           - Race code
SP_STATE_CODE                          - State code
MEDREIMB_IP                            - IP Medicare reimbursement amount
BENRES_IP                              - IP beneficiary responsibility
PPPYMT_IP                              - IP third-party payments
MEDREIMB_OP, BENRES_OP, PPPYMT_OP      - Outpatient amounts
MEDREIMB_CAR, BENRES_CAR, PPPYMT_CAR   - Carrier amounts
```

### Claims (Inpatient/Outpatient/Carrier)

```plain
DESYNPUF_ID           - Beneficiary ID (FK)
CLM_ID                - Claim ID (PK)
CLM_FROM_DT           - Claim start date
CLM_THRU_DT           - Claim end date
PRVDR_NUM             - Provider identifier
CLM_PMT_AMT           - Medicare payment amount
ICD9_DGNS_CD_1...10   - Diagnosis codes
```

### Prescription Events

```plain
DESYNPUF_ID           - Beneficiary ID (FK)
PDE_ID                - Prescription event ID (PK)
SRVC_DT               - Service date
PROD_SRVC_ID          - NDC product code
TOT_RX_CST_AMT        - Total prescription cost
```

## Storage Strategy

### File Format: Parquet

**Rationale**: Selected Parquet for its analytical query advantages:

- Column-oriented storage enables efficient querying on specific fields
- Compression reduces storage requirements by 10-15x compared to CSV
- Schema enforcement ensures data consistency
- Predicate pushdown dramatically improves filter performance

### Partitioning Strategy: year + bene_id_prefix

**Chosen Approach**:

- **Primary partition**: `year` (2008, 2009, 2010)
- **Secondary partition**: `bene_id_prefix` (first 2 chars of beneficiary ID)

**Rationale**:

1. **Year partitioning**: Almost all queries are year-specific, aligning with the member/year level requirement. File pruning is highly effective with only 3 partitions.
2. **Bene_id_prefix**: Creates balanced distributions (~256 partitions per year) while enabling efficient filtering for member-specific queries.

## Data Modeling

### Modeling Approach: Simplified Star Schema

For this specific analytics challenge, a star schema provides the optimal balance between query performance and maintenance complexity.

### Core Tables

1. **dim_beneficiary**
   - One row per beneficiary per year
   - Contains demographics and annual spending summaries
   - Serves as the central dimension for all queries

2. **fact_claims**
   - Consolidated claims data across claim types
   - Includes claim_type discriminator (IP/OP/Carrier)
   - Contains payment amounts and claim dates
   - Enables unified utilization counts and spend aggregation

3. **fact_claim_diagnoses**
   - "Long format" version of diagnosis codes
   - One row per diagnosis per claim
   - Enables efficient aggregation by diagnosis

4. **fact_prescription**
   - Prescription drug events
   - Enables RX fill counting and cost analysis

5. **dim_provider**
   - Provider information extracted from claims
   - Enables provider breadth analysis

### Transformation Logic

Two key transformations enhance query performance:

1. **Diagnosis Normalization**: Transform diagnosis codes from "wide" format (columns 1-10) to "long" format:

   ```plain
   # Input format (simplified)
   | claim_id | icd9_dgns_cd_1 | icd9_dgns_cd_2 | payment |
   |----------|----------------|----------------|---------|
   | 123      | 4019           | 2724           | 500.00  |

   # Output format in fact_claim_diagnoses
   | claim_id | diagnosis_code | position | payment |
   |----------|---------------|----------|---------|
   | 123      | 4019          | 1        | 500.00  |
   | 123      | 2724          | 2        | 500.00  |
   ```

   This structure enables the "top 5 diagnoses" requirement without complex pivoting.

2. **Provider Consolidation**: Extract and deduplicate provider IDs across claim types:
   - Inpatient/Outpatient: PRVDR_NUM, AT_PHYSN_NPI, OP_PHYSN_NPI
   - Carrier: PRF_PHYSN_NPI_1...13
   - Prescription: PRVDR_ID

3. **Payment Calculation for Carrier Claims**: For carrier claims, payment calculation requires special handling:
   - Some carrier claims don't have a claim-level payment amount (CLM_PMT_AMT)
   - When CLM_PMT_AMT is missing, the implementation sums line-level payment amounts (LINE_NCH_PMT_AMT_1...13)
   - Similar logic applies for third-party payments (LINE_BENE_PRMRY_PYR_PD_AMT_1...13)
   - This ensures accurate payment calculation for all claim types

### Performance Optimization: Materialized Views

Two key materialized views accelerate API queries:

1. **member_year_metrics**

   ```sql
   -- Note: This is pseudocode representing the Polars dataframe operations in the implementation
   SELECT 
     b.DESYNPUF_ID as bene_id,
     b.year,
     SUM(c.CLM_PMT_AMT + NVL(c.PTNT_PAY_AMT, 0)) as total_paid,
     -- Additional metrics
   FROM dim_beneficiary b
   LEFT JOIN fact_claims c ON b.DESYNPUF_ID = c.DESYNPUF_ID AND b.year = YEAR(c.CLM_FROM_DT)
   GROUP BY b.DESYNPUF_ID, b.year
   ```

2. **top_diagnoses_by_member**

   ```sql
   -- Note: This is pseudocode representing the Polars dataframe operations in the implementation
   SELECT 
     bene_id,
     year,
     diagnosis_code,
     diagnosis_description,
     paid_amount,
     diagnosis_rank
   FROM (
     SELECT 
       b.DESYNPUF_ID as bene_id,
       b.year,
       d.diagnosis_code,
       d.diagnosis_description,
       SUM(d.payment) as paid_amount,
       ROW_NUMBER() OVER (PARTITION BY b.DESYNPUF_ID, b.year ORDER BY SUM(d.payment) DESC) as diagnosis_rank
     FROM dim_beneficiary b
     JOIN fact_claims c ON b.DESYNPUF_ID = c.DESYNPUF_ID AND b.year = YEAR(c.CLM_FROM_DT)
     JOIN fact_claim_diagnoses d ON c.CLM_ID = d.CLM_ID
     GROUP BY b.DESYNPUF_ID, b.year, d.diagnosis_code, d.diagnosis_description
   ) ranked
   WHERE diagnosis_rank <= 5
   ```

3. **patient_api_view**

   A combined view optimized for the patient API endpoint that joins member metrics with top diagnoses for efficient API responses. This view separates the data into two components:

   - **patient_metrics**: Contains key metrics by patient and year
   - **patient_diagnoses**: Contains top diagnoses by patient and year

   This structure allows for efficient retrieval of patient data without unnecessary joins at query time.

## API Implementation

The FastAPI endpoint (GET /patient/{bene_id}?year=YYYY) leverages the data model for efficient retrieval:

1. **Data Access Pattern**
   - Query pre-calculated patient_api_view (which contains patient_metrics and patient_diagnoses)
   - Retrieves data directly from partitioned files based on bene_id_prefix
   - Format as JSON response

2. **Performance Considerations**
   - Redis caching for frequent requests
   - Partitioned data access based on bene_id_prefix
   - Request-level security and validation
   - Separation of metrics and diagnoses data minimizes data reads for specific queries

## Complete Data Flow Diagram

```plain
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌─────────────┐
│             │    │              │    │             │    │             │
│  Raw CSVs   │───►│ Bronze Layer │───►│ Silver Layer│───►│  Gold Layer │
│  (data/raw) │    │ (Parquet)    │    │ (Star       │    │ (Analytics  │
│             │    │              │    │  Schema)    │    │  Views)     │
└─────────────┘    └──────────────┘    └─────────────┘    └──────┬──────┘
                                                                 │
                                                                 ▼
                                                         ┌─────────────────┐
                                                         │                 │
                                                         │  FastAPI Server │
                                                         │                 │
                                                         └────────┬────────┘
                                                                  │
                                                                  ▼
                                                         ┌─────────────────┐
                                                         │                 │
                                                         │   JSON Response │
                                                         │                 │
                                                         └─────────────────┘
```

## Strategies for Scaling

### Incremental Processing: Streaming-Based Incremental Updates

For efficiently handling new claims data:

1. **Track Changed Records**
   - Maintain a changelog tracking the latest records modified/added
   - Include record key, timestamp, and modification type
   - Enable precise identification of only changed data

2. **Streaming Update Flow**
   - **Source to Bronze**: Track file-level changes and process only new/modified files
   - **Bronze to Silver**: Stream only changed records using the year/bene_id_prefix partitioning
   - **Silver to Gold**: Stream individual record changes from silver to gold without requiring full partition rebuilds

3. **Delta Propagation**
   - Extract modified records from affected silver partitions
   - Apply targeted row-level updates to gold tables using merge operations
   - Maintain summary tables with incremental aggregation techniques

#### Implementation Approach

The streaming pipeline processes updates efficiently:

1. Detect changed records in silver layer
2. Stream changes through CDC system
3. Update gold tables with targeted operations:
   - Update metrics for affected beneficiaries
   - Recalculate rankings only when needed
   - Refresh only changed patient records
4. Use merge operations instead of partition rebuilds

Benefits:

- Eliminates full partition rebuilds
- Resolves silver/gold partitioning mismatch
- Enables near real-time analytics updates
- Scales efficiently with incremental data

#### Performance Considerations

This approach is optimal when:

- Changes affect few beneficiaries
- Analytics need quick refreshes
- Partition rebuilds would be costly

For initial loads, batch processing remains more efficient.

## Scaling Up: Key Optimization for 10x Scale

The single most impactful optimization for 10x scale would be **distributed processing with Ray or Spark**. Ray offers the simplest integration path with the current Polars implementation.

Converting our sequential partition processing to distributed execution would dramatically improve performance:
