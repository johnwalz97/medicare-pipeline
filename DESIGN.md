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

```
DESYNPUF_ID           - Beneficiary ID (PK)
BENE_BIRTH_DT         - Birth date
BENE_SEX_IDENT_CD     - Sex (1=Male, 2=Female)
BENE_RACE_CD          - Race code
SP_STATE_CODE         - State code
MEDREIMB_IP           - IP Medicare reimbursement amount
BENRES_IP             - IP beneficiary responsibility
PPPYMT_IP             - IP third-party payments
MEDREIMB_OP, BENRES_OP, PPPYMT_OP - Outpatient amounts
MEDREIMB_CAR, BENRES_CAR, PPPYMT_CAR - Carrier amounts
```

### Claims (Inpatient/Outpatient/Carrier)

```
DESYNPUF_ID           - Beneficiary ID (FK)
CLM_ID                - Claim ID (PK)
CLM_FROM_DT           - Claim start date
CLM_THRU_DT           - Claim end date
PRVDR_NUM             - Provider identifier
CLM_PMT_AMT           - Medicare payment amount
ICD9_DGNS_CD_1...10   - Diagnosis codes
```

### Prescription Events

```
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

**Alternatives Considered**:

1. **Month partitioning**: Too granular (36 partitions) for a 3-year dataset, resulting in small file problem.
2. **No secondary partition**: Would make member-specific queries scan all data within a year partition.
3. **Full bene_id partitioning**: Would create too many small files (~60,000 per year).

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

   ```
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

### Performance Optimization: Materialized Views

Two key materialized views accelerate API queries:

1. **member_year_metrics**

   ```sql
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

## Incremental Processing (DO NOT IMPLEMENT)

### Strategy: Metadata-Based Change Detection

For handling new or updated claims data:

1. **Track Data Lineage**
   - Store file checksums and processing timestamps in a metadata table
   - Compare checksums to identify changed files

2. **Update Logic by Entity Type**
   - **New beneficiaries**: Insert into dim_beneficiary
   - **Updated beneficiaries**: Update dim_beneficiary using natural key (DESYNPUF_ID + year)
   - **New claims**: Append to fact tables
   - **Updated claims**: Identify by CLM_ID and update with merge operation

3. **View Maintenance**
   - Incrementally update materialized views based on changed data
   - For small changes: Apply incremental updates
   - For large changes: Rebuild affected partitions

### Schema Evolution

As healthcare data standards evolve (e.g., ICD-9 to ICD-10 transition):

1. **Versioning Strategy**
   - Track schema version in table metadata
   - Maintain backward compatibility by keeping core fields unchanged
   - Document changes in a schema registry

2. **Implementation Approach**
   - Add new columns as nullable with default values
   - Use view overlays to present consistent interfaces
   - Create schema migration tools for major changes

## Scaling Strategy (DO NOT IMPLEMENT)

To handle 10x data volume (12GB → 120GB):

1. **Distributed Processing**
   - Implement Spark-based processing to distribute work
   - Process partitions independently and in parallel
   - Leverage dynamic resource allocation based on partition size

2. **Storage Optimizations**
   - **Z-ordering** on beneficiary_id within partitions
   - **Right-sized files**: Target 100-500MB files (vs. many small files)
   - **Bloom filters** for high-cardinality columns like CLM_ID

3. **Query Optimization**
   - Selective column projection to minimize I/O
   - Predicate pushdown for filtering early in query execution
   - Cache frequently accessed aggregates

## API Implementation

The FastAPI endpoint (GET /patient/{bene_id}?year=YYYY) leverages the data model for efficient retrieval:

1. **Data Access Pattern**
   - Query pre-calculated member_year_metrics view
   - Join with top_diagnoses_by_member
   - Format as JSON response

2. **Performance Considerations**
   - Redis caching for frequent requests
   - Partitioned data access based on bene_id_prefix
   - Request-level security and validation

## Complete Data Flow Diagram

```
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
