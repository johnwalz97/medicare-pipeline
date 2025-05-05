# Arlo Data Engineering Challenge - Project Summary

## Original Challenge Description

Welcome to the Arlo Data Engineering Challenge! Arlo ingests high‑volume medical and prescription claims every day, enriches them with clinical and network context, and then runs real‑time models so our underwriting engine can price new groups within minutes. For this challenge, you’ll simulate a slice of that workflow using the publicly available CMS DE‑SynPUF dataset (synthetic Medicare claims).

Using the dataset (see Appendix), develop a solution that answers the following at the *member/year* level:

1. **Annual Spend**

   * Total allowed
   * Total paid
   * Inpatient stays (≈ distinct `clm_id` rows in the IP file)
   * Outpatient visits (≈ distinct `clm_id` rows in the OP file)
   * RX fills (≈ rows in the PDE file)
2. **Top Diagnoses**

   * The 5 diagnoses (ICD‑9 codes) with the highest paid dollars for that member‑year
3. **Provider Breadth**

   * Count of distinct billing providers seen by the member in that year

> **Note:** These files use ICD‑9 codes. Use online help accordingly.

### Requirements & Approach

#### 1. Ingestion

* **Convert** raw CSVs to Delta/Parquet (Spark, Polars, dbt, or another scalable tool).
* **Normalize** column names and data types.
* **Partition Strategy:**

  * Choose partition keys (e.g., year, `desynpuf_id`) to balance write‑conflict mitigation and query efficiency. Explain your rationale and any alternatives considered.

#### 2. Modeling

* **Schema Design:** Create a model (star schema, Data Vault, or other) that answers the questions without rescanning raw files.
* **Explanation:** Justify your choice of dimensional/fact tables or other modeling techniques.

#### 3. Incremental Refresh

* **Strategy:** Describe how to handle new or updated claims continuously rolling in (e.g., merge/upsert logic, watermarking).

---

## Project Overview

This project implements a data pipeline to analyze Medicare claims data from the CMS DE-SynPUF dataset. The solution processes claims data to answer key questions about member-level healthcare utilization and spending patterns. This challenge simulates a slice of Arlo's workflow where we ingest high-volume medical and prescription claims, enrich them with clinical and network context, and run real-time models for underwriting engine pricing.

## Key Requirements

1. **Data Ingestion**
   * Download and extract raw zip files
   * Convert raw CSVs to Delta/Parquet format
   * Normalize column names and types
   * Implement efficient partition strategy
   * Handle incremental data updates

2. **Analysis Requirements**
   * Annual spend metrics:
     * Total allowed
     * Total paid
     * Inpatient stays (≈ distinct `clm_id` rows in the IP file)
     * Outpatient visits (≈ distinct `clm_id` rows in the OP file)
     * RX fills (≈ rows in the PDE file)
   * Top 5 diagnoses by paid dollars
   * Provider breadth (distinct billing providers)

3. **Optional API**
   * FastAPI endpoint: GET /patient/{bene_id}?year=YYYY
   * Returns member-level metrics and top diagnoses
   * Sample Response:

     ```json
     {
       "bene_id": "200284",
       "year": 2009,
       "total_allowed": 13250.43,
       "total_paid": 11206.07,
       "inpatient_stays": 1,
       "outpatient_visits": 3,
       "rx_fills": 12,
       "unique_providers": 4,
       "top_diagnoses": [
         {"code": "250.00", "description": "Diabetes mellitus", "spend": 2423.50},
         ...
       ]
     }
     ```

## Technical Stack

1. **Core Technologies**
   * Python 3.11+
   * Delta/Parquet for data storage
   * FastAPI (optional)
   * Data processing: Spark/Polars/dbt

2. **Data Sources**
   * CMS DE-SynPUF Samples 1 & 2 (2008-2010)
   * ~1.2 GB zipped data
   * Dataset Documentation: [CMS DE-SynPUF Codebook](https://www.cms.gov/files/document/de-10-codebook.pdf-0)

## Project Structure

```
.
├── data/                         # Data directory (not in git)
│   ├── raw/                      # Original CSV files
│   └── processed/                # Processed Delta/Parquet files
├── src/                          # Source code
│   ├── medicare_pipeline/        # Main pipeline package
│   │   ├── __init__.py
│   │   ├── download_data.py
│   │   ├── csv_to_parquet.py
│   │   └── data_cleaner.py
│   └── api/                      # API package
│       └── __init__.py
├── scripts/                      # Utility scripts
│   └── setup.sh
├── pyproject.toml                # Python dependencies
├── uv.lock                       # Python dependencies
├── README.md                     # Project documentation
├── PROJECT_SUMMARY.md            # Project documentation
└── DESIGN.md                     # Design decisions and rationale
```

## Key Design Decisions

1. **Storage & Partition Strategy**
   * Document rationale for chosen partition keys
   * Consider write-conflict and query-skipping implications
   * Include at least one alternative considered

2. **Data Modeling**
   * Choose between star schema, Data Vault, or other approach
   * Document modeling decisions and alternatives
   * Justify choice of dimensional/fact tables or other modeling techniques

3. **Incremental Processing**
   * Strategy for handling new claims
   * Schema evolution approach
   * Merge/upsert logic and watermarking considerations

4. **Scale Optimization**
   * One key optimization for 10x scale
   * Cost or latency considerations

## Deliverables

1. **Code**
   * Git repository with runnable code
   * Python scripts or notebooks
   * Data processing pipeline
   * Optional FastAPI implementation

2. **Documentation**
   * Design brief or Loom video covering:
     * Storage & partition rationale
     * Incremental-load strategy
     * Schema evolution approach
     * Scale optimization
   * Setup instructions
   * Usage examples
