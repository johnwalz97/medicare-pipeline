# Arlo Data Engineering Challenge

This project implements a data pipeline to analyze Medicare claims data from the CMS DE-SynPUF dataset, providing insights into member-level healthcare utilization and spending patterns.

See the [DESIGN.md](./DESIGN.md) for detailed design decisions and the [PROJECT_SUMMARY.md](./PROJECT_SUMMARY.md) for a detailed overview of the project.

## Prerequisites

- Python 3.11+

## Project Structure

```plain
.
├── data/                         # Data directory (not in git)
│   ├── raw/                      # Original CSV files
│   ├── bronze/                   # Normalized Parquet files
│   ├── silver/                   # Dimensional model
│   └── gold/                     # Analytics views
├── src/                          # Source code
│   ├── medicare_pipeline/        # Main pipeline package
│   │   ├── __init__.py
│   │   ├── main.py               # Pipeline orchestration
│   │   ├── download_data.py      # Data acquisition
│   │   ├── csv_to_parquet.py     # Bronze layer conversion
│   │   ├── data_cleaner.py       # Data cleanup
│   │   ├── transform_data.py     # Silver layer transformation
│   │   ├── create_analytics.py   # Gold layer creation
│   │   └── validate_data.py      # Data validation
│   └── api/                      # API package
│       ├── __init__.py
│       ├── main.py               # FastAPI implementation
│       ├── server.py             # API server
│       └── test_client.py        # Test client for API
├── scripts/                      # Utility scripts
│   └── setup.sh                  # Environment setup
├── pyproject.toml                # Python dependencies
├── uv.lock                       # Dependency lock file
├── README.md                     # Project documentation
├── PROJECT_SUMMARY.md            # Project summary
└── DESIGN.md                     # Design decisions and rationale
```

## Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/johnwalz97/medicare-pipeline.git
   cd medicare-pipeline
   ```

2. Create a virtual environment:

   ```bash
   python -m venv .venv
   ```

3. Activate the virtual environment:

   ```bash
   source .venv/bin/activate
   ```

4. Install the dependencies:

   ```bash
   pip install -e .
   ```

## Usage

1. Running the Full Pipeline

   ```bash
   python -m src.medicare_pipeline.main --steps all
   ```

2. Running Specific Pipeline Steps (Optional)

   ```bash
   # Download data only
   python -m src.medicare_pipeline.main --steps download

   # Convert CSV to Parquet
   python -m src.medicare_pipeline.main --steps convert

   # Transform to dimensional model
   python -m src.medicare_pipeline.main --steps transform

   # Create analytics views
   python -m src.medicare_pipeline.main --steps analytics

   # Validate the data
   python -m src.medicare_pipeline.main --steps validate
   ```

3. Running the API

   ```bash
   python -m src.api.server --host 127.0.0.1 --port 8000
   ```

## API Documentation

### API Endpoints

#### GET /patient/{bene_id}?year={year}

Get patient data for a specific beneficiary and year.

**Parameters:**

- `bene_id`: Patient beneficiary ID (path parameter)
- `year`: Year of data (query parameter, 2008-2010)

**Response:**

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
    {"code": "401.9", "description": "Hypertension NOS", "spend": 1528.33},
    {"code": "272.4", "description": "Hyperlipidemia NEC/NOS", "spend": 982.12},
    {"code": "786.50", "description": "Chest pain NOS", "spend": 750.00},
    {"code": "414.00", "description": "Coronary atherosclerosis", "spend": 522.12}
  ]
}
```

#### GET /health

Health check endpoint to verify API is running.

**Response:**

```json
{
  "status": "healthy",
  "data_paths_exist": {
    "member_metrics": true,
    "top_diagnoses": true,
    "patient_api_view": true
  },
  "data_files_exist": {
    "2008_metrics": true,
    "2008_diagnoses": true,
    "2009_metrics": true,
    "2009_diagnoses": true,
    "2010_metrics": true,
    "2010_diagnoses": true
  }
}
```

### Using the Test Client

You can use the included test client to query the API:

```bash
python -m src.api.test_client.py --bene_id F900242EC7459BD5 --year 2009 --pretty
```

### Hosted API Documentation

Once the API is running, you can access the Swagger UI documentation at:

<http://localhost:8000/docs>

And the ReDoc documentation at:

<http://localhost:8000/redoc>

## Contributing

To get started in development mode, follow the instructions below.

### 1. Environment Setup

This project uses [`uv`](https://github.com/astral-sh/uv) for fast dependency management and virtual environments.

- **Install `uv` (if not already installed):**

  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

- **Create a virtual environment:**

  ```bash
  uv venv
  ```

- **Activate the virtual environment:**

  ```bash
  source .venv/bin/activate
  ```

- **Install all dependencies (including development tools):**

  ```bash
  uv pip install -e ".[dev]"
  ```

### 2. Code Formatting and Linting

- **Format and lint the codebase using Ruff:**

  ```bash
  ruff check --fix .
  ```

  This will automatically fix most formatting and linting issues.
