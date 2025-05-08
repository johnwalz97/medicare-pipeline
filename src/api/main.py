from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional
import polars as pl
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="Medicare Claims API",
    description="API for querying Medicare claims data at the member/year level",
    version="1.0.0",
)

# Define data paths
BASE_DATA_PATH = Path("data/gold")
MEMBER_METRICS_PATH = BASE_DATA_PATH / "member_year_metrics"
TOP_DIAGNOSES_PATH = BASE_DATA_PATH / "top_diagnoses_by_member"
PATIENT_API_VIEW_PATH = BASE_DATA_PATH / "patient_api_view"


# Define response models
class Diagnosis(BaseModel):
    code: str
    description: Optional[str] = None
    spend: float = Field(..., description="Total paid amount for this diagnosis")


class PatientYearResponse(BaseModel):
    bene_id: str
    year: int
    total_allowed: float = Field(..., description="Total allowed amount")
    total_paid: float = Field(..., description="Total paid amount")
    inpatient_stays: int = Field(..., description="Count of inpatient stays")
    outpatient_visits: int = Field(..., description="Count of outpatient visits")
    rx_fills: int = Field(..., description="Count of prescription fills")
    unique_providers: int = Field(..., description="Count of unique providers")
    top_diagnoses: List[Diagnosis] = Field(
        ..., description="Top 5 diagnoses by paid amount"
    )


def load_patient_data(bene_id: str, year: int) -> Optional[dict]:
    """
    Load patient data from the pre-computed views.

    Args:
        bene_id: Patient beneficiary ID
        year: Year of data

    Returns:
        Dictionary with combined patient data or None if not found
    """
    # Direct path to the metrics file without patient_api_view folder structure
    patient_metrics_path = (
        PATIENT_API_VIEW_PATH / f"year=({year},)" / "patient_metrics.parquet"
    )

    # Check if metrics file exists
    if not patient_metrics_path.exists():
        logger.warning(f"Patient metrics file does not exist for year {year}")
        return None

    # Load the entire metrics file
    logger.info(f"Loading metrics from {patient_metrics_path}")
    metrics_df = pl.read_parquet(patient_metrics_path)

    # Filter for the specific patient
    logger.info(f"Filtering for patient {bene_id}")
    filtered_df = metrics_df.filter(pl.col("bene_id") == bene_id)

    if len(filtered_df) == 0:
        logger.warning(f"Patient {bene_id} not found in metrics for year {year}")
        return None

    # Convert to dictionary
    patient_data = filtered_df.row(0, named=True)
    logger.info(f"Found patient data: {patient_data}")

    # Check for diagnoses file
    diagnoses_path = (
        PATIENT_API_VIEW_PATH / f"year=({year},)" / "patient_diagnoses.parquet"
    )
    diagnoses = []

    if diagnoses_path.exists():
        # Load and filter diagnoses
        diagnoses_df = pl.read_parquet(diagnoses_path)
        filtered_diagnoses = diagnoses_df.filter(pl.col("bene_id") == bene_id)

        if len(filtered_diagnoses) > 0:
            # Sort by payment amount if rank not available
            if "diagnosis_rank" in filtered_diagnoses.columns:
                sorted_diagnoses = filtered_diagnoses.sort("diagnosis_rank").head(5)
            else:
                sorted_diagnoses = filtered_diagnoses.sort(
                    "diagnosis_payment", descending=True
                ).head(5)

            # Extract diagnosis data
            for row in sorted_diagnoses.iter_rows(named=True):
                diagnoses.append(
                    {
                        "code": row.get("diagnosis_code", ""),
                        "description": row.get("diagnosis_description", None),
                        "spend": float(row.get("diagnosis_payment", 0)),
                    }
                )

    # Add diagnoses to patient data
    patient_data = dict(patient_data)
    patient_data["diagnoses"] = diagnoses

    return patient_data


@app.get("/")
def read_root():
    return {"message": "Medicare Claims API"}


@app.get("/patient/{bene_id}", response_model=PatientYearResponse)
def get_patient_year_data(bene_id: str, year: int = Query(..., ge=2008, le=2010)):
    """
    Get patient data for a specific year.

    Args:
        bene_id: Patient beneficiary ID
        year: Year of data (2008-2010)

    Returns:
        PatientYearResponse with metrics and top diagnoses
    """
    # Load patient data
    patient_data = load_patient_data(bene_id, year)

    if patient_data is None:
        raise HTTPException(
            status_code=404, detail=f"Patient {bene_id} not found for year {year}"
        )

    # Extract diagnoses data
    diagnoses_data = [
        Diagnosis(
            code=diag["code"], description=diag["description"], spend=diag["spend"]
        )
        for diag in patient_data.get("diagnoses", [])
    ]

    # Create response
    response = PatientYearResponse(
        bene_id=patient_data["bene_id"],
        year=year,
        total_allowed=float(patient_data.get("total_allowed", 0)),
        total_paid=float(patient_data.get("total_paid", 0)),
        inpatient_stays=int(patient_data.get("inpatient_stays", 0)),
        outpatient_visits=int(patient_data.get("outpatient_visits", 0)),
        rx_fills=int(patient_data.get("rx_fills", 0)),
        unique_providers=int(patient_data.get("unique_providers", 0)),
        top_diagnoses=diagnoses_data,
    )

    return response


# Health check endpoint
@app.get("/health")
def health_check():
    """
    Health check endpoint to verify API is running.
    """
    # Check if we can access the data directories
    health_status = {
        "status": "healthy",
        "data_paths_exist": {
            "member_metrics": MEMBER_METRICS_PATH.exists(),
            "top_diagnoses": TOP_DIAGNOSES_PATH.exists(),
            "patient_api_view": PATIENT_API_VIEW_PATH.exists(),
        },
        "data_files_exist": {
            "2008_metrics": (
                PATIENT_API_VIEW_PATH / "year=(2008,)" / "patient_metrics.parquet"
            ).exists(),
            "2008_diagnoses": (
                PATIENT_API_VIEW_PATH / "year=(2008,)" / "patient_diagnoses.parquet"
            ).exists(),
            "2009_metrics": (
                PATIENT_API_VIEW_PATH / "year=(2009,)" / "patient_metrics.parquet"
            ).exists(),
            "2009_diagnoses": (
                PATIENT_API_VIEW_PATH / "year=(2009,)" / "patient_diagnoses.parquet"
            ).exists(),
            "2010_metrics": (
                PATIENT_API_VIEW_PATH / "year=(2010,)" / "patient_metrics.parquet"
            ).exists(),
            "2010_diagnoses": (
                PATIENT_API_VIEW_PATH / "year=(2010,)" / "patient_diagnoses.parquet"
            ).exists(),
        },
    }

    # If any data path doesn't exist, mark as unhealthy
    if not all(health_status["data_paths_exist"].values()):
        health_status["status"] = "unhealthy"

    return health_status
