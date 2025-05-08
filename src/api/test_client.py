import requests
import json
import argparse
import sys
from typing import Optional


def query_patient(
    bene_id: str, year: int, base_url: str = "http://localhost:8000"
) -> Optional[dict]:
    """
    Query the patient API endpoint.

    Args:
        bene_id: Patient beneficiary ID
        year: Year of data
        base_url: Base URL of the API server

    Returns:
        Patient data as dictionary or None if error
    """
    url = f"{base_url}/patient/{bene_id}?year={year}"

    response = requests.get(url)
    response.raise_for_status()

    return response.json()


def main():
    """Main entry point for the test client."""
    parser = argparse.ArgumentParser(description="Test client for Medicare Claims API")
    parser.add_argument("bene_id", help="Patient beneficiary ID")
    parser.add_argument(
        "--year", type=int, default=2009, help="Year of data (default: 2009)"
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="Base URL of the API server (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--pretty", action="store_true", help="Pretty print the JSON response"
    )

    args = parser.parse_args()

    # Query the API
    result = query_patient(args.bene_id, args.year, args.url)
    if result:
        if args.pretty:
            print(json.dumps(result, indent=2))
        else:
            print(result)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
