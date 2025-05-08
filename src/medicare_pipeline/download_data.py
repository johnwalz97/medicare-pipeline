#!/usr/bin/env python3

import requests
import zipfile
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataDownloader:
    """Handles downloading and extracting CMS DE-SynPUF data files."""

    def __init__(self, output_dir: str = "data/raw"):
        """
        Initialize the downloader.

        Args:
            output_dir: Directory where files will be downloaded and extracted
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Define file URLs for Sample 1
        self.file_urls = {
            # Beneficiary Summary Files
            "DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip": "https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_beneficiary_summary_file_sample_1.zip",
            "DE1_0_2009_Beneficiary_Summary_File_Sample_1.zip": "https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2009_beneficiary_summary_file_sample_1.zip",
            "DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip": "https://www.cms.gov/sites/default/files/2020-09/DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip",
            # Carrier Claims
            "DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip": "http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip",
            "DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip": "http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip",
            # Inpatient and Outpatient Claims
            "DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.zip": "https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_to_2010_inpatient_claims_sample_1.zip",
            "DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.zip": "https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_to_2010_outpatient_claims_sample_1.zip",
            # Prescription Drug Events
            "DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.zip": "http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.zip",
        }

    def download_file(self, url: str, filename: str) -> bool:
        """
        Download a single file.

        Args:
            url: URL to download from
            filename: Name to save the file as
        """
        output_path = self.output_dir / filename

        logger.info(f"Downloading {filename}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        block_size = 8192
        downloaded = 0

        with open(output_path, "wb") as f:
            for data in response.iter_content(block_size):
                downloaded += len(data)
                f.write(data)

                # Log progress
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    logger.info(f"Downloaded {percent:.1f}% of {filename}")

    def extract_zip(self, zip_path: Path) -> bool:
        """
        Extract a ZIP file.

        Args:
            zip_path: Path to the ZIP file
        """
        logger.info(f"Extracting {zip_path.name}...")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(self.output_dir)

    def download_and_extract_all(self) -> bool:
        """
        Download and extract all files.
        """
        for filename, url in self.file_urls.items():
            self.download_file(url, filename)

            zip_path = self.output_dir / filename
            self.extract_zip(zip_path)

            zip_path.unlink()


def main():
    """Main entry point for the script."""
    downloader = DataDownloader()
    downloader.download_and_extract_all()

    logger.info("All files downloaded and extracted successfully.")


if __name__ == "__main__":
    main()
