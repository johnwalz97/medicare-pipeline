#!/usr/bin/env python3

import uvicorn
import argparse
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """
    Run the Medicare Claims API server.
    """
    parser = argparse.ArgumentParser(description="Run the Medicare Claims API server")
    parser.add_argument(
        "--host", default="0.0.0.0", help="Host address to bind to (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=8000, help="Port to listen on (default: 8000)"
    )
    parser.add_argument(
        "--reload", action="store_true", help="Enable auto-reload for development"
    )
    parser.add_argument(
        "--workers", type=int, default=1, help="Number of worker processes (default: 1)"
    )

    args = parser.parse_args()

    # Check if the data directories exist
    data_dir = Path("data/gold")
    if not data_dir.exists():
        logger.warning(f"Data directory {data_dir} does not exist.")
        logger.warning("The API may not function correctly without data.")

    # Start the API server
    logger.info(f"Starting Medicare Claims API server on {args.host}:{args.port}")
    uvicorn.run(
        "api.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
