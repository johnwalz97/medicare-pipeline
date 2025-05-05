#!/bin/bash

# Exit on error
set -e

# Install uv if not already installed
if ! command -v uv &>/dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    uv venv
fi

# Activate virtual environment
source .venv/bin/activate

# Install dependencies using uv
echo "Installing dependencies..."
uv pip install -e ".[dev]"

# Create necessary directories
mkdir -p data/{raw,processed}

echo "Setup complete! Activate the virtual environment with:"
echo "source .venv/bin/activate"
