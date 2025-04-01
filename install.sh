#!/bin/bash

# Snowflake to Ellie Transfer Tool Installation Script

echo "==============================================="
echo "  Snowflake to Ellie Transfer Tool Installer"
echo "==============================================="
echo

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is required but not found."
    echo "Please install Python 3 before continuing."
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 --version | cut -d " " -f 2)
echo "Using Python version: $PYTHON_VERSION"

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv || { echo "Failed to create virtual environment. Please ensure you have venv installed."; exit 1; }
source venv/bin/activate || { echo "Failed to activate virtual environment."; exit 1; }

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt || { echo "Failed to install requirements."; exit 1; }

# Install the ellie package in development mode
echo "Installing ellie package..."
cd python || { echo "Failed to enter python directory."; exit 1; }
pip install -e . || { echo "Failed to install ellie package."; exit 1; }
cd ..

# Create config directory
echo "Creating config directory..."
mkdir -p config

# Copy example config if it doesn't exist
if [ ! -f config/default_config.yaml ]; then
    echo "Creating example configuration..."
    cp config/example_config.yaml config/default_config.yaml || { echo "Failed to create configuration file."; exit 1; }
fi

echo
echo "Installation complete!"
echo
echo "==== NEXT STEPS ===="
echo "1. Edit the config file: config/default_config.yaml"
echo "   - Add your Snowflake credentials"
echo "   - Add your Ellie API token"
echo "2. Activate the virtual environment: source venv/bin/activate"
echo "3. Navigate to the python directory: cd python"
echo "4. Start the application: streamlit run app.py"
echo
echo "Enjoy using Snowflake to Ellie Transfer Tool!" 