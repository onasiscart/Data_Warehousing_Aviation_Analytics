#!/usr/bin/env python3
"""
Main entry point for running the ETL pipeline.
Run this script from the project root directory.
"""
import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from etl_control_flow import *

if __name__ == "__main__":
    print("ETL pipeline completed successfully.")
