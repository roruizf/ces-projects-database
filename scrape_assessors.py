"""
CES Assessors Data Scraper

This script downloads the latest CES assessors registry from the official website,
processes it, and cleans the data.

Usage:
    python scrape_assessors.py
"""

import pandas as pd
from pathlib import Path
import logging
import requests
from typing import Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Official URL for the assessors registry (Excel format)
ASSESSORS_URL = "https://storage.certificacionsustentable.cl/static/asesores_acreditados.xlsx"
TEMP_FILE = Path('./asesores_acreditados_TEMP.xlsx')
DATA_PATH = Path('./data/raw/')


def download_assessors_file(url: str, dest_path: Path) -> bool:
    """
    Download the assessors Excel file from the provided URL.
    
    Args:
        url: The URL to download from
        dest_path: The local path to save the file
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Downloading assessors registry from {url}...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            f.write(response.content)
        logger.info("Download successful.")
        return True
    except Exception as e:
        logger.error(f"Failed to download file: {e}")
        return False


def parse_spanish_date(date_str: str) -> Optional[datetime]:
    """
    Parse dates in format 'DD mon. YYYY' (Spanish).
    Example: '18 ago. 2020'
    """
    if not isinstance(date_str, str) or not date_str.strip():
        return None
        
    months = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'ago': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
    }
    
    try:
        # Expected format: "18 ago. 2020"
        parts = date_str.lower().replace('.', '').split()
        if len(parts) != 3:
            return None
            
        day = parts[0].zfill(2)
        month_name = parts[1][:3]
        year = parts[2]
        
        month = months.get(month_name)
        if not month:
            return None
            
        return pd.to_datetime(f"{year}-{month}-{day}")
    except Exception:
        return None


def process_assessors() -> None:
    """
    Download, process, and clean the assessors registry data.
    
    This function:
    - Downloads the latest Excel file from the official source
    - Reads the Excel file
    - Cleans and standardizes date formats
    - Selects relevant columns
    - Saves the cleaned data to a new CSV file in data/raw/
    """
    if not download_assessors_file(ASSESSORS_URL, TEMP_FILE):
        logger.error("Could not proceed without the registry file.")
        return

    logger.info(f"Reading {TEMP_FILE}...")
    try:
        # Using openpyxl as engine for .xlsx files
        df = pd.read_excel(TEMP_FILE, engine='openpyxl')
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        if TEMP_FILE.exists():
            TEMP_FILE.unlink()
        return

    # Select target columns and cleanup names
    df.columns = [col.strip() for col in df.columns]
    
    date_col = 'Fecha de inscripción'
    target_columns = [
        'Región', date_col, 'N° Inscripción', 'Apellido Paterno', 
        'Apellido Materno', 'Nombre(s)', 'RUT', 'Teléfono', 'email',
        'Cantidad de proyectos con actividad en los últimos 3 años'
    ]
    
    # Check if all columns exist
    missing_cols = [col for col in target_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing columns in downloaded file: {missing_cols}")
        logger.info(f"Available columns: {list(df.columns)}")
        if TEMP_FILE.exists():
            TEMP_FILE.unlink()
        return

    # Process dates
    logger.info("Processing dates...")
    try:
        # Try custom parser for Spanish format first
        df[date_col] = df[date_col].apply(lambda x: parse_spanish_date(x) if isinstance(x, str) else x)
        # Convert to datetime (if not already)
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    except Exception as e:
        logger.error(f"Error processing dates: {e}")

    df_to_csv = df[target_columns]

    # Standardized output filename
    date_str = datetime.now().strftime("%Y_%m_%d")
    output_filename = f'[CES]_Registro_AsesoresCES_Full_List-{date_str}.csv'
    output_path = DATA_PATH / output_filename

    DATA_PATH.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving cleaned data to {output_path}...")
    df_to_csv.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    # Clean up temporary file
    if TEMP_FILE.exists():
        TEMP_FILE.unlink()
        
    logger.info("Done.")


if __name__ == '__main__':
    process_assessors()
