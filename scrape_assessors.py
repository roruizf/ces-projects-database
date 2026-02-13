"""
CES Assessors Data Processor

This script processes and cleans the CES assessors registry data.

Usage:
    python scrape_assessors.py
"""

import pandas as pd
from pathlib import Path
import logging
from typing import Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

INPUT_FILE = Path('./Registro_AsesoresCES_v41.2022.csv')
DATA_PATH = Path('./data/raw/')


def process_assessors(input_file: Path = INPUT_FILE) -> None:
    """
    Process and clean the assessors registry data.
    
    This function:
    - Reads the raw assessors CSV file
    - Cleans and standardizes date formats
    - Selects relevant columns
    - Saves the cleaned data to a new CSV file in data/raw/
    
    Args:
        input_file: Path to the input CSV file
    """
    if not input_file.exists():
        logger.error(f"Input file {input_file} not found.")
        return

    logger.info(f"Reading {input_file}...")
    df = pd.read_csv(input_file)

    # Process dates
    logger.info("Processing dates...")
    try:
        df['Fecha inscripción'] = df['Fecha inscripción'].str.capitalize()
        new = df['Fecha inscripción'].str.split('-', expand=True)
        df['Mes inscripción'] = new[0].str[:3]
        df['Año inscripción'] = pd.to_numeric(new[1])
        
        # Adjust year (add 2000 if year < 2000)
        df.loc[df['Año inscripción'] < 2000, 'Año inscripción'] += 2000

        # Map Spanish months to English abbreviations
        month_dict = {
            'Ene': 'Jan', 'Feb': 'Feb', 'Mar': 'Mar', 'Abr': 'Apr',
            'May': 'May', 'Jun': 'Jun', 'Jul': 'Jul', 'Ago': 'Aug',
            'Sep': 'Sep', 'Oct': 'Oct', 'Nov': 'Nov', 'Dic': 'Dec'
        }
        
        df['Mes inscripción'] = df['Mes inscripción'].map(month_dict)
        
        # Create datetime column
        df['Fecha inscripción'] = pd.to_datetime(
            df['Mes inscripción'] + '-' + df['Año inscripción'].astype(str), 
            format='%b-%Y'
        )
    except Exception as e:
        logger.error(f"Error processing dates: {e}")
        return

    # Select target columns
    target_columns = [
        'Región', 'Fecha inscripción', 'N° Inscripción', 'Apellido Paterno', 
        'Apellido Materno', 'Nombre(s)', 'RUT', 'Teléfono', 'email'
    ]
    
    # Check if all columns exist
    missing_cols = [col for col in target_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing columns in input file: {missing_cols}")
        return

    df_to_csv = df[target_columns]

    # Standardized output filename
    date_str = datetime.now().strftime("%Y_%m_%d")
    output_filename = f'[CES]_Registro_AsesoresCES_Full_List-{date_str}.csv'
    output_path = DATA_PATH / output_filename

    DATA_PATH.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving cleaned data to {output_path}...")
    df_to_csv.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info("Done.")


if __name__ == '__main__':
    process_assessors()
