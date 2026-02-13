"""
CES Projects Database Scraper

This script scrapes project data from the CES (Certificación Edificio Sustentable) website,
processes it, and generates consolidated CSV files.

Usage:
    python scrape_projects.py
"""

import pandas as pd
from time import time
from datetime import datetime
from pathlib import Path
from unidecode import unidecode
import logging
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import scraper

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HOME_URL = 'https://www.certificacionsustentable.cl/'
DATA_PATH = Path('./data/raw/')


def single_page_projects(response) -> pd.DataFrame:
    """
    Extract project summary data from a single page.
    
    Args:
        response: HTTP response object containing the page HTML
        
    Returns:
        DataFrame with columns: name, url, image, mandante, arquitecto
    """
    parsed = scraper.parse_html(response.content)
    
    # Extract project information using XPath
    projects_names = parsed.xpath('//div[@class="layer-content"]/a/text()')
    projects_url = parsed.xpath('//div[@class="layer-media"]/a/@href')
    projects_images = parsed.xpath('//div[@class="layer-media"]/a/img/@src')
    projects_mandante_arquitecto = parsed.xpath('//div[@class="layer-content"]/div/text()')
    
    # Split mandante and arquitecto (alternating in the list)
    projects_mandante = projects_mandante_arquitecto[0::2]
    projects_arquitecto = projects_mandante_arquitecto[1::2]

    projects_dict = {
        'name': projects_names,
        'url': projects_url,
        'image': projects_images,
        'mandante': projects_mandante,
        'arquitecto': projects_arquitecto
    }
    
    return pd.DataFrame.from_dict(data=projects_dict)


def get_state_url(state: str) -> Optional[str]:
    """
    Get the URL path for a given certification state.
    
    Args:
        state: Certification state ('en-proceso', 'pre-certificacion', 'certificacion', 'sello-plus')
        
    Returns:
        URL path string, or None if state is invalid
    """
    state_dict = {
        'en-proceso': 'en-proceso/',
        'pre-certificacion': 'pre-certificacion/',
        'certificacion': 'certificacion/',
        'sello-plus': 'sello-plus/'
    }
    return state_dict.get(state)


def get_number_of_pages(state: str) -> int:
    """
    Determine the number of pages for a given certification state.
    
    Args:
        state: Certification state
        
    Returns:
        Number of pages (defaults to 1 if unable to determine)
    """
    state_url = get_state_url(state)
    if not state_url:
        logger.error(f"Invalid state: {state}")
        return 1
        
    url = HOME_URL + state_url
    response = scraper.get_single_page_response(url)
    if not response:
        return 1
        
    parsed = scraper.parse_html(response.content)
    number_of_pages = parsed.xpath('//div[@class="paginate"]/a[@class="page-numbers"]/text()')
    number_of_pages = [int(x) for x in number_of_pages]
    
    return max(number_of_pages) if number_of_pages else 1


def scrape_single_project(url: str, index: int) -> Optional[Dict]:
    """
    Scrape data from a single project page.
    
    Args:
        url: Project URL to scrape
        index: Project index for logging
        
    Returns:
        Dictionary with project data, or None if scraping failed
    """
    response = scraper.get_single_page_response(url)
    if not response:
        logger.warning(f"Failed to fetch {url}")
        return None

    parsed = scraper.parse_html(response.content)

    # Extract project name
    project_name = parsed.xpath('//h1[@class="entry-title"]/text()')
    project_name = project_name[0].strip() if project_name else None

    # Extract project image URL
    project_image_url = parsed.xpath('//figure[@class="wp-block-image size-large"]/img/@src')
    project_image_url = project_image_url[0].strip().replace('http:', 'https:') if project_image_url else None

    # Extract entry date
    project_entry_date = parsed.xpath('//time[@class="entry-date published"]/@datetime')
    project_entry_date = project_entry_date[0][:10].strip() if project_entry_date else None

    project_dict = {
        'project_name': project_name,
        'project_image_url': project_image_url,
        'project_entry_date': project_entry_date
    }

    # Extract project details
    project_details_keys = parsed.xpath('//div[@class="entry-content"]//li/b/text()')
    project_details_keys = [item.strip().replace(':', '').rstrip(':') for item in project_details_keys]
    project_details_keys = [unidecode(item) for item in project_details_keys]

    project_details_values = parsed.xpath('//div[@class="entry-content"]//li/text()')
    project_details_values = [item.strip().replace(':', '').rstrip(':') for item in project_details_values]
    
    project_details_dict = dict(zip(project_details_keys, project_details_values))

    target_keys = [
        'Mandante', 'Arquitecto', 'Unidad tecnica', 'Asesor', 'Entidad Evaluadora',
        'Region', 'Comuna', 'Version de certificacion', 'Nivel obtenido',
        'Fecha de logro obtenido', 'Puntaje obtenido', 'Asesor precertificacion',
        'Entidad evaluadora precertificacion', 'Asesor certificacion',
        'Entidad evaluadora certificacion'
    ]

    for key in target_keys:
        project_dict[key] = project_details_dict.get(key, None)

    return project_dict


def get_project_page_data(state_df: pd.DataFrame, state: str, max_workers: int = 5) -> None:
    """
    Scrape detailed data for each project using concurrent requests and save to CSV.
    
    Args:
        state_df: DataFrame containing project URLs
        state: Certification state name
        max_workers: Number of concurrent workers (default: 5)
    """
    projects_data_list = []
    total_projects = len(state_df)
    logger.info(f"Processing {total_projects} projects for state '{state}' with {max_workers} concurrent workers")

    # Use ThreadPoolExecutor for concurrent requests
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_url = {
            executor.submit(scrape_single_project, url, i): (url, i) 
            for i, url in enumerate(state_df['url'])
        }
        
        # Process completed tasks
        completed = 0
        for future in as_completed(future_to_url):
            url, index = future_to_url[future]
            try:
                project_data = future.result()
                if project_data:
                    projects_data_list.append(project_data)
                completed += 1
                
                if completed % 10 == 0:
                    logger.info(f'Processed {completed}/{total_projects} projects')
            except Exception as e:
                logger.error(f"Error processing {url}: {e}")

    if not projects_data_list:
        logger.warning(f"No detailed data collected for {state}")
        return

    # Create DataFrame and rename columns
    main_df = pd.DataFrame(projects_data_list)
    
    column_name_dict = {
        'project_name': 'project_name',
        'project_image_url': 'project_image_url',
        'project_entry_date': 'project_entry_date',
        'Mandante': 'mandante',
        'Arquitecto': 'arquitecto',
        'Unidad tecnica': 'unidad_tecnica',
        'Asesor': 'asesor',
        'Entidad Evaluadora': 'entidad_evaluadora',
        'Region': 'region',
        'Comuna': 'comuna',
        'Version de certificacion': 'version_certificacion',
        'Nivel obtenido': 'nivel_obtenido',
        'Fecha de logro obtenido': 'fecha_logro_obtenido',
        'Puntaje obtenido': 'puntaje_obtenido',
        'Asesor precertificacion': 'asesor_precertificacion',
        'Entidad evaluadora precertificacion': 'entidad_evaluadora_precertificacion',
        'Asesor certificacion': 'asesor_certificacion',
        'Entidad evaluadora certificacion': 'entidad_evaluadora_certificacion'
    }

    main_df = main_df.rename(columns=column_name_dict)
    main_df.drop_duplicates(inplace=True)
    main_df.reset_index(drop=True, inplace=True)
    
    # Save to CSV
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    filename = f'{datetime.now().strftime("%Y_%m_%d")}-{state}-2.csv'
    full_path = DATA_PATH / filename
    logger.info(f'Saving detailed file: {full_path}')
    main_df.to_csv(full_path, index=False, encoding="utf-8-sig")


def all_pages_projects(state: str) -> None:
    """
    Scrape all pages for a given certification state.
    
    Args:
        state: Certification state name
    """
    logger.info(f"Getting number of pages for {state}...")
    number_of_pages = get_number_of_pages(state)
    logger.info(f"Total pages for {state}: {number_of_pages}")

    state_url = get_state_url(state)
    page_dfs = []
    
    for page_number in range(1, number_of_pages + 1):
        url = HOME_URL + state_url if page_number == 1 else f"{HOME_URL}{state_url}page/{page_number}/"
        logger.info(f"Scraping page {page_number}/{number_of_pages}: {url}")
        
        try:
            response = scraper.get_single_page_response(url)
            if response:
                projects_df = single_page_projects(response)
                page_dfs.append(projects_df)
            else:
                logger.error(f"Failed to retrieve page {page_number}")
        except Exception as e:
            logger.error(f"Exception on page {page_number}: {e}")
            DATA_PATH.mkdir(parents=True, exist_ok=True)
            with open(DATA_PATH / f'{state}_failed.txt', "a") as f:
                f.write(f'The process stopped at page {page_number} out of {number_of_pages}. Error: {e}\n')

    if not page_dfs:
        logger.warning(f"No projects found for {state}")
        return

    # Concatenate and save summary
    main_df = pd.concat(page_dfs, ignore_index=True)
    main_df.drop_duplicates(inplace=True)
    main_df.reset_index(drop=True, inplace=True)
    
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    filename = f'{datetime.now().strftime("%Y_%m_%d")}-{state}-1.csv'
    full_path = DATA_PATH / filename
    logger.info(f'Saving summary file: {full_path}')
    main_df.to_csv(full_path, index=False, encoding="utf-8-sig")
    
    # Scrape detailed data
    logger.info('-------------------------------------------------------------------------------')
    logger.info(f'Getting inside the {state} process: {main_df.shape[0]} projects to download...')
    logger.info('-------------------------------------------------------------------------------\n')
    get_project_page_data(main_df, state)


def consolidate_csvs() -> None:
    """
    Consolidate all project CSV files into a single master file.
    """
    if not DATA_PATH.exists():
        logger.error(f"Directory {DATA_PATH} does not exist.")
        return

    csv_files = list(DATA_PATH.glob('*-2.csv'))
    
    if not csv_files:
        logger.warning("No CSV files found to consolidate.")
        return

    logger.info(f"Found {len(csv_files)} files to consolidate.")
    
    main_df = pd.DataFrame()
    
    for file_path in csv_files:
        try:
            parts = file_path.name.split('-')
            status = "-".join(parts[1:-1])
            
            df_i = pd.read_csv(file_path)
            df_i.insert(1, 'status', status)
            main_df = pd.concat([main_df, df_i], ignore_index=True)
            logger.info(f"Loaded {file_path.name} with status '{status}'")
        except Exception as e:
            logger.error(f"Error reading {file_path.name}: {e}")

    if main_df.empty:
        logger.warning("Resulting DataFrame is empty.")
        return

    main_df = main_df.where(pd.notnull(main_df), None)
    main_df.reset_index(drop=True, inplace=True)

    date_str = datetime.now().strftime("%Y_%m_%d")
    output_filename = f'[CES]_Projects_Full_List-{date_str}.csv'
    output_path = DATA_PATH / output_filename
    
    main_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(f"Successfully saved consolidated data to {output_path}")

    # Cleanup intermediate files
    logger.info("Cleaning up intermediate files...")
    all_temp_files = list(DATA_PATH.glob('*-1.csv')) + list(DATA_PATH.glob('*-2.csv'))
    for temp_file in all_temp_files:
        try:
            temp_file.unlink()
            logger.debug(f"Deleted temporary file: {temp_file.name}")
        except Exception as e:
            logger.warning(f"Could not delete temporary file {temp_file.name}: {e}")
    
    logger.info("Cleanup complete. Only the consolidated file remains.")


def main(states: List[str]) -> None:
    """
    Main execution function.
    
    Args:
        states: List of certification states to scrape
    """
    start_time = time()
    
    for state in states:
        logger.info('\n----------------------------')
        logger.info(f'Starting {state} process')
        logger.info('----------------------------\n')
        all_pages_projects(state)
        
    elapsed_time = time() - start_time
    logger.info("\nElapsed time: %0.2f seconds." % elapsed_time)
    
    # Consolidate CSVs
    logger.info("Consolidating all CSV files...")
    try:
        consolidate_csvs()
    except Exception as e:
        logger.error(f"Error during consolidation: {e}")


if __name__ == '__main__':
    states = ['en-proceso', 'pre-certificacion', 'certificacion', 'sello-plus']
    
    try:
        main(states)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
    except Exception as e:
        logger.exception("An unexpected error occurred during execution.")
