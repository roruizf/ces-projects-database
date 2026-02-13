"""
Web scraping utilities for CES Projects Database.

This module provides helper functions for HTTP requests and HTML parsing.
"""

import requests
import time
import logging
import lxml.html as html
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_single_page_response(url: str, max_retries: int = 10, sleep_time: int = 1) -> Optional[requests.Response]:
    """
    Download data from a URL with retry logic.
    
    This function attempts to fetch a URL and retries on failure with exponential backoff.
    
    Args:
        url: The URL to request
        max_retries: Maximum number of retries for non-200 status codes
        sleep_time: Time in seconds to sleep between retries
        
    Returns:
        The response object if successful, None otherwise
    """
    try_number = 1
    
    while try_number <= max_retries:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return response
            else:
                logger.warning(
                    f"Status code {response.status_code} at try {try_number} for {url}. "
                    f"Retrying in {sleep_time}s..."
                )
        except requests.RequestException as e:
            logger.error(f"Request exception at try {try_number} for {url}: {e}")

        try_number += 1
        time.sleep(sleep_time)

    logger.error(f"Max retries reached ({max_retries}) for {url}.")
    return None


def parse_html(content: bytes) -> html.HtmlElement:
    """
    Parse HTML content using lxml.
    
    Args:
        content: The HTML content as bytes
        
    Returns:
        The parsed HTML element tree
    """
    return html.fromstring(content)
