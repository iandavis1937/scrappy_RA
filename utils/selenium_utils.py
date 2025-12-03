

import os
import time
import random
from typing import List, Dict, Callable

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from bs4 import BeautifulSoup

import polars as pl

from . import gen_utils


def setup_driver():
    """Setup and return a configured Selenium WebDriver"""
    print("Setting up Selenium WebDriver...")
    chrome_options = Options()
    
    # Uncomment the line below to run in headless mode (background)
    # chrome_options.add_argument('--headless')
    
    # Anti-detection measures
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=chrome_options)
    
    # Remove webdriver property
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver


def get_soup_selenium(driver, url, wait_selectors=None, wait_time=30, sleep_time=[1.5, 3], save_debug_html=True):
    """
    Load a URL in Selenium, wait for an element, return BeautifulSoup.
    """
    # Navigate to the URL
    driver.get(url)

    # Save the page source for debugging
    if save_debug_html:
        with open(f'selenium_page.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print(f"Saved page source to selenium_page.html")
    
    # Wait for job records to load - try multiple selectors                
    try:
        if wait_selectors:
            WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, wait_selectors))
            )
            
    except TimeoutException:
        print("Timeout waiting for job records to load")
        print("If enabled, check selenium_page.html to see what loaded")
        

    time.sleep(random.uniform(sleep_time[0], sleep_time[1]))
    
    # Get page source and parse with BeautifulSoup
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    return soup


def scrape_selenium(
    base_url,
    search_kw,
    parser: Callable[[BeautifulSoup, str], list[dict]],
    wait_selectors: str=".record, .row.record, div[class*='record']",
    out_cols: list=['title', 'organization', 'location', 'url', 'salary', 'category', 'posted_date', 'priority', 'job_code'],
    save_debug_html: bool=True
    ):
    """
    Scrape job listings using Selenium
    
    Args:
        base_url: The search results URL
    """
    page = 1
    all_jobs = []
    
    driver = setup_driver()
    
    try:
        
        while True:
            print(f"\nFetching page {page}...", end=" ")
            
            try:
                # Make URL with keywords
                url = gen_utils.build_search_url(base_url, search_kw, 'filterKeywords')
                print(url)
                
                soup = get_soup_selenium(
                    driver,
                    url=url,
                    wait_selectors=wait_selectors,
                    wait_time=30,
                    sleep_time=[1.5, 3],
                    save_debug_html=True
                )
                
                # Save HTML for debugging
                if save_debug_html:
                    with open('page_debug.html', 'w', encoding='utf-8') as f:
                        f.write(soup.prettify())
                    print("Saved first page HTML to page_debug.html")
                    
                
                jobs = parser(soup, base_url)
                if not jobs:
                    print("No jobs found on this page → stopping.")
                    break
                
                all_jobs.extend(jobs)
                            
                # Check if there's a next page button and click it
                try:
                    # Look for next page button - adjust selector based on actual pagination
                    next_button = driver.find_element(By.LINK_TEXT, "Next")
                    
                    # Check if button is disabled
                    if 'disabled' in next_button.get_attribute('class'):
                        print("\nReached last page.")
                        break
                    
                    # Scroll to button and click
                    driver.execute_script("arguments[0].scrollIntoView();", next_button)
                    time.sleep(1)
                    next_button.click()
                    
                    # Wait for new page to load
                    time.sleep(random.uniform(2, 4))
                    page += 1
                    
                except NoSuchElementException:
                    print("\nNo next page button found. Reached last page.")
                    break
                except Exception as e:
                    print(f"\nError navigating to next page: {e}")
                    break
    
            except Exception as e:
                print(f"Fatal error during scraping: {e}")
            
    finally:
        # Always close the browser
        print("\nClosing browser...")
        driver.quit()
    
    # Make Data Frame
    if all_jobs:
        print(f"\nConverting {len(all_jobs)} jobs to Polars DataFrame...")
        
        try:
            # Convert list of dicts to Polars DataFrame
            df = pl.DataFrame(all_jobs)
            
            # Reorder columns for consistency
            # Only select columns that exist in the dataframe
            existing_cols = [col for col in out_cols if col in df.columns]
            df = df.select(existing_cols)
            
            print(f"✓ Successful -- DataFrame shape: {df.shape}")
            
            return df
            
        except Exception as e:
            print(f"✗ Error creating DataFrame: {e}")
            return None
    else:
        print("No jobs found to save.")
        return None