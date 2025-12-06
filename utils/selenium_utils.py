

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


def get_soup_selenium(
    driver,
    url,
    wait_selectors=None,
    wait_time=30,
    sleep_time=[1.5, 3],
    enable_scroll=True,
    scroll_container_id: str="win0divHRS_AGNT_RSLT_I$grid$0",
    enable_search_box=False,
    search_box_id: str='HRS_SCH_WRK_HRS_SCH_TEXT100',
    search_button_id: str='HRS_SCH_WRK_FLU_HRS_SEARCH_BTN',
    search_kw: str='data',
    no_results_id: str='win0divHRS_SCH_WRK_HRS_CC_NO_RSLT',
    save_debug_html=True
    ):
    """
    Load a URL in Selenium, wait for an element, return BeautifulSoup.
    """
    # Navigate to the URL
    driver.get(url)

    # Save the page source for debugging
    if save_debug_html:
        with open(f'selenium_page_init.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print(f"\nSaved page source to selenium_page_init.html")
    
    # Wait for job records to load - try multiple selectors                
    try:
        if wait_selectors:
            WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, wait_selectors))
            )
            
    except TimeoutException:
        print("Timeout waiting for job records to load")
        print("If enabled, check selenium_page.html to see what loaded")
    
    # Wait for and interact with search box
    if enable_search_box:
        try:
            kw_has_results = get_search_box_results(
                driver=driver,
                search_box_id=search_box_id,
                search_button_id=search_button_id,
                search_kw=search_kw,
                no_results_id=no_results_id,
                wait_selectors=wait_selectors,
                wait_time=10
                )
            print(f"kw_has_results: {kw_has_results}")
            if not kw_has_results:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                return soup
                    
        except Exception as e:
            print(f"Search did not work: {e}")
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            return soup
        
    
    # Scroll down page to load all jobs    
    if enable_scroll:
        print("Starting to scroll and load all jobs...")
        try:
            n_jobs_top_to_bottom = scroll_and_load_all(driver, scroll_container_id)
            print(f"{n_jobs_top_to_bottom} found")
            
        except Exception as e:
            print(f"Scroll Error: {e}")
            
    else:
        time.sleep(random.uniform(sleep_time[0], sleep_time[1]))
    
    # Get page source and parse with BeautifulSoup
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    return soup


def get_search_box_results(
    driver,
    search_box_id,
    search_button_id,
    search_kw,
    no_results_id,
    wait_selectors,   # <-- you set this
    wait_time=10
):
    print(f"Searching for: {search_kw}")

    # Type search kw
    search_box = WebDriverWait(driver, wait_time).until(
        EC.presence_of_element_located((By.ID, search_box_id))
    )
    search_box.clear()
    search_box.send_keys(search_kw)

    # Click search
    driver.find_element(By.ID, search_button_id).click()

    # Wait for page load
    total_seconds = 10
    for i in range(total_seconds, 0, -1):
        print(f"Waiting... {i} seconds remaining ", end="\r")
        time.sleep(1)

    # Race: either results OR no-results
    try:
        WebDriverWait(driver, wait_time).until(
            lambda d: (
                d.find_elements(By.CSS_SELECTOR, wait_selectors) or
                d.find_elements(By.ID, no_results_id)
            )
        )
    except TimeoutException:
        print("⚠ Neither results nor no-results appeared — treating as NO RESULTS.")
        return False

    # Final classification
    if driver.find_elements(By.ID, no_results_id):
        print("No results found.")
        return False

    if driver.find_elements(By.CSS_SELECTOR, wait_selectors):
        print("Results found.")
        return True

    print("⚠ Unexpected state — treating as NO RESULTS.")
    return False
        

def scroll_and_load_all(driver, scroll_container_id=None, max_scrolls=100, wait_time=15, 
                        no_change_threshold=3, poll_frequency=0.5, verbose=False):
    """
    Scroll until all content is loaded with smart waiting for content changes.
    Uses JavaScript to avoid stale element references.
    
    Args:
        driver: Selenium WebDriver instance
        scroll_container_id: ID of scrollable container (None = scroll entire page)
        max_scrolls: Maximum number of scroll attempts
        wait_time: Maximum seconds to wait for content to change after each scroll
        no_change_threshold: Number of scrolls with no change before stopping
        poll_frequency: How often to check for changes (in seconds). Default 0.5s.
    
    Returns:
        Number of times content changed
    """
    # Determine what to scroll
    scroll_entire_page = False
    
    if scroll_container_id:
        # Check if container exists using JavaScript
        exists = driver.execute_script(
            f"return document.getElementById('{scroll_container_id}') !== null"
        )
        
        if exists:
            scroll_entire_page = False
            if verbose:
                print(f"✓ Found scroll container: {scroll_container_id}")
            
            # Check if element is scrollable using JavaScript
            scroll_info = driver.execute_script(f"""
                var elem = document.getElementById('{scroll_container_id}');
                return {{
                    scrollHeight: elem.scrollHeight,
                    clientHeight: elem.clientHeight,
                    scrollTop: elem.scrollTop
                }};
            """)
            
            if verbose:
                print(f"DEBUG: scrollHeight={scroll_info['scrollHeight']}, clientHeight={scroll_info['clientHeight']}")
                print(f"DEBUG: Is scrollable? {scroll_info['scrollHeight'] > scroll_info['clientHeight']}")
        else:
            print(f"✗ Could not find container '{scroll_container_id}'")
            print("Falling back to scrolling entire page")
            scroll_container_id = None
            scroll_entire_page = True
    else:
        scroll_entire_page = True
        print("Scrolling entire page (no container specified)")
    
    # Custom expected condition using JavaScript - no element references!
    class content_has_changed:
        def __init__(self, container_id, is_page, initial_length):
            self.container_id = container_id
            self.is_page = is_page
            self.initial_length = initial_length
            self.checks = 0
        
        def __call__(self, driver):
            self.checks += 1
            try:
                if self.is_page:
                    current_length = len(driver.page_source)
                else:
                    # Use JavaScript to get innerHTML length directly - no element reference needed
                    current_length = driver.execute_script(
                        f"return document.getElementById('{self.container_id}').innerHTML.length"
                    )
                
                if current_length > self.initial_length:
                    print(f"  ✓ Content changed after {self.checks} checks ({poll_frequency}s intervals)")
                    return True
                return False
            except Exception as e:
                # Element might be temporarily unavailable
                return False
    
    no_change_count = 0
    content_changes = 0
    scroll_count = 0
    
    # Get initial content length using JavaScript
    if scroll_entire_page:
        previous_content_length = len(driver.page_source)
    else:
        previous_content_length = driver.execute_script(
            f"return document.getElementById('{scroll_container_id}').innerHTML.length"
        )
    
    if verbose:
        print(f"Initial content length: {previous_content_length} characters")
        print(f"Starting scroll loop (checking every {poll_frequency}s, max wait {wait_time}s per scroll)\n")
    
    while scroll_count < max_scrolls:
        # Get scroll position before using JavaScript
        if not scroll_entire_page:
            scroll_info_before = driver.execute_script(f"""
                var elem = document.getElementById('{scroll_container_id}');
                return {{
                    scrollTop: elem.scrollTop,
                    scrollHeight: elem.scrollHeight
                }};
            """)
            print(f"Scroll {scroll_count + 1}: scrollTop={scroll_info_before['scrollTop']}, scrollHeight={scroll_info_before['scrollHeight']}")
        
        # Scroll down using JavaScript
        if scroll_entire_page:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        else:
            driver.execute_script(f"""
                var elem = document.getElementById('{scroll_container_id}');
                elem.scrollTop = elem.scrollHeight;
            """)
        
        # Wait for content to change with custom polling interval
        try:
            print(f"  Waiting for content (max {wait_time}s, polling every {poll_frequency}s)...")
            WebDriverWait(driver, wait_time, poll_frequency=poll_frequency).until(
                content_has_changed(scroll_container_id, scroll_entire_page, previous_content_length)
            )
            
            # Content changed - get new length using JavaScript
            if scroll_entire_page:
                current_content_length = len(driver.page_source)
            else:
                current_content_length = driver.execute_script(
                    f"return document.getElementById('{scroll_container_id}').innerHTML.length"
                )
            
            content_changes += 1
            change_amount = current_content_length - previous_content_length
            print(f"  ✓ Content loaded: {previous_content_length:,} → {current_content_length:,} chars (+{change_amount:,})")
            previous_content_length = current_content_length
            no_change_count = 0
            
        except TimeoutException:
            # No content change detected within wait_time
            no_change_count += 1
            print(f"  ✗ No change detected within {wait_time}s (attempt {no_change_count}/{no_change_threshold})")
        
        # Get scroll position after using JavaScript
        if not scroll_entire_page:
            try:
                scroll_info_after = driver.execute_script(f"""
                    var elem = document.getElementById('{scroll_container_id}');
                    return {{
                        scrollTop: elem.scrollTop,
                        scrollHeight: elem.scrollHeight
                    }};
                """)
                if verbose:
                    print(f"  After: scrollTop={scroll_info_after['scrollTop']}, scrollHeight={scroll_info_after['scrollHeight']}")
                    print(f"  Position changed: {scroll_info_before['scrollTop'] != scroll_info_after['scrollTop']}, Height changed: {scroll_info_before['scrollHeight'] != scroll_info_after['scrollHeight']}\n")
            except Exception as e:
                print(f"  (Could not get post-scroll position: {e})\n")
        
        if no_change_count >= no_change_threshold:
            print(f"\n{'='*60}")
            print(f"Finished loading after {scroll_count + 1} scrolls")
            print(f"Content changed {content_changes} times")
            print(f"Final content length: {previous_content_length:,} characters")
            print(f"{'='*60}")
            break
        
        scroll_count += 1
    
    if scroll_count >= max_scrolls:
        print(f"\n⚠ Reached maximum scroll limit ({max_scrolls})")
        print(f"Content changed {content_changes} times")
    
    return content_changes


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