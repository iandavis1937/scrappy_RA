

import time
import re
import random
from typing import Callable, List, Dict
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import polars as pl
from scrappy_RA.scrapers.scraper import Scraper

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from scrappy_RA.utils import selenium_utils


class IdealistScraper(Scraper):
    # Set Idealist-specific defaults
    def __init__(
        self,
        driver = None,
        wait_selectors: str="span.ps_box-value[id^='HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID']", #"li.ps_grid-row[id^='HRS_AGNT_RSLT_I']",
        wait_time: int=10,
        enable_scroll: bool=True,
        scroll_container_id: str="win0divHRS_AGNT_RSLT_I$grid$0",
        enable_search_box=True,
        search_box_id: str='HRS_SCH_WRK_HRS_SCH_TEXT100',
        search_button_id: str='HRS_SCH_WRK_FLU_HRS_SEARCH_BTN',
        no_results_id: str='win0divHRS_SCH_WRK_HRS_CC_NO_RSLT',
        save_debug_html = True,
        **kwargs
    ):
        # Store Idealist-specific attributes
        self.driver = driver
        self.wait_selectors = wait_selectors
        self.wait_time = wait_time
        self.enable_scroll = enable_scroll
        self.scroll_container_id = scroll_container_id
        self.enable_search_box = enable_search_box
        self.search_box_id = search_box_id
        self.search_button_id = search_button_id
        self.no_results_id = no_results_id
        self.save_debug_html = save_debug_html
                
        # Call parent with only its recognized parameters
        super().__init__(
            base_url="https://www.idealist.org/en/jobs", 
            kw_param_nm="",
            out_cols=['title', 'department', 'location', 'url', 'posted_date', 'job_id'],
            **kwargs
        )
    
    '''
    def _ensure_driver(self):
        """Initialize driver if not already created."""
        if self.driver is None:
            self.driver = selenium_utils.setup_driver()
            
    def cleanup(self):
        """Close the browser driver."""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                print("✓ Browser closed")
            except Exception as e:
                print(f"Warning: Error closing driver - {e}")
                
    def __enter__(self):
        """Context manager entry."""
        self._ensure_driver()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto cleanup."""
        self.cleanup()
        return False
    '''
    
    def get_soup(self, url: str) -> BeautifulSoup:
        """Fetch page using selenium-based soup fetcher."""
        # self._ensure_driver()
        
        soup = selenium_utils.get_soup_selenium(
            driver=self.driver,
            url=url,
            wait_selectors=self.wait_selectors,
            wait_time=self.wait_time,
            sleep_time=self.sleep_time,
            enable_scroll=self.enable_scroll,
            scroll_container_id=self.scroll_container_id,
            enable_search_box=self.enable_search_box,
            search_box_id=self.search_box_id,
            search_button_id=self.search_button_id,
            search_kw=self.search_kw[0],
            no_results_id=self.no_results_id,
            save_debug_html=self.save_debug_html
            )
        return soup
        
    def parse_page(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Extract all job listings from Idealist.
        """
        # Find all job rows in the grid
        job_records = soup.find_all('li', class_='ps_grid-row', id=re.compile(r'^HRS_AGNT_RSLT_I\$\d+_row_\d+$'))
        
        if not job_records:
            print("No more job listings found.")
            return []
            
        print(f"Found {len(job_records)} jobs on this page")
        
        jobs_on_page = []
        for record in job_records:
            try:
                job_data = {
                    'title': '',
                    'job_id': '',
                    'location': '',
                    'department': '',
                    'posted_date': '',
                    'url': ''
                }
                
                # Extract Job Title
                title_elem = record.find('span', id=re.compile(r'^SCH_JOB_TITLE\$\d+$'))
                if title_elem:
                    full_title = title_elem.get_text(strip=True)
                    job_data['title'] = full_title
                    
                    # Some titles have the job ID embedded (e.g., "Title - Davis, CA, Job ID 82838")
                    # Extract and remove it from title if present
                    job_id_match = re.search(r',\s*Job ID\s+(\d+)$', full_title)
                    if job_id_match:
                        job_data['job_id'] = job_id_match.group(1)
                        # Clean the title to remove the job ID suffix
                        job_data['title'] = re.sub(r',\s*Job ID\s+\d+$', '', full_title).strip()
                
                else:
                    print(record)
                
                # Extract Job ID (from dedicated field)
                job_id_elem = record.find('span', id=re.compile(r'^HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID\$\d+$'))
                if job_id_elem:
                    job_data['job_id'] = job_id_elem.get_text(strip=True) or job_data['job_id']
                
                # Extract Location
                location_elem = record.find('span', id=re.compile(r'^LOCATION\$\d+$'))
                if location_elem:
                    job_data['location'] = location_elem.get_text(strip=True) or ''
                
                # Extract Department
                dept_elem = record.find('span', id=re.compile(r'^HRS_APP_JBSCH_I_HRS_DEPT_DESCR\$\d+$'))
                if dept_elem:
                    job_data['department'] = dept_elem.get_text(strip=True) or ''
                
                # Extract Posted Date
                date_elem = record.find('span', id=re.compile(r'^SCH_OPENED\$\d+$'))
                if date_elem:
                    job_data['posted_date'] = date_elem.get_text(strip=True) or ''
                
                # Extract URL from onclick attribute
                onclick = record.get('onclick', '')
                if onclick:
                    # The row is clickable, but we need to construct the actual job detail URL
                    # This will depend on the base URL structure
                    # For now, we'll store the job ID which can be used to construct URLs later
                    if job_data['job_id']:
                        # Example URL construction (you'll need to adjust based on actual URL pattern)
                        job_data['url'] = f"{self.base_url}?JobCode={job_data['job_id']}"
                
                # Only add job if at least title or job_id exists
                if job_data['title'] or job_data['job_id']:
                    jobs_on_page.append(job_data)
                else:
                    print("Warning: Skipping job record with no title or job ID")
                    
            except Exception as e:
                print(f"Error processing job record: {e}")
                continue
                                        
        return jobs_on_page
        
        
def search_idealist(search_kw, output_file, exclusion_role_kw):
    all_jobs_df_list = []
    driver = selenium_utils.setup_driver()
            
    # Loop through the list of keywords (index corresponding to relevance)
    i = 1
    for kw in search_kw:
        print(f"\nSearch #{i}, kw: {kw}...")
        idealist_scraper = IdealistScraper(search_kw=[kw], driver=driver)
        jobs_df_i = idealist_scraper.scrape()
        
        if jobs_df_i is not None and not jobs_df_i.is_empty():
            jobs_df_i = jobs_df_i.with_columns(
                pl.lit(kw).alias('kw'),
                pl.lit(str(i)).alias('kw_idx')
            )
            
            all_jobs_df_list.append(jobs_df_i)
            i += 1

    all_jobs = pl.concat(all_jobs_df_list)
    
    if driver:
        try:
            driver.quit()
            driver = None
            print("✓ Browser closed")
        except Exception as e:
            print(f"Warning: Error closing driver - {e}")

    # Concatenate kw_idx values for duplicate job_ids, then keep first of other columns
    all_jobs = all_jobs.group_by('job_id', maintain_order=True).agg([
        pl.col('kw_idx').str.concat(delimiter=', ').alias('kw_idx'),
        pl.col('kw').flatten().str.concat(delimiter=' + ').alias('kw'),
        pl.all().exclude(['kw_idx', 'kw']).first()
    ])
    
    print(f"\n{'='*50}")
    print(f"Total jobs scraped (w/o dups): {len(all_jobs)}")
    print(f"{'='*50}")
    
    # Exclude rows where title contains any negative keyword
    jobs = all_jobs
    for keyword in exclusion_role_kw:
        jobs = jobs.filter(~pl.col('title').str.to_lowercase().str.contains(keyword.lower())) 
    
    print(f"Total jobs after filtering: {len(jobs)}")
    
    # Save to CSV
    try:
        jobs = jobs.sort('kw_idx')  # Sort by kw_idx (ascending by default)
        jobs.write_csv(output_file)
        print(f"✓ Successfully saved {len(jobs)} job listings!")
    except Exception as e:
        print(f"✗ Error saving to CSV: {e}")
        
    '''
    # Check for skills keywords
    if fetch_job_desc_flag:
        print("Checking for skills keywords in job descriptions...")
        jobs = fetch_job_descriptions(jobs)  # Fetch all job descriptions
        jobs = add_keyword_matches(jobs, keywords=skills_kw)  # Search for keywords in descriptions
    '''
        
    # # Display sample
    # print("\nSample of first 3 jobs:")
    # print(jobs.head(3))
    
    return jobs
    