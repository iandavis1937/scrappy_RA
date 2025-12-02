

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


class HigherEdScraper(Scraper):
    # Set HigherEd-specific defaults
    def __init__(
        self,
        driver = None, 
        wait_selectors: str = ".record, .row.record, div[class*='record']",
        wait_time: int=30,
        save_debug_html = False,
        **kwargs
    ):
        # Store HigherEd-specific attributes
        self.driver = driver
        self.wait_selectors = wait_selectors
        self.wait_time = wait_time
        self.save_debug_html = save_debug_html
                
        # Call parent with only its recognized parameters
        super().__init__(
            kw_param_nm="keywordFilter",
            out_cols=['title', 'organization', 'location', 'url', 'salary', 'category', 'posted_date', 'priority', 'job_code'],
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
            self.driver,
            url,
            self.wait_selectors,
            self.wait_time,
            self.sleep_time,
            self.save_debug_html
            )
        return soup
        
    def parse_page(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Extract all job listings from a page soup.
        """
        # Find all job records in the result page
        job_records = soup.find_all('div', class_='row record')
        
        if not job_records:
            print("No more job listings found.")
            return []  # Add explicit return
            
        print(f"Found {len(job_records)} jobs on this page")
        
        jobs_on_page = []
        for record in job_records:
            try:
                job_data = {
                    'title': '',
                    'url': '',
                    'job_code': '',
                    'organization': '',
                    'location': '',
                    'salary': '',
                    'category': '',
                    'posted_date': '',
                    'priority': ''
                }
                
                # Find the left column (col-sm-7) and right column (col-sm-5)
                left_col = record.find('div', class_='col-sm-7')
                right_col = record.find('div', class_='col-sm-5')
                
                # Extract job title and URL from left column
                if left_col:
                    try:
                        title_link = left_col.find('a')
                        if title_link:
                            job_data['title'] = title_link.get_text(strip=True) or ''
                            href = title_link.get('href', '')
                            if href:
                                # Extract job code from URL (JobCode parameter)
                                job_code_match = re.search(r'JobCode=(\d+)', href)
                                if job_code_match:
                                    job_data['job_code'] = job_code_match.group(1)
                                job_data['url'] = urljoin(self.base_url, href)
                    except Exception as e:
                        print(f"Warning: Could not extract title/URL - {e}")
                    
                    # Extract organization, location, and salary from left column
                    try:
                        text_parts = []
                        for content in left_col.children:
                            if content.name == 'br':
                                continue
                            text = str(content).strip() if isinstance(content, str) else content.get_text(strip=True)
                            if text and text not in ['<br>', '<br/>']:
                                text_parts.append(text)
                        
                        # Remove the title (first item) since we already have it
                        if text_parts:
                            text_parts = text_parts[1:]
                        
                        # Typically: [organization, location, salary]
                        if len(text_parts) >= 1:
                            job_data['organization'] = text_parts[0]
                        if len(text_parts) >= 2:
                            job_data['location'] = text_parts[1]
                        
                        # Extract salary if present (has class 'job-salary')
                        salary_elem = left_col.find('span', class_='job-salary')
                        if salary_elem:
                            job_data['salary'] = salary_elem.get_text(strip=True) or ''
                            
                    except Exception as e:
                        print(f"Warning: Could not extract organization/location/salary - {e}")
                
                # Extract category and posted date from right column
                if right_col:
                    try:
                        text_content = right_col.get_text(separator='|', strip=True)
                        parts = [p.strip() for p in text_content.split('|') if p.strip()]
                        
                        # Typically: [category, "Posted MM/DD/YYYY"]
                        if len(parts) >= 1:
                            job_data['category'] = parts[0]
                        
                        # Extract posted date
                        for part in parts:
                            if 'Posted' in part:
                                job_data['posted_date'] = part.replace('Posted', '').strip()
                                break
                    except Exception as e:
                        print(f"Warning: Could not extract category/date - {e}")
                
                # Check if job is marked as priority
                try:
                    priority_marker = record.find('span', class_='addon-marker')
                    if priority_marker and 'Priority' in priority_marker.get_text():
                        job_data['priority'] = 'Yes'
                    else:
                        job_data['priority'] = 'No'
                except Exception as e:
                    print(f"Warning: Could not check priority status - {e}")
                
                # Only add job if at least title or job_code exists
                if job_data['title'] or job_data['job_code']:
                    jobs_on_page.append(job_data)
                else:
                    print("Warning: Skipping job record with no title or job code")
                    
            except Exception as e:
                print(f"Error processing job record: {e}")
                continue
                                        
        return jobs_on_page
        
        
def search_higher_ed_category(base_url, search_kw, output_file, exclusion_role_kw):
    all_jobs_df_list = []
    driver = selenium_utils.setup_driver()

    # Loop through the dictionary items (kw_idx, keywords)
    for kw_idx, keywords in search_kw.items():
        print(f"Search #{kw_idx}...")
        
        higher_ed_scraper = HigherEdScraper(base_url=base_url, search_kw=keywords, driver=driver)
        jobs_df_i = higher_ed_scraper.scrape()
        
        # with HigherEdScraper(base_url=base_url, search_kw=keywords, driver=driver) as higher_ed_scraper:
        #     jobs_df_i = higher_ed_scraper.scrape()
        
        if jobs_df_i is not None and not jobs_df_i.is_empty():
            jobs_df_i = jobs_df_i.with_columns(
                pl.lit(str(kw_idx)).alias('kw_idx'),
                pl.lit(keywords).alias('kw')
            )
            
            all_jobs_df_list.append(jobs_df_i)

    all_jobs = pl.concat(all_jobs_df_list)
    
    if driver:
        try:
            driver.quit()
            driver = None
            print("✓ Browser closed")
        except Exception as e:
            print(f"Warning: Error closing driver - {e}")

    # Concatenate kw_idx values for duplicate job_codes, then keep first of other columns
    all_jobs = all_jobs.group_by('job_code', maintain_order=True).agg([
        pl.col('kw_idx').str.concat(delimiter=', ').alias('kw_idx'),
        pl.col('kw').flatten().str.concat(delimiter=' | ').alias('kw'),
        pl.all().exclude(['kw_idx', 'kw']).first()
    ])
    
    print(f"\n{'='*50}")
    print(f"Total jobs scraped: {len(all_jobs)}")
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
    