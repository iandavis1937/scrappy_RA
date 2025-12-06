

import time
from typing import Callable, List, Dict

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import polars as pl

from scrappy_RA.utils import gen_utils
from scrappy_RA.scrapers.scraper import Scraper

# UMICH_DF_SCHEMA = {
#     'title': pl.Utf8,
#     'department': pl.Utf8,
#     'location': pl.Utf8,
#     'posting_date': pl.Utf8, # Assuming you convert this to Polars Date type
#     'employment_type': pl.Utf8,
#     'url': pl.Utf8,
#     'job_id': pl.Utf8
#     }


class UMichScraper(Scraper):
    # Set UMich-specific defaults
    def __init__(self, **kwargs):
        
        # Store HigherEd-specific attributes
        # self.search_kw = search_kw
        
        super().__init__(
            base_url="https://careers.umich.edu/search-jobs?career_interest=All&work_location=All&field_job_modes_of_work_target_id=All&position=All&regular_temporary=All&job_id=&department=&title=&keyword=",
            kw_param_nm="keyword",  # UMich-specific param name
            out_cols=['title', 'department', 'location', 'posting_date', 'employment_type', 'url', 'job_id'],
            out_df_schema={
                'title': pl.Utf8,
                'department': pl.Utf8,
                'location': pl.Utf8,
                'posting_date': pl.Utf8, # Assuming you convert this to Polars Date type
                'employment_type': pl.Utf8,
                'url': pl.Utf8,
                'job_id': pl.Utf8
            },
            **kwargs
        )
    
    def get_soup(self, url: str, headers=None) -> BeautifulSoup:
        """Fetch page using requests-based soup fetcher."""
        if not headers:    
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
            }
        
        return gen_utils.get_soup_requests(url, headers)
        
    def parse_page(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse a single UMich job listings page into a list of job dicts."""

        job_table = soup.find('table', class_='cols-5')
        if not job_table:
            return []

        tbody = job_table.find('tbody')
        if not tbody:
            return []

        job_rows = tbody.find_all('tr')
        if not job_rows:
            return []

        print(f"{len(job_rows)} results", end = "    ")
        
        jobs_on_page = []

        for row in job_rows:
            try:
                job_data = {
                    'title': '',
                    'url': '',
                    'job_id': '',
                    'department': '',
                    'location': '',
                    'posting_date': '',
                    'employment_type': ''
                }

                cells = row.find_all('td')
                if len(cells) < 5:
                    print(f"Warning: Row has fewer than 5 cells, skipping")
                    continue

                # Posting date (first column)
                try:
                    date_elem = cells[0].find('time')
                    if date_elem:
                        job_data['posting_date'] = date_elem.get_text(strip=True) or ''
                except Exception as e:
                    print(f"Warning: Could not extract posting date - {e}")

                # Title + link (second column)
                try:
                    title_elem = cells[1].find('a')
                    if title_elem:
                        job_data['title'] = title_elem.get_text(strip=True) or ''
                        href = title_elem.get('href', '')
                        if href:
                            job_data['url'] = urljoin('https://careers.umich.edu', href)
                except Exception as e:
                    print(f"Warning: Could not extract title - {e}")

                # Job ID (third column)
                try:
                    job_data['job_id'] = cells[2].get_text(strip=True) or ''
                except Exception as e:
                    print(f"Warning: Could not extract job ID - {e}")

                # Department (fourth column)
                try:
                    job_data['department'] = cells[3].get_text(strip=True) or ''
                except Exception as e:
                    print(f"Warning: Could not extract department - {e}")

                # Location (fifth column)
                try:
                    job_data['location'] = cells[4].get_text(strip=True) or ''
                except Exception as e:
                    print(f"Warning: Could not extract location - {e}")
                    
                # Only add job if at least title or job_id exists
                if job_data['title'] or job_data['job_id']:
                    jobs_on_page.append(job_data)
                else:
                    print("Warning: Skipping job row with no title or job ID")

            except Exception:
                continue

        return jobs_on_page
    

def search_umich(search_kw, exclusion_role_kw, output_file):
    all_jobs_df_list = []

    # Loop through the list of keywords (index corresponding to relevance)
    i = 1
    for kw in search_kw:
        print(f"Search #{i}, kw: {kw}...")
        umich_scraper = UMichScraper(search_kw=[kw])
        jobs_df_i = umich_scraper.scrape()
        
        if jobs_df_i is not None and not jobs_df_i.is_empty():
            jobs_df_i = jobs_df_i.with_columns(
                pl.lit(kw).alias('kw'),
                pl.lit(str(i)).alias('kw_idx')
            )
            
            all_jobs_df_list.append(jobs_df_i)
            i += 1

    all_jobs = pl.concat(all_jobs_df_list)
    
    # Concatenate kw_idx values for duplicate job_ids, then keep first of other columns
    all_jobs = all_jobs.group_by('job_id', maintain_order=True).agg([
        pl.col('kw_idx').str.concat(delimiter=', ').alias('kw_idx'),
        pl.col('kw').str.concat(delimiter=' + ').alias('kw'),
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
    
    try:
        jobs = jobs.sort(['kw_idx', 'department'])  # Sort by department (alphabetical)
        jobs.write_csv(output_file)
        print(f"✓ Successfully saved {len(jobs)} job listings!")
    except Exception as e:
        print(f"✗ Error saving to CSV: {e}")
        
    # Display sample
    # print("\nSample of first 3 jobs:")
    # print(jobs.head(3))
    
    return jobs
