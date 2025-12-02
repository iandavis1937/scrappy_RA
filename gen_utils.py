

import time
from typing import Callable
# from urllib.parse import urlencode, urlparse, urlunparse, parse_qs, urljoin
import requests
from bs4 import BeautifulSoup
import polars as pl

'''
def build_search_url(base_url, search_kw, kw_param_nm):
    """Build search URL by ADDING a new query to existing ones."""
    search_string = " OR ".join(search_kw)
    
    # 1. Parse base URL
    parsed = urlparse(base_url)
    
    # 2. Parse existing query string into a dictionary
    existing_params = parse_qs(parsed.query)
    
    # 3. Add the new filterKeywords to the existing dictionary
    # Note: parse_qs returns lists, so we use list for consistency, though urlencode handles it
    existing_params[kw_param_nm] = [search_string]
    
    # 4. Re-encode the combined parameters
    combined_query_string = urlencode(existing_params, doseq=True)
    
    # 5. Reconstruct URL with the COMBINED query
    url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        combined_query_string, # Use the combined query string
        parsed.fragment
    ))
    
    return url
'''

def get_soup_requests(url, headers=None):
    if not headers:    
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Request failed: {e}")

    soup = BeautifulSoup(response.content, "html.parser")
    
    return soup


def scrape_requests(
    base_url: str,
    search_kw: str,
    kw_param_nm: str,
    parser: Callable[[BeautifulSoup, str], list[dict]],
    out_cols: list=['title', 'department', 'location', 'posting_date', 'employment_type', 'url', 'job_id'],
    out_df_schema: dict={'title': pl.Utf8}
):
    """
    Generic scraper for paginated job listings using requests + BeautifulSoup.
    The parser function must accept (soup, base_url) and return a list[dict].
    """

    all_jobs = []
    page = 0
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }

    print("Starting job listing scrape...")

    while True:
        if page == 0:
            url = build_search_url(base_url, search_kw, kw_param_nm)
        else:
            url = f"{build_search_url(base_url, search_kw, kw_param_nm)}&page={page}"

        print(f"\nFetching page {page + 1}: {url}")
        soup = get_soup_requests(url, headers)
        if not soup:
            print("Failed to fetch page.")
            break

        # --------- SITE-SPECIFIC PARSER ---------
        jobs_on_page = parser(soup, base_url)
        # ----------------------------------------

        if not jobs_on_page:
            print("No more jobs found on this page.")
            break

        print(f"Parsed {len(jobs_on_page)} jobs from this page.")
        all_jobs.extend(jobs_on_page)

        # Pagination
        next_button = soup.find('a', {'rel': 'next'})
        if not next_button:
            print("Reached last page.")
            break

        page += 1
        time.sleep(1)
        
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
        empty_df = pl.DataFrame(
            data=[],
            schema=out_df_schema
        )
        return empty_df



