

import time
import random
from typing import Callable, List, Dict, Union

from urllib.parse import urlencode, urlparse, urlunparse, parse_qs, urljoin
import requests
from bs4 import BeautifulSoup
import polars as pl

class Scraper:
    def __init__(
        self,
        base_url: str,
        search_kw: Union[List[str], Dict] = None,  # Accept either type
        kw_join: str=" OR ",
        kw_param_nm: str="keyword",
        sleep_time: list = [1.5, 3],
        out_cols: List[str] = ['title'],
        out_df_schema: dict={'title': pl.Utf8}
        ):
        
        self.base_url = base_url
        self.search_kw = search_kw
        self.kw_join = kw_join
        self.kw_param_nm = kw_param_nm
        self.sleep_time = sleep_time
        self.jobs_from_search: List[Dict] = []
        self.out_cols = out_cols
        self.out_df_schema = out_df_schema

    
    def build_search_url(self) -> str:
        """Build search URL by adding keywords to existing query parameters."""
        search_string = self.kw_join.join(self.search_kw)

        # Parse base URL
        parsed = urlparse(self.base_url)

        # Parse existing query string into a dictionary
        existing_params = parse_qs(parsed.query)

        # Add the new keyword parameter
        existing_params[self.kw_param_nm] = [search_string]

        # Re-encode the query
        combined_query_string = urlencode(existing_params, doseq=True)

        # Reconstruct the full URL
        url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            combined_query_string,
            parsed.fragment
        ))

        return url
    
    def get_soup(self, url: str) -> BeautifulSoup:
        """
        Fetch a page and return BeautifulSoup object using either requests or selenium.
        To be implemented in subclasses.
        """
        raise NotImplementedError("Subclasses must implement get_soup")

    def parse_page(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Site-specific page parsing. To be implemented in subclasses.
        """
        raise NotImplementedError("Subclasses must implement parse_page")

    def scrape(self) -> pl.DataFrame:
        """Main scraping loop with pagination."""
        page = 0

        while True:
            url = self.build_search_url()
            if not page == 0:
                url = f"{url}&page={page}"
            print(f"Fetching page {page + 1}", end=" ")
            
            soup = self.get_soup(url)

            jobs_on_page = self.parse_page(soup)
            if not jobs_on_page:
                print("No jobs found. Stopping.")
                break
            
            self.jobs_from_search.extend(jobs_on_page)

            # Pagination
            if not self.has_next_page(soup):
                print("Reached last page.")
                break

            page += 1
            time.sleep(random.uniform(self.sleep_time[0], self.sleep_time[1]))
            
        return self.to_dataframe()

    def has_next_page(self, soup: BeautifulSoup) -> bool:
        """Determine if there's a next page. Can be overridden."""
        # Check if there's a next page button
        try:
            next_button = soup.find('a', {'rel': 'next'})
        except Exception as e:
            print(f"\nNo next page button found. {e}")
            return False

        if not next_button:
            print("Reached last page.")
            return False
        
        else:
            return True

    def to_dataframe(self) -> pl.DataFrame:
        """Convert collected jobs to Polars DataFrame."""
        print(f"\nConverting {len(self.jobs_from_search)} jobs to Polars DataFrame...")
        if not self.jobs_from_search:
            df = pl.DataFrame(schema=self.out_df_schema)
        
        else:
            df = pl.DataFrame(self.jobs_from_search)
            # Reorder columns for consistency -- Only select columns that exist in the dataframe
            existing_cols = [col for col in self.out_cols if col in df.columns]
            df = df.select(existing_cols)
            print(f"✓ Successful -- DataFrame shape: {df.shape}")

        return df
    

# scraper = UMichScraper(base_url="https://careers.umich.edu/search", search_kw="research")
# df = scraper.scrape()
