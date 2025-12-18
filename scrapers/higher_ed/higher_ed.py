

import os
from pathlib import Path
import yaml
import polars as pl

# Explicit relative import
from . import higher_ed_scraper


# Get the directory where THIS script is located
BASE_DIR = Path(__file__).resolve().parent     # __file__ is a built-in variable, points to your .py file


def run_higher_ed_module(search_remote_jobs_page=True, search_lab_jobs_page=True):
    print('Running HigherEd scraper...')
    remote_jobs = None
    lab_jobs = None
    
    # Get keywords from profile YAML
    yaml_path = BASE_DIR / ".." / ".." / "profiles" / "profile1.yaml"
    print(f"Profiles available: {os.listdir(yaml_path.parent)}")
    with open(yaml_path, "r") as f:
        profile = yaml.safe_load(f)
    EXCLUSION_ROLE_KW = profile['EXCLUSION_ROLE_KW']
    SEARCH_KW = profile['SEARCH_KW_HIGHERED_REMOTE']

    # Get remote jobs
    if search_remote_jobs_page:
        BASE_URL = 'https://www.higheredjobs.com/search/remote.cfm'
        OUTPUT_FILE='./scrappy_RA/data_saved_locally/higher_ed/higher_ed_remote_jobs.csv'
        FETCH_JOB_DESC_FLAG = False
        
        remote_jobs = higher_ed_scraper.search_higher_ed_category(
            BASE_URL, SEARCH_KW, OUTPUT_FILE, EXCLUSION_ROLE_KW
            )
        remote_jobs = remote_jobs.with_columns(
            pl.lit('remote').alias('remote_or_lab'),
            pl.lit('True').alias('remote')
        )
    
    
    # Get lab & research jobs (mostly in-person)
    if search_lab_jobs_page:
        BASE_URL = 'https://www.higheredjobs.com/admin/search.cfm?JobCat=150&CatName=Laboratory%20and%20Research'
        OUTPUT_FILE='./scrappy_RA/data_saved_locally/higher_ed/higher_ed_lab_jobs.csv'
        FETCH_JOB_DESC_FLAG = False
        SEARCH_KW = profile['SEARCH_KW_HIGHERED_LAB']
        
        lab_jobs = higher_ed_scraper.search_higher_ed_category(
            BASE_URL, SEARCH_KW, OUTPUT_FILE, EXCLUSION_ROLE_KW
            )
        lab_jobs = lab_jobs.with_columns(
            pl.lit('lab').alias('remote_or_lab'),
            pl.lit('').alias('remote')
        )
    
    if search_remote_jobs_page and search_lab_jobs_page:
        jobs = pl.concat([remote_jobs, lab_jobs])
        jobs = jobs.sort('kw')
        jobs.write_csv('./scrappy_RA/data_to_unify/higher_ed_jobs.csv')
    
        return jobs
    
    elif remote_jobs is not None:
        return remote_jobs
        
    elif lab_jobs is not None:
        return lab_jobs
    
    else:
        print("No results")
        return None


# It's good practice to have a main execution block here too
if __name__ == '__main__':
    # This only runs if higher_ed.py is executed directly, not when imported
    print('higher_ed.py running directly. Define test parameters here.')
    
    # EXAMPLE USAGE:
    # BASE_URL = 'https://example.com/search'
    # SEARCH_KW = ['data science', 'analysis']
    # OUTPUT_FILE = 'test_jobs.csv'
    # EXCLUSION_ROLE_KW = ['Student']
    # run_higher_ed_module(BASE_URL, SEARCH_KW, OUTPUT_FILE, EXCLUSION_ROLE_KW)
    pass