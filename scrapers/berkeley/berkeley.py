

import os
from pathlib import Path
import yaml
import polars as pl

from . import berkeley_scraper

BASE_DIR = Path(__file__).resolve().parent     # __file__ is a built-in variable, points to your .py file


def run_berkeley_module(search_jobs_page=True):
    print('Running UC Berkeley scraper...')

    # Get listings (mostly in-person)
    if search_jobs_page:
        BASE_URL = 'https://careerspub.universityofcalifornia.edu/psc/ucb/EMPLOYEE/HRMS/c/HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL?Page=HRS_APP_SCHJOB_FL&Action=U'
        OUTPUT_FILE='./scrappy_RA/data_saved_locally/berkeley/berkeley_lab_jobs.csv'
        
        # Get keywords from profile YAML
        yaml_path = BASE_DIR / ".." / ".." / "profiles" / "profile1.yaml"
        print(f"Profiles available: {os.listdir(yaml_path.parent)}")
        with open(yaml_path, "r") as f:
            profile = yaml.safe_load(f)
        EXCLUSION_ROLE_KW = profile['EXCLUSION_ROLE_KW']
        SEARCH_KW = profile['SEARCH_KW_INDIVIDUAL']
        
        jobs = berkeley_scraper.search_berkeley(
            SEARCH_KW, OUTPUT_FILE, EXCLUSION_ROLE_KW
            )
     
    jobs.write_csv('./scrappy_RA/data_to_unify/berkeley_jobs.csv')
    
    return jobs


# It's good practice to have a main execution block here too
if __name__ == '__main__':
    # This only runs if berkeley.py is executed directly, not when imported
    print('berkeley.py running directly. Define test parameters here.')
    
    # EXAMPLE USAGE:
    # BASE_URL = 'https://example.com/search'
    # SEARCH_KW = ['data science', 'analysis']
    # OUTPUT_FILE = 'test_jobs.csv'
    # EXCLUSION_ROLE_KW = ['Student']
    # run_berkeley_module(BASE_URL, SEARCH_KW, OUTPUT_FILE, EXCLUSION_ROLE_KW)
    pass