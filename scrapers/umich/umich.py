

import os
from pathlib import Path
import yaml
import polars as pl

from .umich_scraper import search_umich

BASE_DIR = Path(__file__).resolve().parent     # __file__ is a built-in variable, points to your .py file


def run_umich_module():

    # UMICH_DF_SCHEMA = {
    #     'title': pl.Utf8,
    #     'department': pl.Utf8,
    #     'location': pl.Utf8,
    #     'posting_date': pl.Utf8, # Assuming you convert this to Polars Date type
    #     'employment_type': pl.Utf8,
    #     'url': pl.Utf8,
    #     'job_id': pl.Utf8
    #     }

    # Get keywords from profile YAML
    yaml_path = BASE_DIR / ".." / ".." / "profiles" / "profile1.yaml"
    print(f"Profiles available: {os.listdir(yaml_path.parent)}")
    with open(yaml_path, "r") as f:
        profile = yaml.safe_load(f)
    EXCLUSION_ROLE_KW = profile['EXCLUSION_ROLE_KW']
    SEARCH_KW = profile['SEARCH_KW_INDIVIDUAL']
    OUTPUT_FILE='./scrappy_RA/data_to_unify/umich_jobs.csv'

    umich_jobs = search_umich(SEARCH_KW, EXCLUSION_ROLE_KW, OUTPUT_FILE)

    # Save a backup copy as well
    umich_jobs.write_csv("umich_jobs.csv")
    
    return umich_jobs