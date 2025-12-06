

import polars as pl
# Explicit relative import
from . import berkeley_scraper

def run_berkeley_module(search_jobs_page=True):
    print('Running UC Berkeley scraper...')
    
    EXLCUSION_ROLE_KW=[
        'president', 'director', 'senior', 'principal', 'professor', 'faculty', 'postdoc',
        # Health/Medicine
        'medicine',
        'anesthesiolog', 'cancer', 'cardiolog', 'dermatolog','endocrinolog',
        'gastroenterolog', 'geriatric', 'gynecolog', 'hematolog',
        'nephrolog', 'neurolog', 'nursing',
        'obstetric', 'oncolog', 'ophthalmolog', 'orthopedic', 'otolaryngolog',
        'patholog', 'pediatric', 'physiolog', #'rehabilitat',
        'psychiatr', 'pulmonolog', 'radiolog', 'rheumatolog', 'surgery', 'urolog',
        # More Natural Sciences
        'animal', 'biolog', 'biochem',
        'chemist', 'physics', 'atronom'
        ]

    # Get listings (mostly in-person)
    if search_jobs_page:
        BASE_URL = 'https://careerspub.universityofcalifornia.edu/psc/ucb/EMPLOYEE/HRMS/c/HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL?Page=HRS_APP_SCHJOB_FL&Action=U'
        OUTPUT_FILE='./scrappy_RA/data_saved_locally/berkeley/berkeley_lab_jobs.csv'
        SEARCH_KW = [
                'RStudio', 'tidyverse', ' R ',
                'Stata', 'STATA', 'regression', 'econometric', 'Qualtrics',
                'remote', 'work from home', 'work-from-home',
                'survey research', 'data science', 'data scientist', 'quantitative', 'economic',
                'Python', 'statistic', 'SQL',
                'analysis', 'data',
                'tutor', 'assistant'
            ]
        FETCH_JOB_DESC_FLAG = False
        
        jobs = berkeley_scraper.search_berkeley(
            SEARCH_KW, OUTPUT_FILE, EXLCUSION_ROLE_KW
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