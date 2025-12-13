

import polars as pl
# Explicit relative import
from . import idealist_scraper

def run_idealist_module(search_jobs_page=True):
    print('Running idealist scraper...')
    
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
        BASE_URL = 'https://www.idealist.org/en/jobs'
        OUTPUT_FILE='./scrappy_RA/data_saved_locally/idealist/idealist_lab_jobs.csv'
        SEARCH_KW = [
                'RStudio', 'tidyverse', ' R language', ' R programming', ' R statistic',
                'Stata', 'STATA', 'regression', 'econometric', 'Qualtrics',
                # 'remote',  seems to be related to bug, may just be its order in the sequence 
                'work from home', 'work-from-home',
                'survey research', 'data science', 'data scientist', 'quantitative', 'economic',
                'Python', 'statistic', 'SQL',
                'analysis', 'data',
                'tutor', 'assistant'
            ]
        FETCH_JOB_DESC_FLAG = False
        
        jobs = idealist_scraper.search_idealist(
            SEARCH_KW, OUTPUT_FILE, EXLCUSION_ROLE_KW
            )
     
    jobs.write_csv('./scrappy_RA/data_to_unify/idealist_jobs.csv')
    
    return jobs


# It's good practice to have a main execution block here too
if __name__ == '__main__':
    # This only runs if idealist.py is executed directly, not when imported
    print('idealist.py running directly. Define test parameters here.')
    
    # EXAMPLE USAGE:
    # BASE_URL = 'https://example.com/search'
    # SEARCH_KW = ['data science', 'analysis']
    # OUTPUT_FILE = 'test_jobs.csv'
    # EXCLUSION_ROLE_KW = ['Student']
    run_idealist_module()
    pass