

import polars as pl
# Explicit relative import
from . import higher_ed_scraper

def run_higher_ed_module(search_remote_jobs_page=True, search_lab_jobs_page=True):
    print('Running HigherEd scraper...')
    
    EXLCUSION_ROLE_KW=[
        'president', 'director', 'senior', 'principal', 'professor', 'faculty', 'postdoc',
        # Health/Medicine
        'medicine', 'medical', 'surgical',
        'anesthesi', 'cancer', 'cardiolog', 'dermatolog','endocrinolog',
        'gastroenterolog', 'geriatric', 'gynecolog', 'hematolog',
        'nephrolog', 'neurolog', 'nursing', 'nurse',
        'obstetric', 'oncolog', 'ophthalmolog', 'orthopedic', 'otolaryngolog',
        'patholog', 'pediatric', 'pharmac', 'physiolog', #'rehabilitat',
        'psychiatr', 'pulmonolog', 'radiolog', 'rheumatolog', 'surgery', 'urolog',
        # More Natural Sciences
        'animal', 'biolog', 'biochem', 'ecolog',
        'chemist', 'physics', 'astronom'
        ]

    # Get remote jobs
    if search_remote_jobs_page:
        BASE_URL = 'https://www.higheredjobs.com/search/remote.cfm'
        OUTPUT_FILE='./scrappy_RA/data_saved_locally/higher_ed/higher_ed_remote_jobs.csv'
        
        SEARCH_KW = {
            1: ['" R language"', '" R programming"', '" R statistic"', 'RStudio', 'Stata', 'STATA', 'regression', 'econometrics', '"statistical software"', '"statistical program"'],
            2: ['"data science"', '"data scientist"', '"survey research"', 'economic', 'quantitative', 'Python', 'SQL', 'Qualtrics', 'statistic'],
            3: ['analysis', 'data'],
            4: ['tutor', 'assistant']
        }
        
        SKILLS_KW=['R', 'RStudio', 'Stata', 'STATA', 'statistic', 'regression']
        FETCH_JOB_DESC_FLAG = False
        
        remote_jobs = higher_ed_scraper.search_higher_ed_category(
            BASE_URL, SEARCH_KW, OUTPUT_FILE, EXLCUSION_ROLE_KW
            )
        remote_jobs = remote_jobs.with_columns(
            pl.lit('remote').alias('remote_or_lab'),
            pl.lit('True').alias('remote')
        )
    
    
    # Get lab & research jobs (mostly in-person)
    if search_lab_jobs_page:
        BASE_URL = 'https://www.higheredjobs.com/admin/search.cfm?JobCat=150&CatName=Laboratory%20and%20Research'
        OUTPUT_FILE='./scrappy_RA/data_saved_locally/higher_ed/higher_ed_lab_jobs.csv'
        SEARCH_KW = {
            1: ['remote', 'work from home', 'work-from-home'],
            2: ['" R language"', '" R programming"', '" R statistic"', 'RStudio', 'Stata', 'STATA', 'regression', 'econometrics', '"statistical software"', '"statistical program"'],
            3: ['"data science"', '"data scientist"', '"survey research"', 'economic', 'quantitative', 'Python', 'SQL', 'Qualtrics', 'statistic'],
            4: ['analysis', 'data'],
            5: ['tutor', 'assistant']
        }
        FETCH_JOB_DESC_FLAG = False
        
        lab_jobs = higher_ed_scraper.search_higher_ed_category(
            BASE_URL, SEARCH_KW, OUTPUT_FILE, EXLCUSION_ROLE_KW
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
    
    elif not remote_jobs.empty():
        return remote_jobs
        
    elif not lab_jobs.empty():
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