

from .umich_scraper import search_umich


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

    SEARCH_KW = [
                    'RStudio', 'tidyverse', ' R ',
                    'Stata', 'STATA', 'regression', 'econometric', 'Qualtrics',
                    'remote', 'work from home', 'work-from-home',
                    'survey research', 'data science', 'data scientist', 'quantitative', 'economic',
                    'Python', 'statistic', 'SQL',
                    'analysis', 'data',
                    'tutor', 'assistant'
                ]
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
    OUTPUT_FILE='./scrappy_RA/data_to_unify/umich_jobs.csv'

    umich_jobs = search_umich(SEARCH_KW, EXLCUSION_ROLE_KW, OUTPUT_FILE)

    # Save a backup copy as well
    umich_jobs.write_csv("umich_jobs.csv")
    
    return umich_jobs