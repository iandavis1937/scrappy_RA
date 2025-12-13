

print("Importing modules...")
import os
from .utils import post_process_utils
from .scrapers.higher_ed.higher_ed import run_higher_ed_module
from .scrapers.umich.umich import run_umich_module
from .scrapers.berkeley.berkeley import run_berkeley_module


# --- CONFIGURATION ---
FOLDER_PATH = './scrappy_RA/data_to_unify'
CREDENTIALS_FILE = './scrappy_RA/creds/job-scraper-479904-fcff09f61f6f.json' # Rename this to your file
SHEET_TITLE = 'Job Scraper'
WORKSHEET_NAME = 'Universities (RA, Admin., TA, etc.)'

FETCH_HIGHER_ED_FLAG = True
FETCH_UMICH_FLAG = True
FETCH_BERKELEY_FLAG = True


if __name__ == '__main__':
    if FETCH_HIGHER_ED_FLAG:
        run_higher_ed_module(True, True)
        
    if FETCH_UMICH_FLAG:
        run_umich_module()
        
    if FETCH_BERKELEY_FLAG:
        run_berkeley_module()
        
    # Check if the input folder exists and find files
    if not os.path.exists(FOLDER_PATH):
        print(f"Creating input folder: '{FOLDER_PATH}'. Please place your CSV files here.")
        os.makedirs(FOLDER_PATH)
    
    csv_files = post_process_utils.get_csv_files(FOLDER_PATH)
    
    # Combine files into a Polars DataFrame
    combined_df = post_process_utils.combine_csvs_to_polars(csv_files)
    combined_df = combined_df.sort(['kw_idx1', 'kw_num'], descending=[False, True], nulls_last=True)
    combined_df.write_csv("./scrappy_RA/data_saved_locally/jobs_combined.csv")

    # Export to Google Sheets
    if not combined_df.is_empty():
        post_process_utils.export_to_google_sheets(combined_df, SHEET_TITLE, WORKSHEET_NAME, CREDENTIALS_FILE)
    else:
        print("\nProcess finished. No valid data was combined or exported.")