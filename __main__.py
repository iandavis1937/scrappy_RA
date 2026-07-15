

print("Importing modules...")
import os
import sys
import traceback
from scrappy_RA.utils import post_process_utils
from scrappy_RA.scrapers.higher_ed.higher_ed import run_higher_ed_module
from scrappy_RA.scrapers.umich.umich import run_umich_module
from scrappy_RA.scrapers.berkeley.berkeley import run_berkeley_module


# --- CONFIGURATION ---
FOLDER_PATH = './scrappy_RA/data_to_unify'
CREDENTIALS_FILE = './scrappy_RA/creds/job-scraper-479904-fcff09f61f6f.json'
SHEET_TITLE = 'Job Scraper'
WORKSHEET_NAME = 'Universities (RA, Admin., TA, etc.)'

FETCH_HIGHER_ED_FLAG = True
FETCH_UMICH_FLAG = False
FETCH_BERKELEY_FLAG = False


def has_results(result) -> bool:
    """True if a module's return value is a non-empty DataFrame."""
    if result is None:
        return False
    if hasattr(result, "is_empty"):
        return not result.is_empty()
    return bool(result)


def run_module_safely(name, fn, **kwargs):
    """
    Run a scraper module, catching any exception so one module's failure
    doesn't stop the others from running. Returns the module's result on
    success, or None if it raised an error.
    """
    try:
        result = fn(**kwargs)
    except Exception:
        print(f"\n✗ {name} module failed with an error:")
        traceback.print_exc()
        return None

    if has_results(result):
        print(f"✓ {name} module completed - {len(result)} rows.")
    else:
        print(f"⚠ {name} module returned no results.")
    return result


if __name__ == '__main__':
    results = {}

    if FETCH_HIGHER_ED_FLAG:
        results['higher_ed'] = run_module_safely(
            'HigherEd', run_higher_ed_module,
            search_remote_jobs_page=True,
            search_lab_jobs_page=True,
            fetch_desc=True,
            desc_limit=100,  # will continue to fetch uncached descriptions on subsequent runs
            ai_enrich=False
            )

    if FETCH_UMICH_FLAG:
        results['umich'] = run_module_safely('UMich', run_umich_module)

    if FETCH_BERKELEY_FLAG:
        results['berkeley'] = run_module_safely('Berkeley', run_berkeley_module)

    # Only halt if every module that ran failed or returned nothing. The
    # combine/export step below is gated behind this same check.
    any_results = any(has_results(r) for r in results.values())
    if not any_results:
        print("\n✗ No modules returned any results. Halting.")
        sys.exit(1)

    n_ok = sum(1 for r in results.values() if has_results(r))
    print(f"\n{n_ok}/{len(results)} module(s) completed with results.")

    if any_results:
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