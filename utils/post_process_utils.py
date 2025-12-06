

import os
from pathlib import Path
from typing import List
import polars as pl
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def get_csv_files(folder_path: str) -> List[Path]:
    """Finds all CSV files in the specified folder."""
    try:
        path = Path(folder_path)
        # Finds all files ending with .csv, case-insensitively
        csv_files = list(path.glob('*.csv')) + list(path.glob('*.CSV'))
        if not csv_files:
            print(f"✗ Error: No CSV files found in '{folder_path}'.")
            # Create the folder if it doesn't exist to guide the user
            path.mkdir(exist_ok=True)
        return csv_files
    except Exception as e:
        print(f"✗ Error reading directory: {e}")
        return []
    

def batch_rename(df: pl.DataFrame, colnm_d: dict) -> pl.DataFrame:
    """
    Checks if 'job_code' exists in the DataFrame and renames it to 'job_id'.
    Leaves the DataFrame unchanged if 'job_code' is not present.
    """
    
    for old_nm, new_nm in colnm_d.items():
        
        if old_nm in df.columns:
            # print(f"-> Renaming column '{old_nm}' to '{new_nm}'.")
            df = df.rename({old_nm: new_nm})
        else:
            # print(f"-> Column '{old_nm}' not found. Column names remain as is.")
            print()
        
    return df


def combine_csvs_to_polars(csv_files: List[Path]) -> pl.DataFrame:
    """Reads and combines CSV files into a single Polars DataFrame."""
    if not csv_files:
        # Define an empty DataFrame with a useful schema to ensure consistency
        print("Returning empty DataFrame with schema.")
        return pl.DataFrame(schema={
            'title': pl.Utf8, 'job_id': pl.Utf8, 'location': pl.Utf8
        })

    # Read all CSV files
    try:
        print("Reading CSVs...")
        file_paths = [str(f) for f in csv_files]
        print(file_paths)
        
        # Standardize data frame schema (Add missing columns as Null)
        EXPECTED_COLUMNS = [
            'title', 'organization', 'department', 'location', 'remote',
            'posted_date',
            'kw', 'kw_idx',
            'url', 'salary', 'category',
            'employment_type', 'job_id', 'remote_or_lab'
            ]
        
        df_list = []
        for i, path in enumerate(file_paths):
            
            # Read with schema_overrides as dict
            colnms = set(EXPECTED_COLUMNS)
            df = pl.read_csv(
                path,
                schema_overrides={nm: pl.Utf8 for nm in colnms}
            )
            
            if df is None or df.is_empty():
                print("Skipping empty or None DataFrame.")
                continue
            
            # Standardize column names
            print("Standardizing column names...")
            colnm_d = {
                'job_code': 'job_id', 'posting_date': 'posted_date'
            }
            df = batch_rename(df, colnm_d)
    
            clean_cols = []
            for col in EXPECTED_COLUMNS:
                if col in df.columns:
                    clean_cols.append(pl.col(col).cast(pl.Utf8))  # Mutate to string type
                else:
                    # Add missing column with a Null type (important for concat)
                    clean_cols.append(pl.lit(None, dtype=pl.Utf8).alias(col)) 

            std_df = df.select(clean_cols)
            
            # Add filename column
            std_df = std_df.with_columns(pl.lit(file_paths[i]).alias('scraper'))
            df_list.append(std_df)

        df = pl.concat(df_list)
        print(f"✓ Successfully combined {len(df_list)} files into one DataFrame.")
        
        # Add matched keywords count column
        df = df.with_columns(
            pl.col('kw_idx').str.count_matches(',').add(1).alias('kw_num'),
            pl.col('kw_idx').str.split(', ').list.first().cast(pl.Int64).alias('kw_idx1')
        )
        
        return df
    except Exception as e:
        print(f"✗ Error during Polars read/concat: {e}")
        # Return an empty DataFrame on failure
        return pl.DataFrame()


def export_to_google_sheets(df: pl.DataFrame, sheet_title: str, worksheet_name: str, creds_file: str):
    """
    Exports a Polars DataFrame to a specified Google Sheet.
    """
    if df.is_empty():
        print("No data to export to Google Sheets.")
        return

    print(f"\nAuthenticating and exporting data to Google Sheet '{sheet_title}'...")
    try:
        # Authenticate using the downloaded JSON key file
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        client = gspread.authorize(creds)
    except Exception as e:
        print(f"✗ Authentication Error: Check your CREDENTIALS_FILE path and ensure the API key is valid.")
        print(f"Details: {e}")
        return

    try:
        # Open the target Google Sheet document by title
        sh = client.open(sheet_title)
    except gspread.SpreadsheetNotFound:
        print(f"✗ Error: Google Sheet named '{sheet_title}' not found.")
        print("Please ensure the sheet exists and the Service Account email has editor access.")
        return
    except Exception as e:
        print(f"✗ Error opening sheet: {e}")
        return

    # Prepare data for gspread
    # 1. Convert Polars DataFrame to a list of lists (including headers)
    header = df.columns
    data = df.rows()
    data_for_sheet = [header] + data
    
    try:
        # Get or create the worksheet
        try:
            worksheet = sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=worksheet_name, rows=len(data_for_sheet) + 1, cols=len(header))
            print(f"Worksheet '{worksheet_name}' created.")
            
        # Clear existing data and write new data
        worksheet.clear()
        worksheet.update('A1', data_for_sheet)
        
        print(f"✓ Success! Exported {df.shape[0]} rows and {df.shape[1]} columns to:")
        print(f"  https://docs.google.com/spreadsheets/d/{sh.id}")
        
    except Exception as e:
        print(f"✗ Error updating worksheet: {e}")
