

import os
import pickle
from pathlib import Path
from typing import List
from datetime import datetime, timedelta
import polars as pl
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


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


def parse_relative_date(date_str: str) -> datetime:
    """Convert relative date strings to actual dates."""
    date_str = date_str.strip().lower()
    today = datetime.now()
    
    if date_str == "today":
        return today
    elif date_str == "yesterday":
        return today - timedelta(days=1)
    elif "day ago" in date_str or "days ago" in date_str:
        days = int(date_str.split()[0])
        return today - timedelta(days=days)
    elif "week ago" in date_str or "weeks ago" in date_str:
        weeks = int(date_str.split()[0])
        return today - timedelta(weeks=weeks)
    elif "month ago" in date_str or "months ago" in date_str:
        months = int(date_str.split()[0])
        return today - timedelta(days=months * 30)  # Approximation
    else:
        # Try parsing as actual date
        try:
            return datetime.strptime(date_str, "%m/%d/%Y")
        except ValueError:
            return today


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
            col_nms = set(EXPECTED_COLUMNS)
            df = pl.read_csv(
                path,
                schema_overrides={nm: pl.Utf8 for nm in col_nms}
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
        
        # Filter out dates earlier than 3 months ago
        # Cast posted_date to Date type and filter out dates earlier than 3 months ago
        three_months_ago = datetime.now() - timedelta(days=90)

        df = df.with_columns(
            pl.col("posted_date")
            .map_elements(parse_relative_date, return_dtype=pl.Datetime)
            .alias("posted_date")
        ).filter(
            pl.col("posted_date") >= three_months_ago.date()
        ).with_columns(
            pl.col("posted_date").dt.strftime("%m/%d/%Y").alias("posted_date")
        )
        print(f"✓ Filtered to dates from {three_months_ago.date()} onward. Rows remaining: {len(df)}")
        
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


def export_to_google_sheets(
    df: pl.DataFrame,
    sheet_title: str,
    worksheet_name: str,
    creds_file: str,
    column_name: str = None):
    """
    Exports a Polars DataFrame to a specified Google Sheet.
    
    Args:
        df: Polars DataFrame to export
        sheet_title: Name of the Google Sheet document
        worksheet_name: Name of the worksheet/tab
        creds_file: Path to credentials JSON file
        column_name: Optional. If provided, only updates this column. 
                    If None, updates the entire sheet.
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
            worksheet = sh.add_worksheet(title=worksheet_name, rows=df.shape[0] + 1, cols=df.shape[1])
            print(f"Worksheet '{worksheet_name}' created.")
        
        # If column_name is specified, only update that column
        if column_name:
            if column_name not in df.columns:
                print(f"✗ Error: Column '{column_name}' not found in DataFrame.")
                print(f"Available columns: {df.columns}")
                return
            
            # Find the column index in the existing sheet
            existing_headers = worksheet.row_values(1)
            
            if column_name not in existing_headers:
                print(f"✗ Error: Column '{column_name}' not found in existing sheet headers.")
                print(f"Existing headers: {existing_headers}")
                return
            
            # Get column index (1-based for gspread)
            col_index = existing_headers.index(column_name) + 1
            col_letter = gspread.utils.rowcol_to_a1(1, col_index)[:-1]  # Get just the letter (e.g., 'A', 'B')
            
            # Prepare column data (including header)
            column_data = [[column_name]] + [[val] for val in df[column_name].to_list()]
            
            # Update only this column
            range_notation = f'{col_letter}1:{col_letter}{len(column_data)}'
            worksheet.update(range_notation, column_data)
            
            print(f"✓ Success! Updated column '{column_name}' ({len(column_data)-1} rows) in column {col_letter}")
            print(f"  https://docs.google.com/spreadsheets/d/{sh.id}")
            
        else:
            # Update entire sheet (original behavior)
            header = df.columns
            data = df.rows()
            data_for_sheet = [header] + data
            
            # Clear existing data and write new data
            worksheet.clear()
            worksheet.update('A1', data_for_sheet)
            
            print(f"✓ Success! Exported {df.shape[0]} rows and {df.shape[1]} columns to:")
            print(f"  https://docs.google.com/spreadsheets/d/{sh.id}")
        
    except Exception as e:
        print(f"✗ Error updating worksheet: {e}")
        

def read_from_google_sheets(sheet_title: str, worksheet_name: str, creds_file: str) -> pl.DataFrame:
    """
    Reads data from a specified Google Sheet and returns a Polars DataFrame.
    
    Args:
        sheet_title: Name of the Google Sheet document
        worksheet_name: Name of the specific worksheet/tab
        creds_file: Path to the service account credentials JSON file
    
    Returns:
        Polars DataFrame with the sheet data
    """
    print(f"\nAuthenticating and reading data from Google Sheet '{sheet_title}'...")
    
    try:
        # Authenticate using the downloaded JSON key file
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        client = gspread.authorize(creds)
    except Exception as e:
        print(f"✗ Authentication Error: Check your CREDENTIALS_FILE path and ensure the API key is valid.")
        print(f"Details: {e}")
        return pl.DataFrame()

    try:
        # Open the target Google Sheet document by title
        sh = client.open(sheet_title)
    except gspread.SpreadsheetNotFound:
        print(f"✗ Error: Google Sheet named '{sheet_title}' not found.")
        print("Please ensure the sheet exists and the Service Account email has viewer/editor access.")
        return pl.DataFrame()
    except Exception as e:
        print(f"✗ Error opening sheet: {e}")
        return pl.DataFrame()

    try:
        # Get the worksheet
        worksheet = sh.worksheet(worksheet_name)
        
        # Get all values from the worksheet
        data = worksheet.get_all_values()
        
        if not data:
            print(f"✗ Warning: Worksheet '{worksheet_name}' is empty.")
            return pl.DataFrame()
        
        # First row is headers, rest is data
        headers = data[0]
        rows = data[1:]
        
        # Create Polars DataFrame
        df = pl.DataFrame(rows, schema=headers, orient="row")
        
        print(f"✓ Success! Read {df.shape[0]} rows and {df.shape[1]} columns from worksheet '{worksheet_name}'")
        print(f"  Columns: {df.columns}")
        
        return df
        
    except gspread.WorksheetNotFound:
        print(f"✗ Error: Worksheet '{worksheet_name}' not found in sheet '{sheet_title}'.")
        return pl.DataFrame()
    except Exception as e:
        print(f"✗ Error reading worksheet: {e}")
        return pl.DataFrame()


def get_user_credentials():
    """Authenticate as the user (not service account)"""
    scope = [
        'https://www.googleapis.com/auth/documents',
        'https://www.googleapis.com/auth/drive'
    ]
    
    OAuth_creds_file = "/mnt/c/wd/scrappy_RA/creds/OAuth_creds.json"
    
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(OAuth_creds_file, scope)
            creds = flow.run_local_server(port=0)
        
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return creds


def write_gdoc_letter(
    text: str,
    doc_title: str,
    creds_file: str,
    folder_id: str="1fXbsMrE_MU4wchsll1X4bi9SuK6q5ikV"
    ):
    """
    Creates a formatted cover letter in Google Docs inside the specified folder.

    Args:
        text: The cover letter text.
        doc_title: Title for the document.
        creds_file: Path to the credentials file.
        folder_id: The ID of the folder to store the document in.

    Returns:
        Document URL.
    """
    try:
        ## Define the scopes for Docs and Drive APIs
        # scope = [
        #     'https://www.googleapis.com/auth/documents',
        #     'https://www.googleapis.com/auth/drive'
        # ]
        
        ## Load credentials from service account
        # creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        
        creds = get_user_credentials()
        service = build('drive', 'v3', credentials=creds)  # Using Drive API to create the document
        
        # Create the Google Docs document directly in the specified folder
        print("Setting metadata...")
        file_metadata = {
            'name': doc_title,  # Set document title
            'mimeType': 'application/vnd.google-apps.document',  # Google Docs mime type
            'parents': [folder_id]  # Specify the folder ID to place the document inside
        }
        
        print("Creating document...")
        file = service.files().create(
            body=file_metadata
        ).execute()
        
        doc_id = file['id']
        
        # Create the Docs service to insert content and formatting
        docs_service = build('docs', 'v1', credentials=creds)

        # Define header content
        header_text = "Ian A. Davis\niandavis1937@gmail.com              ·            	+1 (518) 278-4071                   ·          	Ballston Spa, NY 12020\n\n"
        # header_text = "Ian A. Davis\niandavis1937@gmail.com  ·  +1 (518) 278-4071  ·  Ballston Spa, NY 12020\n\n"
        
        # Insert header first
        requests = [{
            'insertText': {
                'location': {'index': 1},
                'text': header_text
            }
        }]
        
        # Calculate where body text starts (after header)
        current_index = len(header_text) + 1
        
        # Insert body paragraphs
        paragraphs = text.split('\n\n')
        for para in paragraphs:
            if para.strip():
                requests.append({
                    'insertText': {
                        'location': {'index': current_index},
                        'text': para.strip() + '\n\n'
                    }
                })
                current_index += len(para.strip()) + 2
        
        # Format the header (name bold and larger, contact info centered)
        header_name_end = len("Ian A. Davis") + 1
        header_contact_start = header_name_end + 1
        header_end = len(header_text)
        
        requests.extend([
            # Make name bold and 14pt
            {
                'updateTextStyle': {
                    'range': {
                        'startIndex': 1,
                        'endIndex': header_name_end
                    },
                    'textStyle': {
                        'bold': True,
                        'fontSize': {'magnitude': 14, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Cambria'}
                    },
                    'fields': 'bold,fontSize,weightedFontFamily'
                }
            },
            # Left-align name
            {
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': 1,
                        'endIndex': header_name_end
                    },
                    'paragraphStyle': {
                        'alignment': 'START'
                    },
                    'fields': 'alignment'
                }
            },
            # Format contact info (10pt, centered)
            {
                'updateTextStyle': {
                    'range': {
                        'startIndex': header_contact_start,
                        'endIndex': header_end
                    },
                    'textStyle': {
                        'fontSize': {'magnitude': 10, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Cambria'}
                    },
                    'fields': 'bold,fontSize,weightedFontFamily'
                }
            },
            # Center contact info
            {
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': header_contact_start,
                        'endIndex': header_end
                    },
                    'paragraphStyle': {
                        'alignment': 'CENTER'
                    },
                    'fields': 'alignment'
                }
            },
            # Format body text (Times New Roman, 12pt, single-spaced)
            {
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': header_end,
                        'endIndex': current_index
                    },
                    'paragraphStyle': {
                        'lineSpacing': 100,
                        'spaceAbove': {'magnitude': 0, 'unit': 'PT'},
                        'spaceBelow': {'magnitude': 0, 'unit': 'PT'},
                        'alignment': 'START'  # Left-align body text
                    },
                    'fields': 'lineSpacing,spaceAbove,spaceBelow,alignment'
                }
            },
            {
                'updateTextStyle': {
                    'range': {
                        'startIndex': header_end,
                        'endIndex': current_index
                    },
                    'textStyle': {
                        'fontSize': {'magnitude': 12, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Times New Roman'}
                    },
                    'fields': 'fontSize,weightedFontFamily'
                }
            }
        ])
                
        # Apply the requests (insert text + formatting)
        print("Inserting text...")
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': requests}
        ).execute()

        # Construct the URL to the newly created document
        doc_url = f'https://docs.google.com/document/d/{doc_id}/edit'
        print(f"✓ Formatted document created and saved in folder: {doc_url}")
        
        return doc_url
        
    except HttpError as e:
        print(f"✗ Error creating or saving the document: {e}")
        return None
