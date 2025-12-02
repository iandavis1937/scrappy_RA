<style type="text/css">
    ol { list-style-type: upper-alpha; }
</style>

# --- SETUP INSTRUCTIONS FOR GOOGLE SHEETS API ---
## 1. Install necessary libraries:
```python
pip install polars gspread oauth2client
```

## 2. Set up Google Cloud Project and API:
- Go to Google Cloud Console (console.cloud.google.com).
- Create a new project.
- Enable the "Google Drive API" and "Google Sheets API" for the project.

## 3. Create a Service Account and JSON Key File:
- Go to "IAM & Admin" -> "Service Accounts".
- Create a new Service Account.
- Create a new JSON key for this account and download it.
- Save the JSON file in your project folder and update the 'CREDENTIALS_FILE' variable below.

## 4. Share the Target Google Sheet:
- In Google Sheets, share the document with the Service Account's email address (found in the downloaded JSON file).