# Forthcoming:
- UC Berkeley
- UNC Chapel Hill
- UCLA

# For a given scraper, call main scraping function:
- Set up Selenium driver (if using)
- Append search keywords to URL
- Fetch the URL
- Pull out soup and listings elements
- Iterate through pages of results
- Convert dictionary of results to Polars df

# With results of main scraping function:
- Bind together results Polars data frame results from each search term query
- Filter out rows with roles containing exclusion terms (e.g. Director)
- Sort by keyword index (kw_idx)
- Save site results to CSV in folder meant to stage pieces to bind together into final output

# In main:
- Choose which sites to fetch results from (which scrapers to call)
- Retrieve filenames of CSVs from each scraper in staging folder
- Load CSVs into list of polars data frames
- Standardize column names 
    - Rename equivalent columns
    - Drop unwanted columns & add columns of nulls to always have the same num. of cols
    - Make all columns into string columns (UTF8)
    - Record the intermediate CSV the df came from using the CSV filename as 'scraper' 
- Bind all intermediate data frames into one
- Save a CSV copy
- Export to Google Sheets
    - Establish credentials
    - Restructure from Polars df to list of lists
    - Overwrite with new data