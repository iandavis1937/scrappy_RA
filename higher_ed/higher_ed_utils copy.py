

from bs4 import BeautifulSoup
import time
import re
import random
import polars as pl
import gen_utils

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import selenium_utils


def match_skills_keywords(text, keywords):
    """
    Check which keywords are present in the text
    
    Args:
        text: The text to search in
        keywords: List of keywords to search for
    
    Returns:
        list: Keywords that were found
    """
    if not text:
        return []
    
    text_lower = text.lower()
    found = []
    
    for keyword in keywords:
        # Use word boundaries for whole word matching
        # Special handling for 'R' to avoid false positives
        if keyword == 'R':
            # Match 'R' as standalone word or in common contexts
            pattern = r'\b[R]\b|\bR programming\b|\bR language\b|\bR statistical\b'
        else:
            pattern = r'\b' + re.escape(keyword) + r'\b'
        
        if re.search(pattern, text, re.IGNORECASE):
            found.append(keyword)
    
    return found


def fetch_job_descriptions(df):
    """
    Fetch job descriptions for all jobs in the dataframe
    
    Args:
        df: Polars DataFrame with job listings
    
    Returns:
        Polars DataFrame with added 'description' column
    """
    if df.is_empty():
        return df
    
    print(f"\nFetching job descriptions for {len(df)} filtered jobs...")
    
    driver = selenium_utils.setup_driver()
    descriptions = []
    
    try:
        for i, row in enumerate(df.iter_rows(named=True), 1):
            if row['url']:
                print(f"  [{i}/{len(df)}] Fetching: {row['title'][:60]}...")
                
                try:
                    driver.get(row['url'])
                    time.sleep(random.uniform(1.5, 2.5))
                    
                    # Wait for job description to load
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 
                                ".job-description, .job-details, .description"))
                        )
                    except TimeoutException:
                        print(f"    Timeout loading description")
                    
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    
                    # Try multiple possible selectors for job description
                    description_elem = (
                        soup.find('div', class_='job-description') or
                        soup.find('div', class_='description') or
                        soup.find('div', id='job-description') or
                        soup.find('div', class_='job-details') or
                        soup.find('div', class_='jobdescription') or
                        soup.find('div', {'role': 'article'}) or
                        soup.find('div', id='JobDescription')
                    )
                    
                    if description_elem:
                        descriptions.append(description_elem.get_text(separator=' ', strip=True))
                    else:
                        descriptions.append('')
                        
                except Exception as e:
                    print(f"    Error fetching description: {e}")
                    descriptions.append('')
                
                time.sleep(random.uniform(1.5, 2.5))
            else:
                descriptions.append('')
    
    finally:
        driver.quit()
    
    # Add description column to dataframe
    df = df.with_columns(pl.Series('description', descriptions))
    
    print(f"✓ Successfully fetched {sum(1 for d in descriptions if d)} descriptions")
    
    return df


def add_keyword_matches(df, keywords):
    """
    Search for keywords in job descriptions and add match columns
    
    Args:
        df: Polars DataFrame with 'description' column
        keywords: List of keywords to search for
    
    Returns:
        Polars DataFrame with added keyword match columns
    """
    if df.is_empty() or 'description' not in df.columns:
        return df
    
    print(f"\nSearching for keywords in {len(df)} job descriptions...")
    
    # Create a column for each keyword showing if it was found
    for keyword in keywords:
        if keyword == 'R':
            # Special handling for 'R'
            pattern = r'\b[R]\b|\bR programming\b|\bR language\b|\bR statistical\b'
        else:
            pattern = r'\b' + re.escape(keyword) + r'\b'
        
        # Create boolean column for each keyword
        df = df.with_columns(
            pl.col('description')
            .str.contains(f'(?i){pattern}')  # (?i) for case-insensitive
            .fill_null(False)
            .alias(f'has_{keyword.lower()}')
        )
    
    # Create a summary column with all matched keywords
    matched_keywords = []
    for row in df.iter_rows(named=True):
        matches = []
        for keyword in keywords:
            col_name = f'has_{keyword.lower()}'
            if row.get(col_name, False):
                matches.append(keyword)
        matched_keywords.append(', '.join(matches))
    
    df = df.with_columns(pl.Series('matched_keywords', matched_keywords))
    
    # Print statistics
    jobs_with_matches = df.filter(pl.col('matched_keywords') != '').height
    print(f"\nJobs matching technical keywords: {jobs_with_matches} out of {len(df)}")
    
    if jobs_with_matches > 0:
        print("\nKeyword match breakdown:")
        for keyword in keywords:
            col_name = f'has_{keyword.lower()}'
            count = df.filter(pl.col(col_name) == True).height
            if count > 0:
                print(f"  {keyword}: {count} jobs")
    
    print(f"✓ Successful -- DataFrame shape: {df.shape}")
    
    return df


def scrape_jobs(base_url, search_kw):
    """
    Scrape job listings using Selenium
    
    Args:
        base_url: The search results URL
    """
    all_jobs = []
    page = 1
    
    driver = selenium_utils.setup_driver()
    
    try:
        
        while True:
            print(f"\nFetching page {page}...", end=" ")
            
            try:
                # Make URL with keywords
                url = gen_utils.build_search_url(base_url, search_kw, 'filterKeywords')
                print(url)
                
                # Navigate to the URL
                driver.get(url)
                
                # Longer initial wait
                time.sleep(5)
                
                # Save the page source for debugging
                with open(f'selenium_page_{page}.html', 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                print(f"Saved page source to selenium_page_{page}.html")
                
                # Wait for job records to load - try multiple selectors
                try:
                    # Try different possible selectors
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".record, .row.record, div[class*='record']"))
                    )
                except TimeoutException:
                    print("Timeout waiting for job records to load")
                    print("Check selenium_page_1.html to see what loaded")
                    break
                
                # Additional wait for dynamic content
                time.sleep(random.uniform(1.5, 3))
                
                # Get page source and parse with BeautifulSoup
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Save HTML for debugging (optional)
                if page == 1:
                    with open('page_debug.html', 'w', encoding='utf-8') as f:
                        f.write(soup.prettify())
                    print("Saved first page HTML to page_debug.html")
                
                # Find all job records in the result page
                job_records = soup.find_all('div', class_='row record')
                
                if not job_records:
                    print("No more job listings found.")
                    break
                
                print(f"Found {len(job_records)} jobs on this page")
                
                for record in job_records:
                    try:
                        job_data = {
                            'title': '',
                            'url': '',
                            'job_code': '',
                            'organization': '',
                            'location': '',
                            'salary': '',
                            'category': '',
                            'posted_date': '',
                            'priority': ''
                        }
                        
                        # Find the left column (col-sm-7) and right column (col-sm-5)
                        left_col = record.find('div', class_='col-sm-7')
                        right_col = record.find('div', class_='col-sm-5')
                        
                        # Extract job title and URL from left column
                        if left_col:
                            try:
                                title_link = left_col.find('a')
                                if title_link:
                                    job_data['title'] = title_link.get_text(strip=True) or ''
                                    href = title_link.get('href', '')
                                    if href:
                                        # Extract job code from URL (JobCode parameter)
                                        job_code_match = re.search(r'JobCode=(\d+)', href)
                                        if job_code_match:
                                            job_data['job_code'] = job_code_match.group(1)
                                        job_data['url'] = urljoin(base_url, href)
                            except Exception as e:
                                print(f"Warning: Could not extract title/URL - {e}")
                            
                            # Extract organization, location, and salary from left column
                            try:
                                text_parts = []
                                for content in left_col.children:
                                    if content.name == 'br':
                                        continue
                                    text = str(content).strip() if isinstance(content, str) else content.get_text(strip=True)
                                    if text and text not in ['<br>', '<br/>']:
                                        text_parts.append(text)
                                
                                # Remove the title (first item) since we already have it
                                if text_parts:
                                    text_parts = text_parts[1:]
                                
                                # Typically: [organization, location, salary]
                                if len(text_parts) >= 1:
                                    job_data['organization'] = text_parts[0]
                                if len(text_parts) >= 2:
                                    job_data['location'] = text_parts[1]
                                
                                # Extract salary if present (has class 'job-salary')
                                salary_elem = left_col.find('span', class_='job-salary')
                                if salary_elem:
                                    job_data['salary'] = salary_elem.get_text(strip=True) or ''
                                    
                            except Exception as e:
                                print(f"Warning: Could not extract organization/location/salary - {e}")
                        
                        # Extract category and posted date from right column
                        if right_col:
                            try:
                                text_content = right_col.get_text(separator='|', strip=True)
                                parts = [p.strip() for p in text_content.split('|') if p.strip()]
                                
                                # Typically: [category, "Posted MM/DD/YYYY"]
                                if len(parts) >= 1:
                                    job_data['category'] = parts[0]
                                
                                # Extract posted date
                                for part in parts:
                                    if 'Posted' in part:
                                        job_data['posted_date'] = part.replace('Posted', '').strip()
                                        break
                            except Exception as e:
                                print(f"Warning: Could not extract category/date - {e}")
                        
                        # Check if job is marked as priority
                        try:
                            priority_marker = record.find('span', class_='addon-marker')
                            if priority_marker and 'Priority' in priority_marker.get_text():
                                job_data['priority'] = 'Yes'
                            else:
                                job_data['priority'] = 'No'
                        except Exception as e:
                            print(f"Warning: Could not check priority status - {e}")
                        
                        # Only add job if at least title or job_code exists
                        if job_data['title'] or job_data['job_code']:
                            all_jobs.append(job_data)
                        else:
                            print("Warning: Skipping job record with no title or job code")
                            
                    except Exception as e:
                        print(f"Error processing job record: {e}")
                        continue
                
                # Check if there's a next page button and click it
                try:
                    # Look for next page button - adjust selector based on actual pagination
                    next_button = driver.find_element(By.LINK_TEXT, "Next")
                    
                    # Check if button is disabled
                    if 'disabled' in next_button.get_attribute('class'):
                        print("\nReached last page.")
                        break
                    
                    # Scroll to button and click
                    driver.execute_script("arguments[0].scrollIntoView();", next_button)
                    time.sleep(1)
                    next_button.click()
                    
                    # Wait for new page to load
                    time.sleep(random.uniform(2, 4))
                    page += 1
                    
                except NoSuchElementException:
                    print("\nNo next page button found. Reached last page.")
                    break
                except Exception as e:
                    print(f"\nError navigating to next page: {e}")
                    break
    
            except Exception as e:
                print(f"Fatal error during scraping: {e}")
            
    finally:
        # Always close the browser
        print("\nClosing browser...")
        driver.quit()
    
    # Make Data Frame
    if all_jobs:
        print(f"\nConverting {len(all_jobs)} jobs to Polars DataFrame...")
        
        try:
            # Convert list of dicts to Polars DataFrame
            df = pl.DataFrame(all_jobs)
            
            # Reorder columns for consistency
            column_order = ['title', 'organization', 'location', 'url',
                          'salary', 'category', 'posted_date', 'priority', 'job_code']
            
            # Only select columns that exist in the dataframe
            existing_cols = [col for col in column_order if col in df.columns]
            df = df.select(existing_cols)
            
            print(f"✓ Successful -- DataFrame shape: {df.shape}")
            
            return df
            
        except Exception as e:
            print(f"✗ Error creating DataFrame: {e}")
            return None
    else:
        print("No jobs found to save.")
        return None
    
    
def get_higher_ed_jobs(base_url, search_kw, output_file, exclusion_role_kw, skills_kw, fetch_job_desc_flag):
    all_jobs_df_list = []

    # Loop through the dictionary items (relevance_score, keyword_list)
    for relevance_score, keyword_list in search_kw.items():
        print(f"Search #{relevance_score}...")
        jobs_df_i = scrape_jobs(base_url=base_url, search_kw=keyword_list)
        jobs_df_i = jobs_df_i.with_columns(
            pl.lit(relevance_score).alias('kw_idx'),
            pl.lit(keyword_list).alias('kw')
        )
        
        all_jobs_df_list.append(jobs_df_i)

    all_jobs = pl.concat(all_jobs_df_list)

    # Concatenate kw_idx values for duplicate job_codes, then keep first of other columns
    all_jobs = all_jobs.group_by('job_code', maintain_order=True).agg([
        pl.col('kw_idx').str.concat(delimiter=', ').alias('kw_idx'),
        pl.col('kw').flatten().str.concat(delimiter=' | ').alias('kw'),
        pl.all().exclude(['kw_idx', 'kw']).first()
    ])
    
    print(f"\n{'='*50}")
    print(f"Total jobs scraped: {len(all_jobs)}")
    print(f"{'='*50}")
    
    # Save to CSV
    if not all_jobs.is_empty():
        # Exclude rows where title contains any negative keyword
        jobs = all_jobs
        for keyword in exclusion_role_kw:
            jobs = jobs.filter(~pl.col('title').str.to_lowercase().str.contains(keyword.lower())) 
        
        print(f"Total jobs after filtering: {len(jobs)}")
        
        try:
            jobs = jobs.sort('kw_idx')  # Sort by kw_idx (ascending by default)
            jobs.write_csv(output_file)
            print(f"✓ Successfully saved {len(jobs)} job listings!")
        except Exception as e:
            print(f"✗ Error saving to CSV: {e}")
        
        # Check for skills keywords
        if fetch_job_desc_flag:
            print("Checking for skills keywords in job descriptions...")
            jobs = fetch_job_descriptions(jobs)  # Fetch all job descriptions
            jobs = add_keyword_matches(jobs, keywords=skills_kw)  # Search for keywords in descriptions
            
            # Save to CSV
            print(f"\nSaving {len(jobs)} jobs to {output_file}...")
            
            try:
                jobs.write_csv(output_file)
                print(f"✓ Successfully saved {len(jobs)} job listings!")
            except Exception as e:
                print(f"✗ Error saving to CSV: {e}")
        
        # Display sample
        print("\nSample of first 3 jobs:")
        print(jobs.head(3))
        
        return jobs
    else:
        print("No jobs found to save.")