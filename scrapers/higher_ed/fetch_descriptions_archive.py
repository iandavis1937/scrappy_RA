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