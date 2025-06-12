import sqlite3
import pandas as pd
import asyncio
import aiohttp
import random
from bs4 import BeautifulSoup, NavigableString
from datetime import datetime
import re
from urllib.parse import urlparse, unquote


def clean_urls(dataframe, url_column='url'):
    """
    Removes query parameters (everything after and including '?') from URLs in the specified column.
    
    Args:
        dataframe (pandas.DataFrame): DataFrame containing the URLs
        url_column (str): Name of the column containing URLs (default: 'url')
        
    Returns:
        pandas.DataFrame: DataFrame with cleaned URLs
    """
    df_clean = dataframe.copy()
    
    def remove_query_params(url):
        if pd.isna(url): 
            return url
        
        return url.split('?')[0]
    
    df_clean[url_column] = df_clean[url_column].apply(remove_query_params)
    
    return df_clean


def create_fulltext_table(db_path):
    """
    Create a full_text table in the SQLite database if it doesn't exist.
    
    Args:
        db_path (str): Path to the SQLite database file
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS full_text (
        id TEXT PRIMARY KEY,
        url TEXT,
        title TEXT,
        content TEXT,
        extraction_date TEXT
    )
    ''')
    
    conn.commit()
    conn.close()
    print("Full_text table created or already exists in the database.")

def extract_id_from_iltalehti_url(url):
    """
    Extract the numeric ID from an Iltalehti URL.
    
    Args:
        url (str): Iltalehti article URL
        
    Returns:
        str: The extracted ID or None if not found
    """
    url = url.split('?')[0] 
    
    path_parts = url.strip('/').split('/')
    if path_parts and len(path_parts) >= 3:
        last_part = path_parts[-1]
        if last_part.startswith('a/'):
            return last_part[2:]
        return last_part
    return None

def extract_id_from_iltasanomat_url(url):
    """
    Extract the ID from an Iltasanomat URL.
    
    Args:
        url (str): Iltasanomat article URL
        
    Returns:
        str: The extracted ID or None if not found
    """
    url = url.split('?')[0]
    
    parsed_url = urlparse(url)
    
    path = parsed_url.path
    
    path_parts = path.strip('/').split('/')
    
    if path_parts and len(path_parts) >= 1:
    
        last_part = path_parts[-1]
        
        match = re.search(r'^(\d+)', last_part)
        if match:
            return match.group(1)
        
        return last_part
    
    return None

def extract_text_from_il_html(html_content):
    """
    Extract article text from Iltalehti HTML content.
    
    Args:
        html_content (str): HTML content of the article
        
    Returns:
        dict: Dictionary with title and content
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    title_element = soup.find('h1', class_='article-title')
    title = title_element.get_text().strip() if title_element else "No title found"
    
    article_body = soup.find('div', class_='article-body')
    
    if not article_body:

        article_body = soup.find('div', class_='article-content')
    
    if not article_body:
        raise ValueError("Could not find article content")
    
    paragraphs = []
    for p in article_body.find_all(['p', 'h2', 'h3']):
    
        if p.get('class') and any(c in ['embed', 'social', 'ad', 'related'] for c in p.get('class')):
            continue
        
        text = p.get_text().strip()
        if text:
            paragraphs.append(text)
    
    if not paragraphs:
        raise ValueError("No paragraphs found in the article")
    
    return {
        'title': title,
        'content': '\n\n'.join(paragraphs)
    }

def extract_text_from_is_html(html_content):
    """
    Extract article text from Ilta-Sanomat (is.fi) HTML content based on the current site structure.
    
    Args:
        html_content (str): HTML content of the article
        
    Returns:
        dict: Dictionary with title and content
    """
    from bs4 import BeautifulSoup, NavigableString
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    
    title_element = soup.find('h1', class_='article-headline--medium')
    if not title_element:
        
        title_element = soup.find('h1', class_='article-headline')
    
    if title_element and title_element.find('span'):
        title = title_element.find('span').get_text().strip()
    elif title_element:
        title = title_element.get_text().strip()
    else:
        title = "No title found"
    
    article_body = soup.find('section', class_='article-body')
    
    if not article_body:
        raise ValueError("Could not find article content section")
    
    paragraphs = []
    
    for p in article_body.find_all('p', class_='article-body'):
        text = p.get_text().strip()
        if text:
            paragraphs.append(text)
    
    for subtitle in article_body.find_all('h3', class_='article-subtitle-20'):
        if subtitle.find('span'):
            text = subtitle.find('span').get_text().strip()
            if text:
                paragraphs.append(f"### {text}")
    
    if not paragraphs:
        
        for p in article_body.find_all('p'):
            text = p.get_text().strip()
            if text:
                paragraphs.append(text)
    
    if not paragraphs:
        raise ValueError("No paragraphs found in the article")
    
    ingress = soup.find('p', class_='article-ingress--medium')
    if ingress:
        ingress_text = ingress.get_text().strip()
        paragraphs.insert(0, ingress_text)
    
    return {
        'title': title,
        'content': '\n\n'.join(paragraphs)
    }


async def fetch_article(session, url, delay=1.0):
    """
    Fetch article HTML content.
    
    Args:
        session (aiohttp.ClientSession): HTTP session
        url (str): Article URL
        delay (float): Delay between requests
        
    Returns:
        tuple: (url, html_content)
    """
    try:
        # Random delay to be polite to the server
        await asyncio.sleep(random.uniform(0, delay * 2))
        
        # Set headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                content = await response.text()
                print(f"Successfully fetched {url}")
                return url, content
            else:
                print(f"Failed to fetch {url}: HTTP {response.status}")
                return url, None
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        return url, None

async def process_urls(urls, db_path, max_concurrent=5, delay=1.0):
    """
    Process a list of URLs by fetching and extracting content.
    
    Args:
        urls (list): List of URLs to process
        db_path (str): Path to the SQLite database
        max_concurrent (int): Maximum number of concurrent requests
        delay (float): Delay between requests
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check which URLs are already in the database
    placeholders = ','.join(['?'] * len(urls))
    cursor.execute(f"SELECT url FROM full_text WHERE url IN ({placeholders})", urls)
    existing_urls = set(row[0] for row in cursor.fetchall())
    
    # Filter out URLs that are already processed and only keep Iltalehti or Iltasanomat URLs
    urls_to_process = [
        url for url in urls 
        if url not in existing_urls and 
        ('iltalehti.fi' in url or 'is.fi' in url)
    ]
    
    if not urls_to_process:
        print("No new URLs to process")
        conn.close()
        return
    
    print(f"Processing {len(urls_to_process)} URLs")
    
    # Create a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def bounded_fetch(url):
        async with semaphore:
            return await fetch_article(session, url, delay)
    
    async with aiohttp.ClientSession() as session:
        # Fetch all articles
        tasks = [bounded_fetch(url) for url in urls_to_process]
        results = await asyncio.gather(*tasks)
        
        # Process results
        for url, html_content in results:
            if html_content:
                try:
                    article_id = None
                    article_data = None
                    
                    # Process based on the source
                    if 'iltalehti.fi' in url:
                        article_id = extract_id_from_iltalehti_url(url)
                        article_data = extract_text_from_il_html(html_content)
                    elif 'is.fi' in url:
                        article_id = extract_id_from_iltasanomat_url(url)
                        article_data = extract_text_from_is_html(html_content)
                    
                    if not article_id:
                        print(f"Could not extract ID from URL: {url}")
                        continue
                    
                    if not article_data:
                        print(f"Could not extract content from URL: {url}")
                        continue
                    
                    # Insert into database
                    cursor.execute(
                        "INSERT INTO full_text (id, url, title, content, extraction_date) VALUES (?, ?, ?, ?, ?)",
                        (
                            article_id,
                            url,
                            article_data['title'],
                            article_data['content'],
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        )
                    )
                    conn.commit()
                    print(f"Stored article with ID {article_id} in database")
                    
                except Exception as e:
                    print(f"Error processing {url}: {str(e)}")
    
    conn.close()

def process_df_urls(df, url_column, db_path, max_concurrent=5, delay=1.0):
    """
    Process URLs from a DataFrame and store article content in the database.
    
    Args:
        df (pandas.DataFrame): DataFrame containing URLs
        url_column (str): Name of the column containing URLs
        db_path (str): Path to the SQLite database
        max_concurrent (int): Maximum number of concurrent requests
        delay (float): Delay between requests
    """
    create_fulltext_table(db_path)
    
    # Filter only Iltalehti and Iltasanomat URLs
    news_urls = [url for url in df[url_column].tolist()] 
    
    if not news_urls:
        print("No Iltalehti or Iltasanomat URLs found in the DataFrame")
        return
    
    # Process the URLs
    asyncio.run(process_urls(news_urls, db_path, max_concurrent, delay))

def get_fulltext_stats(db_path):
    """
    Get statistics about the full_text table.
    
    Args:
        db_path (str): Path to the SQLite database
        
    Returns:
        dict: Statistics about the full_text table
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get total number of articles
    cursor.execute("SELECT COUNT(*) FROM full_text")
    total = cursor.fetchone()[0]
    
    # Get number of articles by source (based on URL)
    cursor.execute("""
    SELECT 
        CASE 
            WHEN url LIKE '%iltalehti.fi%' THEN 'iltalehti'
            WHEN url LIKE '%is.fi%' THEN 'iltasanomat'
            ELSE 'other'
        END as source,
        COUNT(*) as count
    FROM full_text
    GROUP BY source
    """)
    source_counts = dict(cursor.fetchall())
    
    # Get latest extraction date
    cursor.execute("SELECT MAX(extraction_date) FROM full_text")
    latest = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'total_articles': total,
        'by_source': source_counts,
        'latest_extraction': latest
    }