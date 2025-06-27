from middleware.scroller_pager import SeleniumScroller
import argparse
import asyncio
import hashlib
from database.setup import get_connection, return_connection, close_all_connections
from database.table.seed_domain import create_seed_domain_table, insert_into_seed_domain_table, fetch_domain_url, update_completed_at, update_status, update_depth
from database.table.crawled_url import create_crawled_url_table, insert_into_crawled_url_table, fetch_crawled_url, update_crawled_url_status, update_unique_links
from database.table.url_relationship import create__url_relationship_table, insert_into_url_relationship_table
from status.logger import logger
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from datetime import datetime



# Helper funciton to check the status of the url_path
async def check_status_of_url(conn, url_path):
    async with conn.cursor() as cursor:
        query = """
            SELECT 1 from crawled_url
            WHERE url_path = %s AND crawl_status ='visited'
            LIMIT 1     
        """
        await cursor.execute(query, (url_path,))
        result = await cursor.fetchone()
    return result is not None

# Helper function to get crawl_id by url_hash
async def get_crawl_id_by_hash(conn, url_hash):
    """Get crawl_id for a given URL hash"""
    query = "SELECT crawl_id FROM crawled_url WHERE url_hash = %s"
    async with conn.cursor() as cursor:
        await cursor.execute(query, (url_hash,))
        result = await cursor.fetchone()
    return result[0] if result else None


# Helper function for PostgreSQL
async def check_url_hash_exists(conn, url_hash):
    """Check if a URL hash already exists in the crawled_url_table"""
    query = "SELECT 1 FROM crawled_url WHERE url_hash = %s"
    async with conn.cursor() as cursor:
        await cursor.execute(query, (url_hash,))
        result = await cursor.fetchone()
    return result is not None


async def insert_seed_domain_in_crawled_url(conn):
    url = await fetch_domain_url(conn)
    domain_id = url[0]
    url_path = 'https://'+url[1]+'/'
    url_hash = hashlib.sha1(url_path.encode('utf-8')).hexdigest()
    
    # Check if url_hash already exists in crawled_url_table
    existing_url = await check_url_hash_exists(conn, url_hash)
    if existing_url:
        # Return the existing domain_id and url_path
        return 
    
    depth = 0
    await insert_into_crawled_url_table(conn, domain_id, url_path, url_hash, depth, None, None)
    
    # Return both domain_id and url_path for easier use
    return domain_id, url_path


async def crawl_in_loop(conn, urls, domain_id, depth, base_url, parent_crawl_id, parent_url_content):
    """
    Process discovered URLs and establish parent-child relationships
    
    Args:
        conn: Database connection
        urls: List of HTML strings containing <a> tags
        domain_id: Domain ID for the URLs
        depth: Current crawling depth
        base_url: Base URL for resolving relative URLs
        parent_crawl_id: The crawl_id of the parent URL
        parent_url_content: The url_content of the parent URL
    """
    base_domain = urlparse(base_url).netloc
 
  
    parent_url_hash = hashlib.sha1(base_url.encode('utf-8')).hexdigest()
    
    for url in urls:
        try:
        
            soup = BeautifulSoup(url, 'html.parser')
            a_tag = soup.find('a')
            
            if not a_tag or not a_tag.get('href'):
                continue
                
            url_path = a_tag['href']
          
      
            # Handle relative URLs
            if not url_path.startswith('http'):
                url_path = urljoin(base_url, url_path)
                
            
            # Only process URLs from same domain
            if urlparse(url_path).netloc.replace('www.', '') != base_domain.replace('www.', '') or "/page/" in url_path:
                continue

                
            url_hash = hashlib.sha1(url_path.encode('utf-8')).hexdigest()
            content = a_tag.get_text(strip=True)
            
            if content:  # Only if there's actual content
                # Insert child URL into crawled_url table (allow duplicates)
                await insert_into_crawled_url_table(conn, domain_id, url_path, url_hash, depth, content, None)
                logger.info(f"Inserted child URL: {url_path}")
                
                try:
                    await update_unique_links(conn, domain_id)
                except Exception as e:
                    logger.info(f'Error updating unique links: {e}')
                
                # Get the child_crawl_id for the newly inserted URL
                child_crawl_id = await get_crawl_id_by_hash(conn, url_hash)
            
            if child_crawl_id and parent_crawl_id:
                # Insert parent-child relationship
                await insert_into_url_relationship_table(
                    conn,
                    domain_id=domain_id,
                    parent_url_id=parent_crawl_id,
                    child_url_id=child_crawl_id,
                    parent_depth=depth - 1,  # Parent is one level up
                    child_depth=depth,
                    parent_link_text=parent_url_content,  # Use parent's url_content
                    discovered_at=datetime.now()
                )
                logger.info(f"Created relationship: {parent_crawl_id} -> {child_crawl_id}")
                
        except Exception as e:
            logger.warning(f"Error processing URL in crawl_in_loop: {e}")
            continue


async def scroller_pager(conn):
    unique_urls = set()
    
    # Main loop to process all seed domains
    while True:
        # Check if we need to insert seed domain
        url = await fetch_domain_url(conn)
        
        # If no more domains to process, exit
        if not url:
            logger.info("No more seed domains to process - all domains completed!")
            break
            
        domain_id = url[0]
        seed_url = 'https://'+url[1]+'/'
        max_depth = url[2]
        url_hash = hashlib.sha1(seed_url.encode('utf-8')).hexdigest()
        
        logger.info(f"Starting to process seed domain: {seed_url}")
        
        # Only insert seed domain if hash doesn't exist
        seed_exists = await check_url_hash_exists(conn, url_hash)
        if not seed_exists:
            domain_id, seed_url = await insert_seed_domain_in_crawled_url(conn)
            logger.info(f"Inserted seed domain: {seed_url}")
        else:
            logger.info(f"Seed domain already exists: {seed_url}")
        
        # Inner crawling loop for current domain - keep processing until no more URLs or max depth reached
        while True:
            # Fetch next URL to process for current domain
            url_data = await fetch_crawled_url(conn)
            
            if not url_data:
                logger.info(f"No more URLs to crawl for domain {seed_url} - moving to next domain!")
                break
                
            current_url, current_domain_id, current_depth, crawl_id = url_data
            
            # Check if we've already visited the current url
            if await check_status_of_url(conn, current_url):
                logger.info(f'The url:{current_url} already exists')
                await update_crawled_url_status(conn, current_url, 'visited')
                logger.info(f"Marked {current_url} as visited")
                continue
                
            try:
                await update_depth(conn, current_depth, domain_id)
            except Exception as e:
                logger.info(f'Error updating depth: {e}')
                
            # Check if we've reached max depth for current domain
            if current_depth >= 1:
                logger.info(f"Reached maximum depth of {max_depth} for domain {seed_url}")
                break  # Break inner loop to move to next domain
                
            logger.info(f"Processing: {current_url} at depth {current_depth}")
            
            # Clear previous URLs
            unique_urls.clear()
            
            # Set up args (remove argparse since you're calling this programmatically)
            driver_path = r'C:\Program Files\chromedriver-win64\chromedriver.exe'
            
            # Create scroller
            scroller = SeleniumScroller(headless=True, driver_path=driver_path)

            try:
                # Get the current URL's content for use as parent_url_content
                current_url_query = "SELECT url_content FROM crawled_url WHERE crawl_id = %s"
                async with conn.cursor() as cursor:
                    await cursor.execute(current_url_query, (crawl_id,))
                    result = await cursor.fetchone()
                    current_url_content = result[0] if result and result[0] else ""
                
                # Scroll and get links
                result, scroller_links = scroller.scroll_to_bottom(
                    current_url, 
                    scroll_pause_time=7.0, 
                    max_scrolls=200
                )
                
                if scroller_links:
                    unique_urls.update(scroller_links)
            
                if result:
                    logger.info("Successfully completed scrolling")
                    try:
                        pager, pager_links = scroller.pagination()
                        if pager_links:
                            unique_urls.update(pager_links)
                        
                        if pager:
                            logger.info("Successfully completed pagination")
                            
                    except:
                        logger.info('No Pagination found')
                
                # Process found URLs and add to database with parent-child relationships
                if unique_urls:
                    await crawl_in_loop(
                        conn, 
                        unique_urls, 
                        current_domain_id, 
                        current_depth + 1, 
                        current_url,
                        crawl_id, 
                        current_url_content  
                    )
                
                # Mark current URL as visited
                await update_crawled_url_status(conn, current_url, 'visited')
                logger.info(f"Marked {current_url} as visited")
                    
            except Exception as e:
                logger.error(f"Error processing {current_url}: {e}")
                await update_crawled_url_status(conn, current_url, 'error')
                
            finally:
                scroller.close()
        
        # Update domain completion status after finishing all URLs for this domain
        logger.info(f"Completed processing seed domain: {seed_url}")
        try:
            await update_completed_at(conn, domain_id)  
            await update_status(conn, domain_id)        
        except Exception as e:
            logger.info(f'Error updating completed_at timestamp and status for domain {domain_id}: {e}')
        
    logger.info("All seed domains have been processed!")


async def main():
    conn = await get_connection()
    
    try:
        await create_seed_domain_table(conn)
        await create_crawled_url_table(conn)
        await create__url_relationship_table(conn)
        await insert_into_seed_domain_table(conn)
        await scroller_pager(conn)
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
    finally:
        if conn:
            await return_connection(conn)
            logger.info("Database connection closed")


if __name__ == "__main__":
    asyncio.run(main())