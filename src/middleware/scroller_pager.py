import time
import argparse
import http.client
import socket
import gc 
import re
import subprocess
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
from time import sleep
from status.logger import logger
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urljoin, urlparse
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException


http.client.HTTPConnection.timeout = 1200  
socket.setdefaulttimeout(1200) 

class SeleniumScroller:
    
    def __init__(self, headless=False, driver_path=r'C:\Program Files\chromedriver-win64\chromedriver.exe'):
        # Set up Chrome options
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")  # New headless mode uses less memory
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")  # Reduces memory usage
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--js-flags='--expose-gc'")  
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images for even less memory
        
        # Initialize the Chrome driver with the specified path and keep_alive=True
        self.driver = webdriver.Chrome(
        service=Service(driver_path),
        options=chrome_options,
        keep_alive=True
)
        
        # Set WebDriver timeouts
        self.driver.set_page_load_timeout(300)
        self.driver.set_script_timeout(300)
        
    def safe_execute_script(self, script, max_retries=3):
        """Execute JavaScript with retry mechanism for timeouts"""
        for attempt in range(max_retries):
            try:
                return self.driver.execute_script(script)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise  # Re-raise the last exception if all retries failed
                logger.info(f"Script execution failed (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(1)  # Wait before retrying
        
    def extract_and_clear_dom(self):
        """Extract a tags and clear unnecessary DOM elements to save memory"""
        return self.safe_execute_script("""
            // First, collect all the <a> tags we want
            const anchors = Array.from(document.querySelectorAll('a'));
            const links = anchors.map(a => {
                // Extract needed information (href, text, etc.)
                return {
                    href: a.href || '',
                    text: a.textContent || '',
                    outerHTML: a.outerHTML || ''
                };
            });
            
            // Now clean up the DOM
            // 1. Remove all images (they consume a lot of memory)
            const images = document.querySelectorAll('img');
            images.forEach(img => img.remove());
            
            // 2. Remove already processed content (for example, content well above viewport)
            // This assumes we're scrolling down and won't need earlier content
            const cleanupHeight = window.scrollY - 5000; // Keep some buffer above current position
            if (cleanupHeight > 0) {
                // Find elements that are entirely above the cleanup threshold
                const elements = document.querySelectorAll('div, section, article, aside, footer');
                elements.forEach(el => {
                    const rect = el.getBoundingClientRect();
                    // If the element is entirely above our cleanup threshold
                    if (rect.bottom + window.scrollY < cleanupHeight) {
                        // Replace with a small placeholder to maintain document structure
                        const placeholder = document.createElement('div');
                        placeholder.style.height = rect.height + 'px';
                        placeholder.style.width = rect.width + 'px';
                        if (el.parentNode) {
                            el.parentNode.replaceChild(placeholder, el);
                        }
                    }
                });
            }
            
            // 3. Clear innerHTML of hidden elements 
            const hiddenElements = document.querySelectorAll('[style*="display:none"], [style*="display: none"], [hidden]');
            hiddenElements.forEach(el => {
                el.innerHTML = '';
            });
            
            // 4. Remove event listeners (can cause memory leaks)
            const allElements = document.querySelectorAll('*');
            allElements.forEach(el => {
                el.onclick = null;
                el.onmouseover = null;
                el.onmouseout = null;
            });
            
            // Force garbage collection if possible
            if (window.gc) {
                window.gc();
            }
            
            return links;
        """)

    def check_and_click_load_more(self):
        """
        Check for and click load more buttons using the four patterns.
        Returns True if a button was clicked and content was loaded, False otherwise.
        """
        try:
            # Try the four selectors
            selectors = [
                "[onclick*='loadMore']",           # Original
                "button.ant-btn.ant-btn-primary.w-fit",  # Pattern 1
                "button.load__moreGrid",           # Pattern 2  
                "a.td_ajax_load_more", 
                "button#btnLoadMore"
            ]
            
            element = None
            for selector in selectors:
                try:
                    element = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    break
                except (TimeoutException, NoSuchElementException):
                    continue
            
            if not element:
                return False
            
           
            # Get current page height before clicking
            try:
                height_before = self.safe_execute_script("return document.body.scrollHeight")
            except Exception:
                height_before = 0
            
            # Scroll to element
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
            time.sleep(1)
            
            # Try to click
            click_successful = False
            try:
                element.click()
                click_successful = True
            except ElementClickInterceptedException:
                # Try JavaScript click
                self.driver.execute_script("arguments[0].click();", element)
                click_successful = True
            
            if click_successful:
                # Wait for potential content to load
                time.sleep(1.5)
                
                # Check if new content was actually loaded
                try:
                    height_after = self.safe_execute_script("return document.body.scrollHeight")
                    
                    if height_after > height_before:
                        logger.info("Successfully clicked loadMore button - new content loaded")
                        return True
                    else:
                        logger.info("LoadMore button clicked but no new content loaded - button may be inactive")
                        return False
                        
                except Exception as e:
                    logger.info(f"Error checking height after loadMore click: {e}")
                    # Assume content was loaded if we can't check
                    return True
                
        except (TimeoutException, NoSuchElementException):
            # No loadMore button found
            return False
        except Exception as e:
            logger.info(f"Error clicking loadMore button: {e}")
            return False

    def scroll_to_bottom(self, url, scroll_pause_time=1.0, max_scrolls=200):
        logger.info(f'Scrolling page: {url}')
        try:
            self.driver.get(url)
            time.sleep(2.5)  
            
      
            loadmore_clicked = False
            consecutive_failed_loadmore = 0
            max_failed_loadmore = 1
            
            while consecutive_failed_loadmore < max_failed_loadmore:
                loadmore_result = self.check_and_click_load_more()
                if loadmore_result:
                    logger.info("Successfully clicked loadMore button, content loaded")
                    loadmore_clicked = True
                    consecutive_failed_loadmore = 0 
                    time.sleep(1) 
                else:
                    consecutive_failed_loadmore += 1
                    logger.info(f"LoadMore attempt failed ({consecutive_failed_loadmore}/{max_failed_loadmore})")
                    if consecutive_failed_loadmore < max_failed_loadmore:
                        time.sleep(1.5) 
            
            if loadmore_clicked:
                logger.info("LoadMore phase completed, now starting scroll phase")
            else:
                logger.info("LoadMore phase ended after a failed attempt")
            
        except Exception as e:
            logger.info(f"Failed to load page: {e}")
            return None
        
        # Now get the initial height after loadMore clicks are done
        try:
            last_height = self.safe_execute_script("return document.body.scrollHeight")
        except Exception as e:
            logger.info(f"Error getting initial scroll height: {e}")
            return None
            
        scrolls_performed = 0
        scrolling_links = set()  # For deduplication
        
        # Now start the scrolling phase
        while True:
            if max_scrolls and scrolls_performed >= max_scrolls:
                logger.info(f"Reached maximum number of scrolls: {max_scrolls}")
                break
            
            try:
                # Scroll to the bottom of the page to load lazy content
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_pause_time)
                
                # Extract links and clean DOM
                links = self.extract_and_clear_dom()
                
                for link in links:
                    scrolling_links.add(link['outerHTML'])
                
                
               
                # Force Python garbage collection
                gc.collect()
                
            except Exception as e:
                logger.info(f"Error while processing <a> tags and cleaning DOM: {e}")
                # Continue to next scroll attempt
            
            try:
                new_height = self.safe_execute_script("return document.body.scrollHeight")
            except Exception as e:
                logger.info(f"Error while getting scroll height: {e}")
                break

            scrolls_performed += 1
            logger.info(f"Scroll #{scrolls_performed} - Height: {new_height}")
            
            if new_height == last_height:
                logger.info("Reached the bottom of the page")
                break
                
            last_height = new_height
        
        try:
            # Instead of returning the full page source (which could be huge),
            # just return a success indicator
            return True, list(scrolling_links)
        except Exception as e:
            logger.info(f"Error while finishing scroll operation: {e}")
            return None
     
    
    def check_and_click_clickable_page_element(self, page_num):
      
        try:
            
            # Try the four selectors
            selectors = [
                f"a[id='{page_num}']",
                "li.next a[href]"
            
            ]
            
            element = None
            for selector in selectors:
                try:
                    element = WebDriverWait(self.driver, 2).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    break
                except (TimeoutException, NoSuchElementException):
                    continue
            
            if not element:
                return False
        
            
            # Scroll to element
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
            time.sleep(1)
            
            # Try to click
            try:
                element.click()
                logger.info(f"Successfully clicked page {page_num}")
                time.sleep(1.5)  # Wait for page to load
                return True
            except ElementClickInterceptedException:
                # Try JavaScript click
                self.driver.execute_script("arguments[0].click();", element)
                logger.info(f"Successfully clicked page {page_num} using JavaScript")
                time.sleep(1.5)
                return True
                
        except (TimeoutException, NoSuchElementException) as e:
            logger.info(f"Failed to find/click page {page_num}: {e}")
            return False
        
            
    def pagination(self):      
        pagination_links = set()
        
        try:
          
            # Wait for page to load
            time.sleep(1.5)
            
            # Try clickable pagination first
            current_page = 0 
            clickable_pagination_worked = False
            
            while True: 
                current_page += 1  
                
                if not self.check_and_click_clickable_page_element(current_page):
                    logger.info(f"Could not navigate to page {current_page}, ending scraping")
                    break
                
                # Mark that clickable pagination is working
                if current_page > 1:
                    clickable_pagination_worked = True
                
                links = self.extract_and_clear_dom()
                new_links_added = 0
                for link in links:
                    if link['outerHTML'] not in pagination_links:
                        pagination_links.add(link['outerHTML'])
                        new_links_added += 1
                
                logger.info(f"Page {current_page}: Found {len(links)} total links, {new_links_added} new links")
                
                if new_links_added <= 3:
                            logger.info(f"Page {current_page}: No new links found, stopping pagination")
                            break

            logger.info(f"\nScraping completed. Processed {current_page} pages.")

            # If clickable pagination worked (more than 1 page processed), return results
            if clickable_pagination_worked:
                logger.info(f"Found {current_page} pages via clickable pagination, returning results")
                return True, list(pagination_links)
            else:
                logger.info("Only 1 page found via clickable pagination, continuing to URL-based pagination check")
            
            # Try URL-based pagination
            try:
                # Go back to first page to detect pagination links
                # self.driver.get(category_url)
                time.sleep(1)
                
                # Find total number of pages
                total_pages = 1
                try:
                    page_links = self.driver.find_elements(By.XPATH,  "//a[(contains(@href, 'cat=') and contains(@href, 'paged=')) or (contains(@href, 'category_id=') and contains(@href, 'page=')) or contains(@href, '/page/') or contains(@href, 'page=') or (contains(@href, 'per=') and contains(@href, 'p=')) ]")
                    
                    for link in page_links:
                        text = link.text.strip().replace(',', '')
                        if text.isdigit():
                            page_num = int(text)
                            if page_num > total_pages:
                                total_pages = page_num
                                
                    logger.info(f"Total pages detected: {total_pages}")

                except Exception as e:
                    logger.info(f"Error detecting pagination: {e}")
                    logger.info("Defaulting to 1 page")
                    total_pages = 1
                
        
                first_link = page_links[0].get_attribute('href')
                
                pattern = re.compile(r'(paged=|page[/=]|p=)(\d+)')
                
                if not pattern.search(first_link):
                    logger.info("First link doesn't contain pagination pattern, searching other links...")
                    
                    # Loop through links 0-10 to find one with pagination pattern
                    for i in range(min(10, len(page_links))):
                        try:
                            link = page_links[i].get_attribute('href')
                            if pattern.search(link):
                                first_link = link
                                logger.info(f"Found pagination pattern in link {i}: {link}")
                                break
                        except:
                            continue
                    else:
                        logger.info("No pagination pattern found in first 10 links")
                        return True, list(pagination_links)
                            
                if total_pages > 0 and total_pages < 31:
                    page_num = 2
                    while True:
                        page_url = pattern.sub(lambda m: f"{m.group(1)}{page_num}", first_link)
                        logger.info(f'URL: {page_url}')
                        logger.info(f"Navigating to page {page_num}: {page_url}")
                        
                        # Navigate to the next page
                        self.driver.get(page_url)
                        
                        # Wait for page to load
                        time.sleep(1.5)
                        
                        # Check for 404 or page not found
                        try:
                            # Check if page contains 404 indicators
                            page_source = self.driver.page_source.lower()
                            if ("page not found" in page_source or
                                "Oops! Something went wrong here." in page_source or 
                                self.driver.current_url != page_url):  # URL redirect might indicate 404
                                logger.info(f"Page {page_num}: 404 or page not found detected")
                                break
                        except Exception as e:
                            logger.info(f"Error checking page status: {e}")
                            break
                        
                        # Parse the page
                        links = self.extract_and_clear_dom()
                        
                        # Add new links and check if any were actually added
                        new_links_added = 0
                        for link in links:
                            link_html = link['outerHTML']
                            if link_html not in pagination_links:
                                pagination_links.add(link_html)
                                new_links_added += 1
                        
                        logger.info(f"Page {page_num}: Found {len(links)} total links, {new_links_added} new links added")
                    
                        # Break if no new links were added
                        if new_links_added < 5:
                            logger.info(f"Page {page_num}: No new links found, stopping pagination")
                            break
                        
                        # Move to next page
                        page_num += 1
                            
                    return True, list(pagination_links)
                        
                else:
                    for page_num in range(2, total_pages + 1):
                        try:
                            page_url = pattern.sub(lambda m: f"{m.group(1)}{page_num}", first_link)
                            logger.info(f'URL: {page_url}')
                            logger.info(f"Navigating to page {page_num}/{total_pages}: {page_url}")
                            
                            # Navigate to the next page
                            self.driver.get(page_url)
                            
                            # Wait for page to load
                            time.sleep(1.5)
                            
                            # Parse the page
                            links = self.extract_and_clear_dom()
                            for link in links:
                                pagination_links.add(link['outerHTML'])
                            logger.info(f"Page {page_num}: Found {len(links)} article links")
                                
                        except Exception as e:
                            logger.info(f"Error processing page {page_num}: {e}")
                            continue
                    
                    return True, list(pagination_links)
                            
            except Exception as e:
                logger.info(f"Error in URL-based pagination: {e}")
                return True, list(pagination_links)
                    
        except Exception as e:
            logger.info(f"Error in extract_links: {e}")
            return True, list(pagination_links)
            
        
        finally:
            self.driver.quit()

            
    def close(self):
        
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.info(f"Error closing driver: {e}")


