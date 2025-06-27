from status.logger import logger
import datetime

async def create_crawled_url_table(conn):
    """Create table with url_id as primary key and url_path as unique constraint"""
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                
            CREATE SEQUENCE IF NOT EXISTS crawl_id_seq START 1;

            CREATE TABLE IF NOT EXISTS crawled_url (
                crawl_id TEXT PRIMARY KEY DEFAULT 'crawl' || nextval('crawl_id_seq'),
                domain_id TEXT NOT NULL,
                url_path TEXT NOT NULL,
                url_hash VARCHAR(64) NOT NULL,
                discovered_at_depth INTEGER NOT NULL, 
                crawl_status TEXT NOT NULL DEFAULT 'not_visited', 
                url_content VARCHAR(100),
                discovered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                crawled_at TIMESTAMP,
                FOREIGN KEY (domain_id) REFERENCES seed_domain(domain_id) ON DELETE CASCADE
            );

            ALTER SEQUENCE crawl_id_seq OWNED BY crawled_url.crawl_id;

       
            SELECT setval('crawl_id_seq', 
                GREATEST(
                    COALESCE(
                        (SELECT MAX(CAST(SUBSTRING(crawl_id FROM 6) AS INTEGER)) 
                        FROM crawled_url 
                        WHERE crawl_id ~ '^crawl[0-9]+$'), 
                        0
                    ), 
                    0
                ) + 1,
                false
            );

            CREATE INDEX IF NOT EXISTS idx_url_hash ON crawled_url(url_hash);
            CREATE INDEX IF NOT EXISTS idx_domain_depth ON crawled_url(domain_id, discovered_at_depth);
            CREATE INDEX IF NOT EXISTS idx_crawl_status ON crawled_url(crawl_status);
            CREATE INDEX IF NOT EXISTS idx_domain_status_depth ON crawled_url(domain_id, crawl_status, discovered_at_depth);

                                
            """)
        
        await conn.commit()
        logger.info("Crawled-Url Table created successfully with primary key and unique constraints")
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error creating table: {e}")
        

async def insert_into_crawled_url_table(conn, domain_id, url_path, url_hash, discovered_at_depth, url_content, crawled_at):
    try:
        async with conn.cursor() as cursor:
            # Explicitly generate the crawl_id by including it in the VALUES clause
            await cursor.execute("""
                INSERT INTO crawled_url (crawl_id, domain_id, url_path, url_hash, discovered_at_depth, crawl_status, url_content, discovered_at, crawled_at)
                VALUES ('crawl' || nextval('crawl_id_seq'), %s, %s, %s, %s, %s, %s, %s, %s)
            """, (domain_id, url_path, url_hash, discovered_at_depth, 'not_visited', url_content, datetime.datetime.now(), crawled_at))
                        
        await conn.commit()
        logger.info(f"Crawled Url:{url_path} info added to the table successfully")
        
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error inserting crawled URL: {e}")
        raise

async def fetch_crawled_url(conn):
    try:
        async with conn.cursor() as cursor:
            # Fix: Mark as 'in_progress' when fetching, not just update crawled_at
            await cursor.execute("""
                UPDATE crawled_url
                SET crawl_status = 'in_progress', crawled_at = NOW()
                WHERE crawl_id = (
                    SELECT crawl_id
                    FROM crawled_url
                    WHERE crawl_status IN ('not_visited', 'in_progress')
                    ORDER BY  discovered_at_depth ASC, crawl_id ASC  
                    LIMIT 1
                 
                )
                RETURNING url_path, domain_id, discovered_at_depth, crawl_id;
            """)
            
            result = await cursor.fetchone()
            await conn.commit()
            
            return result
    except Exception as e:
        logger.error(f"Error fetching and claiming domain: {e}")
        await conn.rollback()
        return None
    
async def update_crawled_url_status(conn,url_path, status): 
    try:
        async with conn.cursor() as cursor:
            
            await cursor.execute(
            """
                    UPDATE crawled_url
                    SET crawl_status = %s, crawled_at = NOW()
                    WHERE url_path = %s
            """, (status, url_path))
        
        await conn.commit()
        logger.info(f"Updated URL ID {url_path} status to {status}")
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error updating status for URL ID {url_path}: {e}") 
        
        
async def update_unique_links(conn, domain_id):
    try:
        async with conn.cursor() as cursor:
            # Get the distinct count of url_hash for this domain
            await cursor.execute("""
                SELECT COUNT(DISTINCT url_hash)
                FROM crawled_url
                WHERE domain_id = %s
            """, (domain_id,))
            
            result = await cursor.fetchone()
            unique_count = result[0] if result else 0
            
            # Update the seed_domain table with the unique count
            await cursor.execute("""
                UPDATE seed_domain
                SET total_urls_found = %s
                WHERE domain_id = %s
            """, (unique_count, domain_id))
            
        await conn.commit()
        
            
    except Exception as e:
        logger.error(f"Error updating unique links count: {e}")
        await conn.rollback()
        return None
    