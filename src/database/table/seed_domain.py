from status.logger import logger
import datetime
import json

async def create_seed_domain_table(conn):
    """Create table with url_id as primary key and url_path as unique constraint"""
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                DROP SEQUENCE IF EXISTS domain_id_seq CASCADE;
                CREATE SEQUENCE domain_id_seq START 1;
                                 
                CREATE TABLE IF NOT EXISTS seed_domain (
                        domain_id TEXT PRIMARY KEY DEFAULT 'domain' || nextval('domain_id_seq'),
                        domain VARCHAR(255) NOT NULL UNIQUE,
                        created_at TIMESTAMP NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending', 
                        max_depth INTEGER NOT NULL DEFAULT 5,
                        current_depth INTEGER NOT NULL DEFAULT 0,
                        started_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        total_urls_found INTEGER DEFAULT 0
                    );
                        
                CREATE INDEX IF NOT EXISTS idx_domain_status ON seed_domain(status);
            """)
        
        await conn.commit()
        logger.info("Seed-Domain Table created successfully with primary key and unique constraints")
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error creating table: {e}")
        
        
async def insert_into_seed_domain_table(conn):
    with open('assests/seed_domain.json', 'r') as f:
        data = json.load(f)
    
    domains = data.get('domains', [])
    if not domains:
        logger.warning("No domains found in seed_domain.json")
        return
    
    async with conn.cursor() as cursor:
        inserted_domains = []
        skipped_domains = []
        
        for domain in domains:
            await cursor.execute("""
                INSERT INTO seed_domain (domain_id, domain, created_at, status, max_depth, current_depth, started_at, completed_at, total_urls_found)
                VALUES ('domain' || nextval('domain_id_seq'), %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (domain) DO NOTHING
            """, (domain, datetime.datetime.now(), 'pending', 5, 0, None, None, 0))
            
            if cursor.rowcount > 0:
                inserted_domains.append(domain)
                logger.info(f"Inserted new domain: {domain}")
            else:
                skipped_domains.append(domain)
                logger.debug(f"Domain already exists, skipped: {domain}")
        
        await conn.commit()
        
        # Summary logging with domain names
        logger.info(f"Processed {len(domains)} domains total")
        logger.info(f"Inserted {len(inserted_domains)} new domains: {', '.join(inserted_domains)}")
        logger.info(f"Skipped {len(skipped_domains)} existing domains: {', '.join(skipped_domains)}")
            
      
async def fetch_domain_url(conn):
    try:
        async with conn.cursor() as cursor:
            # Atomically select one pending domain and mark it as in progress
            await cursor.execute("""
                UPDATE seed_domain 
                SET status = 'progessing', started_at = NOW()
                WHERE domain_id = (
                    SELECT domain_id
                    FROM seed_domain
                    WHERE status = 'progessing' 
                    ORDER BY domain_id DESC
                    LIMIT 1
                    
                )
                RETURNING domain_id, domain, max_depth;
            """)
            
            result = await cursor.fetchone()
            await conn.commit()
            
            return result
    except Exception as e:
        logger.error(f"Error fetching and claiming domain: {e}")
        await conn.rollback()
        return None


async def update_completed_at(conn, domain_id):
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                    UPDATE seed_domain
                    SET completed_at = NOW()
                    WHERE domain_id = %s
                             """, (domain_id,))
        await conn.commit()
            
            
    except Exception as e:
        logger.error(f"Error updating the completed_at timestamp: {e}")
        await conn.rollback()
        return None
    
async def update_status(conn, domain_id):
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                    UPDATE seed_domain
                    SET status = 'completed' 
                    WHERE domain_id = %s
                             """, (domain_id,))
        await conn.commit()
            
            
    except Exception as e:
        logger.error(f"Error updating the status of the domain: {e}")
        await conn.rollback()
        return None
    
async def update_depth(conn, depth, domain_id):
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                    UPDATE seed_domain
                    SET current_depth = %s 
                    WHERE domain_id = %s
                             """, (depth, domain_id,))
        await conn.commit()
            
            
    except Exception as e:
        logger.error(f"Error updating the status of the domain: {e}")
        await conn.rollback()
        return None