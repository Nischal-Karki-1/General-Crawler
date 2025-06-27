from status.logger import logger

async def create__url_relationship_table(conn):
    """Create table with url_id as primary key and url_path as unique constraint"""
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE SEQUENCE IF NOT EXISTS link_id_seq START 1;

                CREATE TABLE IF NOT EXISTS url_relationship (
                    link_id TEXT PRIMARY KEY DEFAULT 'link' || nextval('link_id_seq'),
                    domain_id TEXT NOT NULL,
                    parent_url_id TEXT NOT NULL,
                    child_url_id TEXT NOT NULL,  
                    parent_depth INTEGER NOT NULL, 
                    child_depth INTEGER NOT NULL,  
                    parent_link_text VARCHAR(500), 
                    discovered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (domain_id) REFERENCES seed_domain(domain_id) ON DELETE CASCADE,
                    FOREIGN KEY (parent_url_id) REFERENCES crawled_url(crawl_id) ON DELETE CASCADE,
                    FOREIGN KEY (child_url_id) REFERENCES crawled_url(crawl_id) ON DELETE CASCADE,
                    CONSTRAINT unique_parent_child UNIQUE (parent_url_id, child_url_id)
                );

                ALTER SEQUENCE link_id_seq OWNED BY url_relationship.link_id;

              
                SELECT setval('link_id_seq', 
                    GREATEST(
                        COALESCE(
                            (SELECT MAX(CAST(SUBSTRING(link_id FROM 5) AS INTEGER)) 
                            FROM url_relationship 
                            WHERE link_id ~ '^link[0-9]+$'), 
                            0
                        ), 
                        0
                    ) + 1,
                    false
                );

                CREATE INDEX IF NOT EXISTS idx_parent_url ON url_relationship(parent_url_id);
                CREATE INDEX IF NOT EXISTS idx_child_url ON url_relationship(child_url_id);
                CREATE INDEX IF NOT EXISTS idx_parent_depth ON url_relationship(parent_depth);
                CREATE INDEX IF NOT EXISTS idx_child_depth ON url_relationship(child_depth);
            """)
        
        await conn.commit()
        logger.info("Url-Relationship Table created successfully with primary key and unique constraints")
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error creating table: {e}")
        
        
async def insert_into_url_relationship_table(conn, domain_id, parent_url_id, child_url_id, parent_depth, child_depth, parent_link_text, discovered_at):
    try:
        async with conn.cursor() as cursor:
            # Explicitly generate the crawl_id by including it in the VALUES clause
            await cursor.execute("""
                INSERT INTO url_relationship (link_id, domain_id, parent_url_id, child_url_id, parent_depth, child_depth, parent_link_text, discovered_at )
                VALUES ('link' || nextval('link_id_seq'), %s, %s, %s, %s, %s, %s, %s)
            """, ( domain_id, parent_url_id, child_url_id, parent_depth, child_depth, parent_link_text, discovered_at))
                        
        await conn.commit()
        logger.info(f"Url relationship:{parent_url_id} -> {child_url_id} added to the table successfully")
        
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error inserting relation URL: {e}")
        raise