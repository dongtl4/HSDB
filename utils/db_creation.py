import psycopg2
from sqlalchemy import create_engine, text

DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

def ensure_database_exists(kb_config: dict):
    target_db = kb_config['dbname']
    # Connect to 'postgres' db to check/create target db
    root_url = f"postgresql+psycopg://{kb_config['user']}:{kb_config['password']}@{kb_config['host']}:{kb_config['port']}/postgres"
    engine = create_engine(root_url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            check_sql = text(f"SELECT 1 FROM pg_database WHERE datname='{target_db}'")
            exists = conn.execute(check_sql).fetchone()
            if not exists:
                create_sql = text(f'CREATE DATABASE "{target_db}"')
                conn.execute(create_sql)
        return True
    except Exception as e:
        # It's possible the DB already exists or connection failed; just proceed/raise
        raise e
    finally:
        engine.dispose()

def create_strict_hsdb():
    commands = [
        # =========================================================
        # LEVEL 1: GLOBAL ROOT (Macro Environment)
        # =========================================================
        # Global Economy State (Interest Rates, Geopolitics)
        
        # 1.1 Global Snapshots (The "Anchor" State)
        """
        CREATE TABLE IF NOT EXISTS global_snapshots (
            id SERIAL PRIMARY KEY,
            valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
            trigger_event VARCHAR(255), -- e.g., "FOMC Meeting"
            
            -- Stores macro vars: { "interest_rate": 5.5, "oil_price": 80.0 }
            data JSONB NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """,

        # 1.2 Global Deltas (The "Ledger" updates)
        """
        CREATE TABLE IF NOT EXISTS global_deltas (
            id SERIAL PRIMARY KEY,
            snapshot_id INTEGER REFERENCES global_snapshots(id),
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            source VARCHAR(255),
            delta_data JSONB NOT NULL -- Patch for the macro state
        );
        """,

        # =========================================================
        # LEVEL 2: SECTORS (GICS Hierarchy)
        # =========================================================
        # Sector Structure & Environment
        
        # 2.1 The GICS Tree Structure (Adjacency List)
        # This handles the 4 levels: Sector -> Industry Group -> Industry -> Sub-Industry
        """
        CREATE TABLE IF NOT EXISTS gics_nodes (
            id SERIAL PRIMARY KEY,
            parent_id INTEGER REFERENCES gics_nodes(id), -- The Link! NULL for top Sectors.
            name VARCHAR(255) NOT NULL,
            code VARCHAR(20) UNIQUE NOT NULL, -- GICS Code (e.g., 45102010)
            level_name VARCHAR(50) CHECK (level_name IN ('Sector', 'Group', 'Industry', 'SubIndustry')),
            description TEXT
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_gics_parent ON gics_nodes(parent_id);",

        # 2.2 Sector Snapshots
        # Stores state for ANY node in the tree (e.g., specific regulation for "Software")
        """
        CREATE TABLE IF NOT EXISTS sector_snapshots (
            id SERIAL PRIMARY KEY,
            gics_node_id INTEGER REFERENCES gics_nodes(id),
            valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
            trigger_event VARCHAR(255),
            data JSONB NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """,

        # 2.3 Sector Deltas
        """
        CREATE TABLE IF NOT EXISTS sector_deltas (
            id SERIAL PRIMARY KEY,
            snapshot_id INTEGER REFERENCES sector_snapshots(id),
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            delta_data JSONB NOT NULL
        );
        """,

        # =========================================================
        # LEVEL 3: ENTITIES (Companies)
        # =========================================================
        # Target Company & 7 Facets
        
        """
        CREATE TABLE IF NOT EXISTS entities (
            cik VARCHAR(10) PRIMARY KEY,
            ticker VARCHAR(20) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            basic_description TEXT, 
            primary_gics_id INTEGER REFERENCES gics_nodes(id) 
        );
        """,

        # 3.1.5 Multi-Sector Exposure 
        """
        CREATE TABLE IF NOT EXISTS entity_business_segments (
            id SERIAL PRIMARY KEY,
            entity_cik VARCHAR(10) REFERENCES entities(cik) ON DELETE CASCADE, 
            gics_node_id INTEGER REFERENCES gics_nodes(id),
            
            revenue_percent DECIMAL(5,2), 
            is_growing BOOLEAN DEFAULT TRUE, 
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(entity_cik, gics_node_id) 
        );
        """,

        # 3.2 Entity Facet Snapshots 
        """
        CREATE TABLE IF NOT EXISTS entity_facet_snapshots (
            id BIGSERIAL PRIMARY KEY,
            entity_cik VARCHAR(10) REFERENCES entities(cik) ON DELETE CASCADE,
            
            facet_name VARCHAR(50) CHECK (facet_name IN (
                'FINANCIAL_HEALTH', 'STRATEGIC_DIRECTION', 'MARKET_PRODUCT',
                'OPS_TECHNOLOGY', 'LEADERSHIP_ORG', 'STAKEHOLDER', 'EXTERNAL_ENV'
            )),
            
            valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
            trigger_event VARCHAR(255),
            
            data JSONB NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """,

        # 3.3 Entity Facet Deltas 
        """
        CREATE TABLE IF NOT EXISTS entity_facet_deltas (
            id BIGSERIAL PRIMARY KEY,
            snapshot_id BIGINT REFERENCES entity_facet_snapshots(id) ON DELETE CASCADE,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            delta_data JSONB NOT NULL
        );
        """
    ]

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("--- Building Strict HSDB Hierarchy ---")
        for cmd in commands:
            cur.execute(cmd)
        conn.commit()
        cur.close()
        conn.close()
        print("--- Schema Created Successfully ---")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    ensure_database_exists(DB_CONFIG)
    create_strict_hsdb()