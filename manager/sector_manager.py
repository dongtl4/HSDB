import pandas as pd
import psycopg2

# Configuration matches your db_creation.py
DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def populate_gics_tree(csv_path="HSDB/GICS_Mappings.csv"):
    """
    Reads the GICS CSV and populates the gics_nodes table hierarchically.
    """
    print(f"--- Reading {csv_path} ---")
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: File {csv_path} not found.")
        return

    # Ensure all codes are treated as strings (e.g. 10 instead of 10.0)
    # The GICS structure is strict:
    # Sector (2 digits), Group (4), Industry (6), Sub-Industry (8)
    code_cols = ['Sector Code', 'Industry Group Code', 'Industry Code', 'Sub-Industry Code']
    for col in code_cols:
        df[col] = df[col].astype(str)

    conn = connect_db()
    cur = conn.cursor()

    # We use a cache to store { gics_code: database_id }
    # This avoids querying the DB repeatedly to find parent_ids.
    node_cache = {}

    try:
        print("--- Starting GICS Population ---")

        # =========================================================
        # LEVEL 1: SECTORS
        # =========================================================
        print("Processing Sectors...")
        sectors = df[['Sector Code', 'Sector']].drop_duplicates()
        
        for _, row in sectors.iterrows():
            code = row['Sector Code']
            name = row['Sector']
            
            # Upsert (Insert or Update if exists) and return ID
            cur.execute("""
                INSERT INTO gics_nodes (name, code, level_name, description, parent_id)
                VALUES (%s, %s, 'Sector', %s, NULL)
                ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
                RETURNING id;
            """, (name, code, name)) # Description = Name for Sector
            
            node_id = cur.fetchone()[0]
            node_cache[code] = node_id

        # =========================================================
        # LEVEL 2: INDUSTRY GROUPS
        # =========================================================
        print("Processing Industry Groups...")
        groups = df[['Industry Group Code', 'Industry Group', 'Sector Code']].drop_duplicates()
        
        for _, row in groups.iterrows():
            code = row['Industry Group Code']
            name = row['Industry Group']
            parent_code = row['Sector Code']
            parent_id = node_cache.get(parent_code)

            if parent_id:
                cur.execute("""
                    INSERT INTO gics_nodes (name, code, level_name, description, parent_id)
                    VALUES (%s, %s, 'Group', %s, %s)
                    ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
                    RETURNING id;
                """, (name, code, name, parent_id)) # Description = Name
                
                node_id = cur.fetchone()[0]
                node_cache[code] = node_id
            else:
                print(f"Warning: Parent Sector {parent_code} not found for Group {code}")

        # =========================================================
        # LEVEL 3: INDUSTRIES
        # =========================================================
        print("Processing Industries...")
        industries = df[['Industry Code', 'Industry', 'Industry Group Code']].drop_duplicates()
        
        for _, row in industries.iterrows():
            code = row['Industry Code']
            name = row['Industry']
            parent_code = row['Industry Group Code']
            parent_id = node_cache.get(parent_code)

            if parent_id:
                cur.execute("""
                    INSERT INTO gics_nodes (name, code, level_name, description, parent_id)
                    VALUES (%s, %s, 'Industry', %s, %s)
                    ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
                    RETURNING id;
                """, (name, code, name, parent_id)) # Description = Name
                
                node_id = cur.fetchone()[0]
                node_cache[code] = node_id
            else:
                print(f"Warning: Parent Group {parent_code} not found for Industry {code}")

        # =========================================================
        # LEVEL 4: SUB-INDUSTRIES
        # =========================================================
        print("Processing Sub-Industries...")
        # Note: Description comes from 'Definition' column here
        sub_industries = df[['Sub-Industry Code', 'Sub-Industry', 'Definition', 'Industry Code']].drop_duplicates()
        
        for _, row in sub_industries.iterrows():
            code = row['Sub-Industry Code']
            name = row['Sub-Industry']
            desc = row['Definition'] # Description = Definition Column
            parent_code = row['Industry Code']
            parent_id = node_cache.get(parent_code)

            if parent_id:
                cur.execute("""
                    INSERT INTO gics_nodes (name, code, level_name, description, parent_id)
                    VALUES (%s, %s, 'SubIndustry', %s, %s)
                    ON CONFLICT (code) DO UPDATE SET 
                        name = EXCLUDED.name,
                        description = EXCLUDED.description
                    RETURNING id;
                """, (name, code, desc, parent_id))
            else:
                print(f"Warning: Parent Industry {parent_code} not found for Sub-Industry {code}")

        conn.commit()
        print("--- GICS Tree Population Complete ---")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # Ensure this script is in the same folder as 'GICS_Mappings.csv'
    populate_gics_tree()