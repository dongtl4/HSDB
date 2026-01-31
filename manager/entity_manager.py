import pandas as pd
import psycopg2
import requests
import io

# Configuration
DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Fetch list of entity*sub-industry from wikipedia
# try:
#     headers = {
#             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
#         }
        
#     # 1. Get the HTML content using requests
#     response = requests.get(WIKI_URL, headers=headers)
#     response.raise_for_status() # Check for other HTTP errors
    
#     # 2. Pass the HTML string to pandas
#     dfs = pd.read_html(io.StringIO(response.text))
    
#     # The first table is usually the Constituents list
#     df = dfs[0]
#     print(f"--- Found {len(df)} companies ---")
#     df.to_csv('HSDB/SnP_GICS.csv')
        
# except Exception as e:
#     print(f"Error fetching Wikipedia data: {e}")

def check_gics_mismatches(df):
    """
    Scans the provided S&P 500 DataFrame for GICS Sub-Industries 
    that do not exist in the local database.
    """
    print("--- Checking for GICS Sub-Industry Mismatches ---")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 1. Fetch all valid Sub-Industries from your Database
    cur.execute("SELECT name FROM gics_nodes WHERE level_name = 'SubIndustry'")
    # Store as a set of normalized strings (lowercase, stripped) for fast comparison
    valid_sub_industries = {row[0].strip().lower() for row in cur.fetchall()}
    
    cur.close()
    conn.close()

    mismatches = {} # format: { 'Unknown Industry Name': ['TICKER1', 'TICKER2'] }

    # 2. Iterate through the Source DataFrame
    # Group by the sub-industry column to check each unique industry once
    if 'GICS Sub-Industry' not in df.columns:
        print("Error: DataFrame missing 'GICS Sub-Industry' column.")
        return

    # Grouping lets us see all tickers affected by one bad industry name
    grouped = df.groupby('GICS Sub-Industry')['Symbol'].apply(list)

    for industry_name, tickers in grouped.items():
        # Normalize the source name exactly how the loader script does
        lookup_key = str(industry_name).strip().lower()
        
        if lookup_key not in valid_sub_industries:
            mismatches[industry_name] = tickers

    # 3. Report Results
    if not mismatches:
        print("✅ SUCCESS: No mismatches found! All industries map correctly.")
    else:
        print(f"⚠️ FOUND {len(mismatches)} MISMATCHES:")
        print("-" * 60)
        print(f"{'Wiki Source Name':<35} | {'Affected Tickers'}")
        print("-" * 60)
        for name, tickers in mismatches.items():
            ticker_str = ", ".join(tickers[:5]) # Show first 5 tickers
            if len(tickers) > 5: ticker_str += f", ... (+{len(tickers)-5} more)"
            print(f"{name:<35} | {ticker_str}")
        print("-" * 60)
        print("Tip: Add these names to a 'patch_map' dictionary or update your GICS_Mappings.csv.")

# Add single entity to DB based on input data
def add_entity(cur, cik, ticker, name, basic_description, sub_industry_name):
    """
    Adds or updates a single entity.
    Optimized: Automatically fetches and caches all Sub-Industries on the first run 
    to handle 'mismatches' perfectly in Python.
    """
    # Check if we have the cache; if not, build it.
    if not hasattr(add_entity, "gics_cache"):
        print("Caching GICS Sub-Industries from DB... ")
        # Fetch everything once
        cur.execute("SELECT name, id FROM gics_nodes WHERE level_name = 'SubIndustry'")
        rows = cur.fetchall()
        # Build the map: { 'normalized_name': id }
        add_entity.gics_cache = {row[0].strip().lower(): row[1] for row in rows}
        print(f"Cached {len(add_entity.gics_cache)} Sub-Industries")

    # Look up 
    lookup_key = str(sub_industry_name).strip().lower()
    gics_id = add_entity.gics_cache.get(lookup_key)

    if not gics_id:
        print(f"⚠️  Skipped {ticker}: Sub-Industry '{sub_industry_name}' not found in GICS cache.")
        return

    # 3. Perform the Insert/Update
    try:
        cur.execute("""
            INSERT INTO entities (cik, ticker, name, basic_description, primary_gics_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (cik) DO UPDATE SET 
                ticker = EXCLUDED.ticker,
                name = EXCLUDED.name,
                basic_description = EXCLUDED.basic_description,
                primary_gics_id = EXCLUDED.primary_gics_id;
        """, (cik, ticker, name, basic_description, gics_id))
        print(f"✅ Saved: {name} (CIK: {cik})")
    except Exception as e:
        print(f"❌ DB Error for {ticker} (CIK: {cik}): {e}")

df = pd.read_csv('HSDB/SnP_GICS.csv', header=0, index_col=0)
df['CIK'] = df['CIK'].astype(str).str.zfill(10)
check_gics_mismatches(df)

with psycopg2.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:
        for i in range(len(df)):
            cik = df['CIK'][i]
            ticker = df['Symbol'][i]
            name = df['Security'][i]
            basic_description = f"Founded: {df['Founded'][i]}; Headquarters Location: {df['Headquarters Location'][i]}"
            sub_industry_name = df['GICS Sub-Industry'][i]

            add_entity(cur, cik, ticker, name, basic_description, sub_industry_name)
