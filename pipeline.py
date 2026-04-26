import pandas as pd
import os
import logging
from sqlalchemy import create_engine

# =========================
# PATH SETUP
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

os.makedirs(LOG_DIR, exist_ok=True)

# =========================
# LOGGING SETUP
# =========================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)

logging.info("Pipeline started")

# =========================
# LOAD CSV FILES
# =========================
files = os.listdir(DATA_DIR)
df_list = []

for file in files:
    if file.endswith(".csv"):
        file_path = os.path.join(DATA_DIR, file)
        temp_df = pd.read_csv(file_path)
        df_list.append(temp_df)

df = pd.concat(df_list, ignore_index=True)
logging.info(f"Loaded {len(df)} rows")

# =========================
# CLEANING FUNCTION
# =========================
def clean_data(df):

    df = df.copy()

    # column cleanup
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # blanks → NA
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # string columns
    object_cols = ['company', 'location', 'industry', 'stage', 'country']

    for col in object_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # country cleaning (your logic)
    if 'country' in df.columns:
        df['country'] = df['country'].str.replace(
            r"[^a-zA-Z0-9 ]", "", regex=True
        )

    # numeric columns
    df['total_laid_off'] = pd.to_numeric(df['total_laid_off'], errors='coerce').astype('Int64')
    df['percentage_laid_off'] = pd.to_numeric(df['percentage_laid_off'], errors='coerce').round(2)
    df['funds_raised_millions'] = pd.to_numeric(df['funds_raised_millions'], errors='coerce').round(2)

    # date
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # mappings
    location_map = {'Dusseldorf': 'Düsseldorf', 'Malmo': 'Malmö'}
    industry_map_fix = {'Crypto': 'Crypto Currency', 'CryptoCurrency': 'Crypto Currency'}

    if 'location' in df.columns:
        df['location'] = df['location'].replace(location_map)

    if 'industry' in df.columns:
        df['industry'] = df['industry'].replace(industry_map_fix)

    # industry fill
    if 'industry' in df.columns and 'company' in df.columns:
        industry_map = (
            df.dropna(subset=['industry'])
              .drop_duplicates('company')
              .set_index('company')['industry']
        )

        df['industry'] = df['industry'].fillna(df['company'].map(industry_map))

    # rules
    df = df.dropna(subset=['total_laid_off', 'percentage_laid_off'], how='all')

    df = df[
        df['percentage_laid_off'].isna() |
        df['percentage_laid_off'].between(0, 1)
    ]

    df = df[
        df['funds_raised_millions'].isna() |
        (df['funds_raised_millions'] >= 0)
    ]

    df = df.drop_duplicates()

    logging.info("Cleaning completed")

    return df


# =========================
# RUN PIPELINE
# =========================
try:
    df = clean_data(df)
    logging.info(f"Final rows after cleaning: {len(df)}")

    # =========================
    # EXPORT TO MYSQL
    # =========================
    engine = create_engine("mysql+pymysql://root:YOUR_PASSWORD@localhost:3306/world_layoffs")

    df.to_sql(
        "layoffs_clean",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000
    )

    logging.info("Export successful")

except Exception as e:
    logging.error(f"Pipeline failed: {str(e)}")
    raise