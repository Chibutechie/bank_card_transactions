import pandas as pd
import os
from dotenv import load_dotenv

def extract_data():
    load_dotenv()
    api_url = os.getenv("API_URL")

    if not api_url:
        print("Error: API_URL not found in environment variables.")
        return None

    df = pd.read_parquet(api_url)
    return df

df = extract_data()

if df is not None:
    print(df.head(20))
