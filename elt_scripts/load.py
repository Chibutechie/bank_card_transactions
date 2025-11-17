from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

def load_data():
    load_dotenv()
    connection_string = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

    engine = create_engine(connection_string)

    df = pd.read_parquet("API_URL")

    df = pd.read_csv("nigerian_card_transactions.csv")

    df.to_sql(
        'card_transactions'
        engine,
        if_exists='replace',
        index=False,
        chunksize=500000
    )

    print(f"Successfully loaded {len(df)} rows!")