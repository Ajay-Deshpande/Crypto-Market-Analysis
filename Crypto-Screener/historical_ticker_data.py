import argparse

def main(args):
    import os
    from dotenv import load_dotenv
    load_dotenv()

    import pandas as pd
    from sqlalchemy import create_engine
    import yfinance

    db_name = args.database_name if args.database_name else os.getenv("POSTGRES_DB")
    db_user = args.username if args.username else os.getenv("POSTGRES_USER")
    db_pass = args.password if args.password else os.getenv("POSTGRES_USER")
    db_host = args.host if args.host else os.getenv("HOST", "localhost")

    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}")
    df = pd.read_sql(os.getenv("TICKER_UNIVERSE_TABLE"), engine)

    data = yfinance.Tickers(df['ticker'].tolist()).download(period='5y')
    data = data.stack().reset_index().set_index(['Date'])
    data.columns.name = ""
    data.columns = data.columns.str.lower()
    data.index.name = data.index.name.lower()
    
    data.to_sql(os.getenv("ALL_TICKER_DATA"), engine, if_exists="replace")
    
    print(f"Found {data.ticker.nunique()} tickers out of {df.ticker.nunique()}")
    print("Successfully Inserted Records")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database_name")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--host")
    parser.add_argument("--port")

    args = parser.parse_args()
    main(args)