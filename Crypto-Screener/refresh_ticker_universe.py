import argparse

def main(args):
        import warnings
        warnings.filterwarnings('ignore')
        
        import os
        from dotenv import load_dotenv
        load_dotenv()

        import psycopg2
        from crypto_utility.database_utility import DatabaseUtility
        from sqlalchemy import create_engine


        db_name = args.database_name if args.database_name else os.getenv("POSTGRES_DB")
        db_user = args.username if args.username else os.getenv("POSTGRES_USER")
        db_pass = args.password if args.password else os.getenv("POSTGRES_USER")
        db_host = args.host if args.host else os.getenv("HOST", "localhost")
        db_port = args.port if args.password else os.getenv("PORT", "5432")
        
        connection = psycopg2.connect(user=db_user, password=db_pass, host=db_host, port=db_port)
        cursor = connection.cursor()
        cursor.execute(f""" SELECT * FROM pg_catalog.pg_database WHERE datname = '{db_name}';""")
        if not cursor.fetchone():
                db_helper = DatabaseUtility(username=db_user, password=db_pass, host=db_host, port=db_port)
                db_helper.create_database(db_name)
        cursor.close()
        connection.close()
        engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}")



        import pandas as pd
        from yahooquery import Screener, Ticker

        screener = Screener()
        screen_tickers = screener.get_screeners(['all_cryptocurrencies_us'], args.number_of_ticker)

        df = pd.DataFrame(screen_tickers['all_cryptocurrencies_us']['quotes'])[['symbol', 'longName', 'shortName', 'marketCap']]
        tickers = Ticker(df.symbol.tolist())
        asset_profile = pd.DataFrame.from_dict(tickers.asset_profile).T[['startDate', 'description']]
        asset_profile = asset_profile[asset_profile['startDate'] != 'No fundamentals data found for any of the summaryTypes=assetProfile']
        asset_profile['startDate'] = pd.to_datetime(asset_profile['startDate'])
        df = df.merge(asset_profile, left_on = 'symbol', right_index=True)
        df = df.rename({'symbol' : 'ticker', 'longName' : 'coin_full_name',
                        'shortName' : 'coin_name', 'marketCap' : "market_cap",
                        'startDate' : 'start_date'}, axis = 1)
        df.to_sql(os.getenv("TICKER_UNIVERSE_TABLE"), engine, if_exists='replace', index = False)
        print("Successfully Inserted Records")

if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        parser.add_argument("--database_name")
        parser.add_argument("--username")
        parser.add_argument("--password")
        parser.add_argument("--host", default="localhost")
        parser.add_argument("--port", default="5432")
        parser.add_argument('--number_of_ticker', default="250")

        args = parser.parse_args()
        main(args)