import os
import argparse
from dotenv import load_dotenv
import psycopg2
from tradingview_screener import Query, col
from crypto_utility.database_utility import DatabaseUtility

load_dotenv()

def main(args):
        db_name = args.database_name if args.database_name else os.getenv("POSTGRES_USER")
        db_user = args.username if args.username else os.getenv("POSTGRES_USER")
        db_pass = args.password if args.password else os.getenv("POSTGRES_USER")
        db_host = args.host if args.username else os.getenv("HOST", "localhost")
        db_port = args.port if args.password else os.getenv("PORT", "5432")

        try:
                connection = psycopg2.connect(database=db_name, 
                                        user=db_user, password=db_pass, host=db_host, port=db_port)
        except Exception as e:
                if f"database {db_name} does not exist" in e:
                        db_helper = DatabaseUtility(username=db_user, password=db_pass, host=db_host, port=db_port)
                        db_helper.create_database(db_name)
                        connection = psycopg2.connect(database=db_name, 
                                                      user=db_user, 
                                                      password=db_pass, 
                                                      host=db_host, 
                                                      port=db_port)
                else:
                        raise e
        


        ticker_count, df = Query().select('name').where(col("market_cap_calc") >= 10 ** 8
                                                        ).set_markets('coin').get_scanner_data()

        print(ticker_count, " Tickers captured")
        print(df.head())

if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        parser.add_argument("--database_name")
        parser.add_argument("--username")
        parser.add_argument("--password")
        parser.add_argument("--host")
        parser.add_argument("--port")

        args = parser.parse_args()
        main(args)