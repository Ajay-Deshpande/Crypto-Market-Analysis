import argparse

def main(args):
    import warnings
    warnings.filterwarnings('ignore')
    
    import os
    from dotenv import load_dotenv
    load_dotenv('../.env')

    import pandas as pd
    from sqlalchemy import create_engine
    import yahooquery
    from ta import add_all_ta_features
    from datetime import datetime
    
    db_name = args.database_name if args.database_name else os.getenv("POSTGRES_DB")
    db_user = args.username if args.username else os.getenv("POSTGRES_USER")
    db_pass = args.password if args.password else os.getenv("POSTGRES_USER")
    db_host = args.host if args.host else os.getenv("HOST", "localhost")

    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}")
    df = pd.read_sql(os.getenv("TICKER_UNIVERSE_TABLE"), engine)
    # Filter in safe coins
    # Consider coins with trading volume more than 1M and the coin is old (1 year)
    df = df[(pd.Timestamp.today() - df['start_date']) > pd.Timedelta(366, 'day')]
    print("Screening Tickers... ", len(df.ticker.tolist()), " Tickers left")
    data = yahooquery.Ticker(df['ticker'].tolist()).history(period='max', interval='1d')
    data = data.reset_index()
    
    # Predefined set of indicators
    volume_cols = set(['volume_obv', 'volume_vwap', 'volume_mfi'])
    volatility_cols = set(['volatility_atr', 'volatility_bbw'])
    trend_cols = set(['trend_macd', 'trend_macd_signal', 'trend_ema_fast', 'trend_ema_slow', 'trend_adx'])
    momentum_cols = set(['momentum_rsi', 'momentum_stoch', 'momentum_roc', 'momentum_ao'])
    other_ind_cols = set(['others_dr'])

    symbols_1y = data.groupby('symbol').count()['date']
    symbols_1y = symbols_1y[symbols_1y < 365]
    data = data[~ data.symbol.isin(symbols_1y.index.tolist())]
    
    print("Removing Tickers with less than one year of data... ", data.symbol.nunique(), " Tickers left")

    data['date'] = pd.to_datetime(data['date'].apply(datetime.strftime, args=("%Y-%m-%d", ) ))

    date_range_last_30d = pd.date_range(end=pd.Timestamp.today(), freq='D', periods=29, normalize=True)
    symbols_last_30d = data.groupby('symbol').apply(lambda x: len(date_range_last_30d.difference(x.date.tolist())), include_groups=False)
    del_symbols = symbols_last_30d[symbols_last_30d > 1]
    data = data[~ data.symbol.isin(del_symbols.index.tolist())]
    print("Removing Tickers with data missing for last 30 days... ", data.symbol.nunique(), " Tickers left")

    last_30d = data.sort_values('date').groupby('symbol').tail(30)
    
    data = data.groupby('symbol').apply(lambda x: add_all_ta_features(x.reset_index(drop=True).set_index('date').sort_index(),
                                open='open', high='high', low='low', close='close', volume='volume',\
                                volume_cols=volume_cols, volatility_cols=volatility_cols, trend_cols=trend_cols, \
                                momentum_cols=momentum_cols, other_ind_cols=other_ind_cols),
                          include_groups = False)
    
    data = data.reset_index().rename(columns = {'symbol' : 'ticker', 'others_dr' : 'daily_return',
                                                    "volume_obv" : "volume_on_balance",
                                                    "volume_vwap" : "volume_weighted_avg_price",
                                                    "volume_mfi" : "volume_money_flow_index",
                                                    "volatility_atr" : "volatility_avg_true_range",
                                                    "volatility_bbw" : "volatility_bollinger_bands_width",
                                                    "trend_adx" : "trend_avg_directional_index",
                                                    "momentum_rsi" : "relative_strength_index",
                                                    "momentum_stoch" : "momentum_stochastic_oscillator",
                                                    "momentum_roc" : "momentum_rate_of_change",
                                                    "momentum_ao" : "momentum_awesome_oscillator",
                                            })

    import boto3
    client = boto3.resource('s3')
    bucket = client.Bucket(os.getenv("TICKER_SNAPSHOT_BUCKET"))
    if not bucket.creation_date:
        bucket.create()
    
    last_30d.to_csv(f's3://{os.getenv("TICKER_SNAPSHOT_BUCKET")}/{os.getenv("TICKER_SNAPSHOT_PRICE_FILE")}', index=False)
    
    data.to_sql(os.getenv("ALL_TICKER_DATA"), engine, if_exists="replace", index=False)
    
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