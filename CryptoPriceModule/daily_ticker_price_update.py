import argparse

def main(args):
    import warnings
    warnings.filterwarnings('ignore')

    import pandas as pd
    
    from dotenv import load_dotenv
    load_dotenv('../.env')
    
    import os
    import yahooquery
    from ta import add_all_ta_features
    from sqlalchemy import create_engine
    from datetime import datetime, timedelta

    volume_cols = set(['volume_obv', 'volume_vwap', 'volume_mfi'])
    volatility_cols = set(['volatility_atr', 'volatility_bbw'])
    trend_cols = set(['trend_macd', 'trend_macd_signal', 'trend_ema_fast', 'trend_ema_slow', 'trend_adx'])
    momentum_cols = set(['momentum_rsi', 'momentum_stoch', 'momentum_roc', 'momentum_ao'])
    other_ind_cols = set(['others_dr'])

    df = pd.read_csv(f's3://{os.getenv("TICKER_SNAPSHOT_BUCKET")}/{os.getenv("TICKER_SNAPSHOT_PRICE_FILE")}')
    print(pd.to_datetime(df['date']).max())
    last_working_day = datetime.today()
    # Find last working day that is completed
    last_working_day = (last_working_day - timedelta(days=int(last_working_day.weekday() == 6), hours=23))
    
    if datetime.strptime(df['date'].max(), "%Y-%m-%d") >= last_working_day:
        print("Today's records already updated")
        return 
    ticks = yahooquery.Ticker(df.symbol.unique().tolist())
    df = ticks.history(period='1mo', interval='1d').reset_index()
    
    df['date'] = pd.to_datetime(df['date'].apply(datetime.strftime, args=("%Y-%m-%d", ) ))
    
    df = df.groupby('symbol').apply(lambda x: add_all_ta_features(x.reset_index(drop=True).set_index('date').sort_index(),
                                open='open', high='high', low='low', close='close', volume='volume',\
                                volume_cols=volume_cols, volatility_cols=volatility_cols, trend_cols=trend_cols, \
                                momentum_cols=momentum_cols, other_ind_cols=other_ind_cols),
                          include_groups = False)

    df = df.reset_index().rename(columns = {'symbol' : 'ticker', 'others_dr' : 'daily_return',
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
    
    db_name = args.database_name if args.database_name else os.getenv("POSTGRES_DB")
    db_user = args.username if args.username else os.getenv("POSTGRES_USER")
    db_pass = args.password if args.password else os.getenv("POSTGRES_USER")
    db_host = args.host if args.host else os.getenv("HOST", "localhost")
    
    df['date'] = pd.to_datetime(df['date'])
    write_df = df.sort_values('date').groupby('ticker')
    write_df.tail(30).to_csv(f's3://{os.getenv("TICKER_SNAPSHOT_BUCKET")}/{os.getenv("TICKER_SNAPSHOT_PRICE_FILE")}', index=False)

    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}")    
    
    write_df.tail(1).to_sql(os.getenv("ALL_TICKER_DATA"), engine, if_exists="append", index=False)
    print(f"Inserted daily records for {df.ticker.nunique()} tickers")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database_name")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--host")
    parser.add_argument("--port")

    args = parser.parse_args()
    main(args)
