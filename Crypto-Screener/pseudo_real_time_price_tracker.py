
def main():
    import pandas as pd
    from dotenv import load_dotenv
    load_dotenv()
    import os
    import yahooquery
    from ta import add_all_ta_features
    from datetime import datetime

    volume_cols = set(['volume_obv', 'volume_vwap', 'volume_mfi'])
    volatility_cols = set(['volatility_atr', 'volatility_bbw'])
    trend_cols = set(['trend_macd', 'trend_macd_signal', 'trend_ema_fast', 'trend_ema_slow', 'trend_adx'])
    momentum_cols = set(['momentum_rsi', 'momentum_stoch', 'momentum_roc', 'momentum_ao'])
    other_ind_cols = set(['others_dr'])

    df = pd.read_csv(f"s3://{os.getenv('TICKER_SNAPSHOT_BUCKET')}/{os.getenv('TICKER_SNAPSHOT_PRICE_FILE')}")
    ticks = yahooquery.Ticker(df.symbol.unique().tolist())
    today_df = ticks.history(period='1d', interval='1d')
    df = pd.concat([df, today_df])
    df['date'] = pd.to_datetime(df['date'])
    
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
    
    df['date'] = pd.to_datetime(df['date'].apply(datetime.strftime, args=("%Y-%m-%d", ) ))


    print(df.isna().sum())
main()