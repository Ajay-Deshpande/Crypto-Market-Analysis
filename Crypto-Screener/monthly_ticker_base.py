from tradingview_screener import Query, col

ticker_count, df = Query().select('market_cap_calc', 'name'
                                  ).where(col("market_cap_calc") >= 10 ** 8
                                          ).set_markets('coin').get_scanner_data()

print(ticker_count, " Tickers captured")
print(df.head())