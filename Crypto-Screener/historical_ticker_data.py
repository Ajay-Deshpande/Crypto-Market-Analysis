import pandas as pd
from yahooquery import Screener
import yfinance

def main():
    screener = Screener()
    screened_tickers = screener.get_screeners(['largest_market_cap_cryptocurrencies'], 250)['largest_market_cap_cryptocurrencies']['quotes']
    df = pd.DataFrame(screened_tickers)[['symbol', 'longName', 'shortName', 'marketCap']]


    data = yfinance.Tickers(df['symbol'].tolist()).download(period='5y')
    data = data.stack().reset_index().set_index(['Date'])
    data.columns.name = ""
    return data

if __name__ == "__main__":
    main()