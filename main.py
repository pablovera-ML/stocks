import pandas as pd

from datetime import datetime
import params
import time
import schedule
import utils
import yfinance as yfi

logger = utils.LOGGER
logger.info(f"Starting at {datetime.now()}...")
schema = 'traditional_finance'
credentials = utils.get_secret()
logger.info("Creating DB engine")
logger.info("DB Engine created")

tickers_list = list(params.tickers.keys())

engine = utils.get_engine()


def pull_data_from_yfinance():
    for interval in params.intervals:
        for ticker_name in tickers_list:
            print(f"Downloading data for {ticker_name} and interval {interval}")
            data = yfi.download(  # or pdr.get_data_yahoo(...
                # tickers list or string as well
                tickers=ticker_name,

                # use "period" instead of start/end
                # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
                # (optional, default is '1mo')
                period="max",

                # fetch data by interval (including intraday if period < 60 days)
                # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
                # (optional, default is '1d')
                interval=interval,

                # group by ticker (to access via data['SPY'])
                # (optional, default is 'column')
                group_by='ticker',

                # adjust all OHLC automatically
                # (optional, default is False)
                auto_adjust=False,

                # download pre/post regular market hours data
                # (optional, default is False)
                prepost=False,

                # use threads for mass downloading? (True/False/Integer)
                # (optional, default is True)
                threads=True,

                # proxy URL scheme use use when downloading?
                # (optional, default is None)
                proxy=None
            )
            print("Creating table in DB")
            ticker_ohlc = data
            ticker_ohlc.columns = list(map(lambda x: x.lower(), ticker_ohlc.columns.to_list()))
            table_name = f"{params.tickers[ticker_name]}_{interval}"
            ticker_ohlc.dropna().to_sql(table_name,
                                        schema=schema,
                                        index_label='date',
                                        if_exists='replace',
                                        con=engine)
            print(f"Table {table_name} created!")


def pull_data_from_alphavantage():
    def insert_earnings(earnings_data, table_name):
        earnings_data.replace({'None': None}, inplace=True)
        earnings_data.rename(columns=params.rename_columns, inplace=True)
        earnings_data.to_sql(table_name,
                             schema=schema,
                             index=False,
                             if_exists='replace',
                             dtype=params.column_types,
                             con=engine)

    for ticker_name in list(params.earnings_tickers.keys()):
        print(f"Creating earnings tables for {ticker_name} in DB")
        symbol_name = params.earnings_tickers[ticker_name]
        data = utils.get_alphavantage_earnings(ticker_name, credentials['alphavantage_api_key'])
        if data:
            insert_earnings(pd.DataFrame.from_dict(data['quarterlyEarnings']), f'{symbol_name}_quarterly_earnings')
            logger.info(f"Table {symbol_name}_quarterly_earnings created!")

            insert_earnings(pd.DataFrame.from_dict(data['annualEarnings']), f'{symbol_name}_annual_earnings')
            logger.info(f"Table {symbol_name}_annual_earnings created!")
            time.sleep(10)
        else:
            print(f"Passing on {ticker_name}")
            pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """schedule.every(6).hours.do(pull_data_from_yfinance)
    schedule.every(6).hours.do(pull_data_from_alphavantage)
    while True:
        schedule.run_pending()
        time.sleep(1)"""
    pull_data_from_yfinance()


