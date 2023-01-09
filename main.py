import pandas as pd
from sqlalchemy import create_engine
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
db_uri = utils.get_db_url(host=credentials['host'],
                          database='timeseries',
                          user_name=credentials['username'],
                          password=credentials['password'],
                          port=5432)
engine = create_engine(db_uri, echo=False)
logger.info("DB Engine created")

tickers_list = list(params.tickers.keys())


def pull_data_from_yfinance():
    for interval in params.intervals:
        print(f"Downloading data for interval {interval}")
        data = yfi.download(  # or pdr.get_data_yahoo(...
            # tickers list or string as well
            tickers=tickers_list,

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

        for ticker_name in tickers_list:
            print("Creating table in DB")
            ticker_ohlc = data[ticker_name]
            ticker_ohlc.columns = list(map(lambda x: x.lower(), ticker_ohlc.columns.to_list()))
            table_name = f"{params.tickers[ticker_name]}_{interval}"
            ticker_ohlc.dropna().to_sql(table_name,
                                        schema=schema,
                                        index_label='date',
                                        if_exists='replace',
                                        con=engine)
            print(f"Table {table_name} created!")


def pull_data_from_alphavantage():
    for ticker_name in tickers_list:
        try:
            print(f"Creating earnings tables for {ticker_name} in DB")
            data = utils.get_alphavantage_earnings(ticker_name, credentials['alphavantage_api_key'])
            quarterly_earnings = pd.DataFrame.from_dict(data['quarterlyEarnings'])
            annual_earnings = pd.DataFrame.from_dict(data['annualEarnings'])
            quarterly_earnings.to_sql(f"{params.tickers[ticker_name]}_quarterly_earnings",
                                      schema=schema,
                                      index_label='date',
                                      if_exists='replace',
                                      con=engine)
            logger.info(f"Table {params.tickers[ticker_name]}_quarterly_earnings created!")

            annual_earnings.to_sql(f"{params.tickers[ticker_name]}_annual_earnings",
                                   schema=schema,
                                   index_label='date',
                                   if_exists='replace',
                                   con=engine)
            logger.info(f"Table {params.tickers[ticker_name]}_annual_earnings created!")
        except:
            logger.info(f"passing on symbol {ticker_name}")
            pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    schedule.every(1).minutes.do(pull_data_from_yfinance)
    schedule.every(1).minutes.do(pull_data_from_alphavantage)
    while True:
        schedule.run_pending()
        time.sleep(1)

