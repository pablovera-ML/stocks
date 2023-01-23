import pandas as pd

from datetime import datetime
import params
import time
import schedule
import utils
import yfinance as yfi

logger = utils.LOGGER
logger.info(f"Starting at {datetime.now()}...")
schema = 'stocks'
credentials = utils.get_secret()
logger.info("Creating DB engine")
logger.info("DB Engine created")

tickers_list = list(params.tickers.keys())

engine = utils.get_engine()


def pull_data_from_yfinance():
    with engine.connect() as connection:
        for interval in params.intervals:
            for ticker_name in tickers_list:
                utils.upsert_market_data(engine=engine,
                                         connection=connection,
                                         schema=schema,
                                         tickers=params.tickers,
                                         ticker_name=ticker_name,
                                         interval=interval)
        connection.close()


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
    pull_data_from_yfinance()
    """schedule.every(6).hours.do(pull_data_from_yfinance)
    while True:
        schedule.run_pending()
        time.sleep(1)
"""

