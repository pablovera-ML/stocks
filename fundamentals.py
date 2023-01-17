import pandas as pd

from datetime import datetime
import params
import time
import schedule
import utils
import warnings
warnings.filterwarnings('ignore')

logger = utils.LOGGER
logger.info(f"Starting at {datetime.now()}...")
schema = 'traditional_finance'
credentials = utils.get_secret()
logger.info("Creating DB engine")
logger.info("DB Engine created")

engine = utils.get_engine()


def pull_fundamentals_from_macrotrends():
    base_url = 'https://www.macrotrends.net/stocks/charts'
    data = pd.read_sql("select symbol, macrotrend_name from fundamentals.macrotrend_symbol_names order by symbol asc",
                       con=engine)
    total = data[data.symbol > 'AB'].shape[0] * 1.0
    i = 0.0
    for _, row in data[data.symbol > 'AB'].iterrows():
        symbol = row['symbol']
        print(symbol)
        macrotrend_name = row['macrotrend_name']
        r = utils.collect_ratios(f"{base_url}/{symbol}/{macrotrend_name}/", params.ratios)
        r['symbol'] = symbol
        r.to_sql(f'{symbol.lower()}_fundamentals',
                 schema='fundamentals',
                 index=True,
                 index_label='Date',
                 if_exists='replace',
                 con=engine)
        print(f"{symbol} Done. {int(i + 1)} out of {int(total + 1)} ({round((100.0 * i / total), 2)}%)")
        i += 1.0


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """schedule.every().minute.do(pull_fundamentals_from_macrotrends)
    while True:
        schedule.run_pending()
        time.sleep(1)
"""
    pull_fundamentals_from_macrotrends()

