from sqlalchemy import create_engine
from datetime import datetime
from itertools import product
import utils

# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logger = utils.LOGGER
    logger.info("Starting...")
    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     logger.info(e)

    #     raise e
    schema = 'binance'

    credentials = utils.get_secret()
    logger.info("Creating DB engine")
    db_uri = utils.get_db_url(host=credentials['host'],
                              database='timeseries',
                              user_name=credentials['username'],
                              password=credentials['password'],
                              port=5432)

    engine = create_engine(db_uri, echo=False)
    logger.info("DB Engine created")
    pairs = {"btcusdt": "2017-08-18",
             "bnbusdt": "2017-11-07",
             "ethusdt": "2017-08-18",
             "adausdt": "2018-04-18",
             "dotusdt": "2019-08-19",
             "ltcusdt": "2017-12-12",
             "yfiusdt": "2020-08-11",
             "filusdt": "2020-10-13",
             "dogeusdt": "2019-07-02",
             "xrpusdt": "2018-05-01",
             "solusdt": "2020-08-11",
             "chzusdt": "2020-09-01",
             "ksmusdt": "2020-09-01"}

    intervals = ["1m", "15m", "1h", "4h", "1d", "1w"]
    pairs_intervals = list(product(list(pairs.keys()), intervals))

    with engine.connect() as con:
        for pair, interval in pairs_intervals:
            table_name = f"{pair}_{interval}"

            if not utils.check_table_existence(connection=con, schema=schema, table_name=table_name):
                logger.info(f"Table {schema}.{table_name} does not exist.")
                start_time = pairs[pair]
                start_time_miliseconds = str(utils.convert_date_to_timestamp_miliseconds(start_time))
                utils.create_table(connection=con,
                                   schema=schema,
                                   table_name=table_name)
            else:
                logger.info(f"Table {schema}.{table_name} already exists.")
                start_time_miliseconds = utils.get_max_timestamp_miliseconds(connection=con,
                                                                             schema=schema,
                                                                             table_name=table_name)

            previous_start_time = 0

            while previous_start_time < int(start_time_miliseconds):
                previous_start_time = int(start_time_miliseconds)
                klines_df = utils.get_klines(pair, interval, start_time_miliseconds)
                utils.insert_klines(engine=engine, connection=con, klines=klines_df, schema=schema,
                                    table_name=table_name)
                start_time_miliseconds = klines_df.open_time.max()
                previous_date = datetime.fromtimestamp(int(klines_df.open_time.min()) / 1000.0)
                start_date = datetime.fromtimestamp(int(klines_df.open_time.max()) / 1000.0)
                logger.info(f"Data range from {previous_date} to {start_date}")

