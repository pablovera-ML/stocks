import argparse
import params
import utils

argParser = argparse.ArgumentParser()
argParser.add_argument("-s", "--symbol", help="symbol to pull data", required=True)
argParser.add_argument("-i", "--interval", help="market data interval", required=True)
argParser.add_argument("-d", "--date", help="starting date", required=False)

if __name__ == '__main__':
    script_parameters = argParser.parse_args()
    symbol = script_parameters.symbol
    interval = script_parameters.interval
    date = script_parameters.date
    engine = utils.get_engine()
    schema = 'stocks'
    download_params = {  # tickers list or string as well
        'tickers': symbol,
        'start': date,
        # fetch data by interval (including intraday if period < 60 days)
        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        # (optional, default is '1d')
        'interval': interval,
        # group by ticker (to access via data['SPY'])
        # (optional, default is 'column')
        'group_by': 'ticker',
        # adjust all OHLC automatically
        # (optional, default is False)
        'auto_adjust': False,
        # download pre/post regular market hours data
        # (optional, default is False)
        'prepost': False,
        # use threads for mass downloading? (True/False/Integer)
        # (optional, default is True)
        'threads': False,
        # proxy URL scheme use use when downloading?
        # (optional, default is None)
        'proxy': None}
    table_name = f"{params.tickers[symbol]}_{interval}"
    with engine.connect() as connection:
        connection.execute(f"drop table if exists {schema}.{table_name}")
        utils.create_table(connection=connection, schema=schema, table_name=table_name)
        utils.download_and_insert_data(engine=engine,
                                       connection=connection,
                                       schema=schema,
                                       table_name=table_name,
                                       tickers=params.tickers,
                                       ticker_name=symbol,
                                       download_params=download_params)
