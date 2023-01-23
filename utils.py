import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

import yfinance as yfi

LOGGER = logging.getLogger()
if len(logging.getLogger().handlers) > 0:
    LOGGER.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)
LOGGER.info("Changing logging level")


def get_secret():
    load_dotenv()
    secret = {'host': os.environ['DATABASE_HOST'], 'password': os.environ['DATABASE_PASSWORD'],
              'username': os.environ['DATABASE_USERNAME'], 'alphavantage_api_key': os.environ['ALPHAVANTAGE_API_KEY']}
    return secret


def get_db_url(host, database, user_name, password, port):
    return "postgresql+psycopg2://{}:{}@{}:{}/{}".format(user_name, password, host, port, database)


def check_table_existence(connection, schema, table_name):
    return connection.execute(f"""SELECT EXISTS (
        SELECT FROM 
            pg_tables
        WHERE 
            schemaname = '{schema}' AND 
            tablename  = '{table_name}'
        );""").first()[0]


def get_max_timestamp_miliseconds(connection, schema, table_name, date_column_name):
    date = connection.execute(f"""select (max({date_column_name})-interval '1 day')::date 
                                      from {schema}.{table_name};""").first()[0]
    return str(date) if date else None


def convert_date_to_timestamp_miliseconds(date):
    return int(datetime.strptime(date, '%Y-%m-%d').timestamp() * 1000)


def get_alphavantage_earnings(symbol, api_key):
    url = f'https://www.alphavantage.co/query?function=EARNINGS&symbol={symbol}&apikey={api_key}'
    r = requests.get(url)
    return r.json()


def get_engine():
    credentials = get_secret()
    db_uri = get_db_url(host=credentials['host'],
                        database='timeseries',
                        user_name=credentials['username'],
                        password=credentials['password'],
                        port=5432)
    return create_engine(db_uri, echo=False)


def get_american_symbols():
    engine = get_engine()
    return pd.read_sql("""select symbol, internal_symbol 
                          from stocks.companies_info order by symbol asc""", con=engine)


def collect_ratios(path, ratios):
    dfs = []
    for ratio in ratios:
        try:
            df = pd.read_html(path + ratio, parse_dates=True)[0]
            df.columns = df.columns.droplevel()
            df['Date'] = pd.to_datetime(df.Date)
            df.set_index('Date', inplace=True)
            dfs.append(df)
        except:
            pass
        finally:
            pass

    ratios = pd.concat(dfs, axis=1)

    price = ratios['Stock Price'].values[:, 0]
    ratios = ratios.drop('Stock Price', axis=1)

    Current_Liabilities = ratios['Current Liabilities'].values[:, 0]
    ratios = ratios.drop('Current Liabilities', axis=1)

    equity = ratios["Shareholder's Equity"].values[:, 0]
    ratios = ratios.drop("Shareholder's Equity", axis=1)

    net_income = ratios["TTM Net Income"].values[:, 0]
    ratios = ratios.drop("TTM Net Income", axis=1)

    ratios['price'] = price
    ratios['Current Liabilities'] = Current_Liabilities
    ratios["Shareholder's Equity"] = equity
    ratios["TTM Net Income"] = net_income

    for c in ratios.columns:
        if pd.api.types.is_object_dtype(ratios[c].dtype):
            ratios[c] = ratios[c].str.replace('$', '').str.replace(',', '').str.replace('B', '').str.replace('%',
                                                                                                             '').astype(
                float)
    ratios.rename({'Long Term Debt': 'Total Liabilities'}, inplace=True, axis=1)
    ratios.columns = [c.lower().replace(' ', '_').replace('-', '_') for c in ratios.columns]

    return ratios


def create_table(connection, schema, table_name):
    connection.execute(f"""create table {schema}.{table_name}(
                                date timestamp,
                                open numeric,
                                high numeric,
                                low numeric,
                                close numeric,
                                volume numeric,
                                symbol text,
                                primary key (date),
                                unique(date)
                            )
                       """)
    print(f"Table {schema}.{table_name} created!")


def insert_klines(engine, connection, schema, table_name, klines):
    klines.to_sql(con=engine,
                  schema=schema,
                  name=f'staging_{table_name}',
                  index=False,
                  if_exists='replace')
    connection.execute(f"""
                        insert into {schema}.{table_name} 
                                    (
                                    date, 
                                    open, 
                                    high, 
                                    low, 
                                    close, 
                                    volume, 
                                    symbol
                                    )
                        select 
                                date::timestamp as date,
                                open::numeric as open,
                                high::numeric as high,
                                low::numeric as low,
                                close::numeric as close,
                                volume::numeric as volume,
                                symbol::text as symbol
                        from {schema}.staging_{table_name}
                        on conflict (date) do update SET
                                date = excluded.date,
                                open = excluded.open,
                                high = excluded.high,
                                low = excluded.low,
                                close = excluded.close,
                                volume = excluded.volume,
                                symbol = excluded.symbol;""")
    connection.execute(f"drop table {schema}.staging_{table_name};")


def download_and_insert_data(engine, connection, schema, table_name, download_params, tickers, ticker_name):
    data = yfi.download(**download_params)
    ticker_ohlc = data.reset_index()
    ticker_ohlc['symbol'] = tickers[ticker_name]
    ticker_ohlc.columns = list(map(lambda x: x.lower(), ticker_ohlc.columns.to_list()))
    ticker_ohlc.dropna().drop(columns=["adj close"], inplace=True)
    insert_klines(engine=engine,
                  connection=connection,
                  schema=schema,
                  table_name=table_name,
                  klines=ticker_ohlc)
    print(f"New data inserted in table {table_name}")


def upsert_market_data(engine, connection, schema, tickers, ticker_name, interval):
    table_name = f"{tickers[ticker_name]}_{interval}"
    print(f"Checking if table {table_name} exists...")
    table_exist = check_table_existence(connection=connection, schema=schema, table_name=table_name)
    start_date = None
    if table_exist:
        start_date = get_max_timestamp_miliseconds(connection=connection,
                                                   schema=schema,
                                                   table_name=table_name,
                                                   date_column_name='date')
        if start_date:
            print(f"Table {table_name} exists and last date is {start_date})")
        else:
            print(f"Table {table_name} exists but is empty")
    else:
        print(f"Table {table_name} does not exist. Creating table {schema}.{table_name}")
        create_table(connection=connection, schema=schema, table_name=table_name)

    download_params = {  # tickers list or string as well
        'tickers': ticker_name,
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
    download_params.update({'start': start_date}) if start_date else download_params.update({'period': "max"})
    print(f"Downloading data for {ticker_name} and interval {interval}")
    download_and_insert_data(engine=engine,
                             connection=connection,
                             schema=schema,
                             table_name=table_name,
                             download_params=download_params,
                             tickers=tickers,
                             ticker_name=ticker_name)


