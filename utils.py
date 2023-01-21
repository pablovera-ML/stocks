import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

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
    return str(connection.execute(f"""select (max({date_column_name})-interval '1 day')::date 
                                      from {schema}.{table_name};""").first()[0])


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
    return pd.read_sql("""select replace(symbol, '.', '_') as symbol 
                          from stocks.companies_info order by symbol asc""", con=engine).symbol.tolist()


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

