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


def get_max_timestamp_miliseconds(connection, schema, table_name):
    return str(connection.execute(f"""select max(open_time) from {schema}.{table_name};""").first()[0])


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
    return pd.read_sql("select symbol from traditional_finance.companies_info order by symbol asc", con=engine).symbol.tolist()

