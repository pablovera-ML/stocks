from binance.spot import Spot
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import pandas as pd
import json
import logging
import os
from dotenv import load_dotenv

LOGGER = logging.getLogger()
if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    LOGGER.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)
LOGGER.info("Changing logging level")


def get_secret():
    load_dotenv()
    secret = {'host': os.environ['DATABASE_HOST'], 'password': os.environ['DATABASE_PASSWORD'],
              'username': os.environ['DATABASE_USERNAME']}
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


def create_table(connection, schema, table_name):
    connection.execute(f"""create table {schema}.{table_name}(
                                open_time bigint,
                                open_date timestamp,
                                close_time bigint,
                                close_date timestamp,
                                open_price numeric,
                                high_price numeric,
                                low_price numeric,
                                close_price numeric,
                                volume numeric,
                                quote_asset_volume numeric,
                                number_of_trades numeric,
                                taker_buy_base_asset_volume numeric,
                                taker_buy_quote_asset_volume numeric,
                                primary key (open_time),
                                unique(open_time)
                            )
                       """)
    print(f"Table {schema}.{table_name} created!")


def get_max_timestamp_miliseconds(connection, schema, table_name):
    return str(connection.execute(f"""select max(open_time) from {schema}.{table_name};""").first()[0])


def convert_date_to_timestamp_miliseconds(date):
    return int(datetime.strptime(date, '%Y-%m-%d').timestamp() * 1000)


def get_klines(pair, interval, start_time):
    batch = Spot(base_url='https://api.binance.us').klines(symbol=pair.upper(),
                          interval=interval,
                          startTime=start_time,
                          limit=1000)

    batch_df = pd.DataFrame(batch, columns=['open_time',
                                            'open_price',
                                            'high_price',
                                            'low_price',
                                            'close_price',
                                            'volume',
                                            'close_time',
                                            'quote_asset_volume',
                                            'number_of_trades',
                                            'taker_buy_base_asset_volume',
                                            'taker_buy_quote_asset_volume',
                                            'ignore'])

    batch_df['open_date'] = pd.to_datetime(batch_df["open_time"], unit='ms')
    batch_df['close_date'] = pd.to_datetime(batch_df["close_time"], unit='ms')
    return batch_df


def insert_klines(engine, connection, schema, table_name, klines):
    klines.to_sql(con=engine,
                  schema=schema,
                  name=f'staging_{table_name}',
                  index=False,
                  if_exists='replace')
    connection.execute(f"""
                        insert into {schema}.{table_name} 
                                    (
                                    open_time,
                                    open_date,
                                    close_time,
                                    close_date,
                                    open_price,
                                    high_price,
                                    low_price,
                                    close_price,
                                    volume,
                                    quote_asset_volume,
                                    number_of_trades,
                                    taker_buy_base_asset_volume,
                                    taker_buy_quote_asset_volume
                                    )
                        select 
                                open_time::bigint as open_time,
                                open_date::timestamp as open_date,
                                close_time::bigint as close_time,
                                close_date::timestamp as close_date,
                                open_price::numeric as open_price,
                                high_price::numeric as high_price,
                                low_price::numeric as low_price,
                                close_price::numeric as close_price,
                                volume::numeric as volume,
                                quote_asset_volume::numeric as quote_asset_volume,
                                number_of_trades::numeric as number_of_trades,
                                taker_buy_base_asset_volume::numeric as taker_buy_base_asset_volume,
                                taker_buy_quote_asset_volume::numeric as taker_buy_quote_asset_volume
                        from {schema}.staging_{table_name}
                        on conflict (open_time) do update SET
                                open_date = excluded.open_date,
                                close_time = excluded.close_time,
                                close_date = excluded.close_date,
                                open_price = excluded.open_price,
                                high_price = excluded.high_price,
                                low_price = excluded.low_price,
                                close_price = excluded.close_price,
                                volume = excluded.volume,
                                quote_asset_volume = excluded.quote_asset_volume,
                                number_of_trades = excluded.number_of_trades,
                                taker_buy_base_asset_volume = excluded.taker_buy_base_asset_volume,
                                taker_buy_quote_asset_volume = excluded.taker_buy_quote_asset_volume;""")
    print(f"Data inserted into table {schema}.{table_name}!")

