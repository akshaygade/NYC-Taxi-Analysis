import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, csv_file):
    print(table_name, csv_file)
    try:
        engine = create_engine(
            f'postgresql://{user}:{password}@{host}:{port}/{db}')

        engine.connect()
    except Exception as e:
        print("Engine creation failed")
    df = pd.read_parquet("output.parquet",
                         engine='fastparquet')

    df.head(10).to_sql(name=table_name, con=engine, if_exists='replace')


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

#     parser.add_argument('--user', required=True, help='user name for postgres')
#     parser.add_argument('--password', required=True,
#                         help='password for postgres')
#     parser.add_argument('--host', required=True, help='host for postgres')
#     parser.add_argument('--port', required=True, help='port for postgres')
#     parser.add_argument('--db', required=True,
#                         help='database name for postgres')
#     parser.add_argument('--table_name', required=True,
#                         help='name of the table where we will write the results to')
#     parser.add_argument('--url', required=True, help='url of the csv file')

#     args = parser.parse_args()

#     main(args)
