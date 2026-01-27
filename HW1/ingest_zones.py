#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import click
import pandas as pd
from sqlalchemy import create_engine
import os

@click.command()
@click.option('--pg_user', required=True, help='Postgres username')
@click.option('--pg_pass', required=True, help='Postgres password')
@click.option('--pg_host', required=True, help='Postgres host')
@click.option('--pg_port', required=True, type=int, help='Postgres port')
@click.option('--pg_db', required=True, help='Postgres database name')
@click.option('--target_table', required=True, help='Name of the table to write to')
@click.option('--csv_file_path', required=True, help='Path to the local .csv file')


def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, csv_file_path):

    if not os.path.exists(csv_file_path):
        print(f"Error: File not found at {csv_file_path}")
        return


    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(f"Reading {csv_file_path} and ingesting into {target_table}...")


    df = pd.read_csv(csv_file_path)


    df.to_sql(name=target_table, con=engine, if_exists='replace', index=False)

    print(f"Successfully ingested {len(df)} rows into {target_table}.")

if __name__ == '__main__':
    run()


# In[ ]:




