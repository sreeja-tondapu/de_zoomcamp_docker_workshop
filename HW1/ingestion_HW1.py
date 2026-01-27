import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import pyarrow.parquet as pq
import os

@click.command()
@click.option('--pg_user', required=True, help='Postgres username')
@click.option('--pg_pass', required=True, help='Postgres password')
@click.option('--pg_host', required=True, help='Postgres host')
@click.option('--pg_port', required=True, type=int, help='Postgres port')
@click.option('--pg_db', required=True, help='Postgres database name')
@click.option('--target_table', required=True, help='Name of the table to write to')
@click.option('--file_path', required=True, help='Path to the local .parquet file')
@click.option('--chunksize', default=100000, help='Number of rows per batch')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, file_path, chunksize):
  
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return

  
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(f"Connecting to database and reading from local file: {file_path}")


    parquet_file = pq.ParquetFile(file_path)

    first = True

    for batch in tqdm(parquet_file.iter_batches(batch_size=chunksize)):
        df_chunk = batch.to_pandas()

        if first:
      
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
            first = False

      
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

    print("Successfully ingested data.")

if __name__ == '__main__':
    run()