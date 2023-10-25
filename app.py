from pyarrow import csv
import polars as pl
import os
import pyarrow.parquet as pq
from automation import Downloader
import os
from sqlalchemy import create_engine

def find_csv_files(path_to_dir, suffix="CSV") -> list:
    if not os.path.exists(path_to_dir):
        raise Exception(f"A pasta {path_to_dir} nao existe")

    filenames = os.listdir(path_to_dir)
    csv_files = [filename for filename in filenames if filename.endswith(suffix)]
    return csv_files

def convert_csv_to_parquet(file_path_list: list):
    for file in file_path_list:
        csv_read_options = csv.ReadOptions(encoding='latin1')
        csv_parse_options = csv.ParseOptions(delimiter=';')
        arrow_df = csv.read_csv(f"data/{file}", read_options=csv_read_options, parse_options=csv_parse_options)
        pq.write_table(arrow_df, f"data/{file}.parquet")
        
def concat_parquet(file_path_parquet: list, output_name: str):
    dfs = []
    for file in file_path_parquet:
        df = pl.read_parquet(file)
        dfs.append(df)
    df_concat = pl.concat(dfs, how="diagonal_relaxed")
    pl.DataFrame.write_parquet(df_concat, f"data/{output_name}.parquet")

if __name__ == "__main__":
    Downloader.download_files()
    
    csv_files = find_csv_files("data")
    
    convert_csv_to_parquet(csv_files)
    
    parquet_files = find_csv_files("data", suffix="parquet")    
    
    course_file = [f"data/{c}" for c in parquet_files if "CURSO" in c]
    ies_file = [f"data/{c}" for c in parquet_files if "IES" in  c]
    
    concat_parquet(course_file, "curso")
    concat_parquet(ies_file, "ies") 
    
    connection_uri = "postgresql://postgres:admin@localhost/censo"
    engine = create_engine(connection_uri)
    
    df = pl.read_parquet('data/curso.parquet')
    df.write_database(table_name='curso', connection=connection_uri, engine="sqlalchemy" )
    
    df = pl.read_parquet('data/ies.parquet')
    df.write_database(table_name='curso', connection=connection_uri, engine="sqlalchemy" )
           