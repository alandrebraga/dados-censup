import os
from pyarrow import csv
import pyarrow.parquet as pq


def find_file_by_suffix(path_to_dir, suffix="CSV") -> list:
    if not os.path.exists(path_to_dir):
        raise Exception(f"A pasta {path_to_dir} nao existe")

    filenames = os.listdir(path_to_dir)
    csv_files = [filename for filename in filenames if filename.endswith(suffix)]
    return csv_files


def convert_csv_to_parquet(file_path_list: list):
    for file in file_path_list:
        csv_read_options = csv.ReadOptions(encoding="latin1")
        csv_parse_options = csv.ParseOptions(delimiter=";")
        arrow_df = csv.read_csv(
            f"data/{file}",
            read_options=csv_read_options,
            parse_options=csv_parse_options,
        )
        pq.write_table(arrow_df, f"data/{file[:-4]}.parquet")


def delete_csv_files(file_path_list: list):
    for file in file_path_list:
        os.remove(f"data/{file}")
