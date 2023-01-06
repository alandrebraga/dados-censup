from automation import Downloader
from loader import bulk_data
import os
import shutil


def find_csv_files(path_to_dir, suffix="CSV") -> list:
    if not os.path.exists(path_to_dir):
        raise Exception(f"A pasta {path_to_dir} nao existe")

    filenames = os.listdir(path_to_dir)
    csv_files = [filename for filename in filenames if filename.endswith(suffix)]
    return csv_files


def delete_files():
    shutil.rmtree("data")
    shutil.rmtree("extracted-files")


if __name__ == "__main__":
    try:
        Downloader.download_files()
        csv_files = find_csv_files("data")
        bulk_data.load(csv_files)
    finally:
        delete_files()
