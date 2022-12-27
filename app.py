from automation import Downloader, Extractor
from loader import bulk_data
import os

def find_csv_files(path_to_dir, suffix='CSV') -> list:
    if not os.path.exists(path_to_dir):
        raise Exception(f"A pasta {path_to_dir} nao existe")

    filenames = os.listdir(path_to_dir)
    csv_files = [filename for filename in filenames if filename.endswith(suffix)]
    return csv_files

if __name__ == '__main__':
    try:
        os.mkdir("data")
    except FileExistsError:
        pass

    Downloader.run_wget()
    Extractor.execute_extraction()

    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    os.chdir(root_dir)

    csv_files = find_csv_files("data")
    bulk_data.load(csv_files)
    Downloader.delete_files()