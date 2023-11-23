from source.extract import Downloader
from source.load import run

if __name__ == "__main__":
    Downloader.download_files()
    run()
