from automation import Downloader, Extractor
import os

if __name__ == '__main__':
    os.mkdir("data")
    Downloader.run_wget()
    Extractor.execute_extraction()