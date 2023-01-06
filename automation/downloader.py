import os
import requests
from io import BytesIO
import zipfile
import glob
import shutil


class Downloader:

    dirname = os.path.dirname(__file__)

    @staticmethod
    def _get_urls() -> list:
        urls = []
        with open("urls.txt", "r") as f:
            urls = [line.strip() for line in f]
        return urls

    @staticmethod
    def _move_csv_to_data():
        for dirpath, _, files in os.walk("extracted-files"):
            for file in files:
                if file.endswith(".CSV"):
                    shutil.copy(f"{dirpath}/{file}", "data")

    @classmethod
    def download_files(cls) -> None:
        try:
            os.makedirs("extracted-files", exist_ok=True)
            os.makedirs("data", exist_ok=True)
        except OSError as e:
            print(f"Directory can not be created, error - {e}")
        for url in cls._get_urls():
            filebytes = BytesIO(requests.get(url, verify=False).content)
            zipfile.ZipFile(filebytes).extractall("extracted-files")
        cls._move_csv_to_data()
