import glob
import os
import re
import shutil
from zipfile import ZipFile

class Extractor:

    _dirname = os.path.dirname(__file__)
    _path = os.path.join(_dirname, 'zipfiles')

    @staticmethod
    def _list_zip_files(folder_path: str) -> list:
        extension = 'zip'
        os.chdir(folder_path)
        zip_files = glob.glob(f'*.{extension}')
        return zip_files

    @classmethod
    def extract_zip_files(cls) -> None:
        extract_dir = '../extracted-files'
        zip_list = Extractor._list_zip_files(cls._path)
        for z in zip_list:
            with ZipFile(f'{cls._path}/{z}', 'r') as f:
               f.extractall(extract_dir)

    @classmethod
    def extract_csv(cls) -> None:
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'extracted-files')
        os.chdir(path)
        folders = os.listdir()
        for folder in folders:
            old_name = folder
            new_name = re.search(r'\d+', folder).group()
            os.rename(f'{path}/{old_name}', f'{path}/{new_name}')
            os.chdir(f'{path}/{new_name}')
            inner_folders = os.listdir()
            for i in inner_folders:
                os.chdir(f'{path}/{new_name}/dados')
                files = os.listdir()
                for file in files:
                    if file.endswith('.CSV'):
                        original_path = os.getcwd()
                        shutil.move(f'{original_path}/{file}', f'../../../../data/{file}')

    @staticmethod
    def execute_extraction():
        Extractor.extract_zip_files()
        Extractor.extract_csv()
