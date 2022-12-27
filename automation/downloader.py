import subprocess
import os
import shutil

class Downloader:

    dirname = os.path.dirname(__file__)
    _filename = os.path.join(dirname, 'zipfiles')
    _command = f'wget --no-check -P {_filename} -i urls.txt'

    @classmethod
    def run_wget(cls) -> None:
        process = subprocess.Popen(
            cls._command,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            text = True,
            shell = True
        )
        std_out, std_err = process.communicate()
        print(std_out.strip(), std_err)

    @staticmethod
    def folder_exist(file_path: str) -> bool:
        return os.path.isdir(file_path)

    @classmethod
    def delete_files(cls) -> None:
        if cls.folder_exist('automation/zipfiles'):
            shutil.rmtree('automation/zipfiles')
        if cls.folder_exist('automation/extracted-files'):
            shutil.rmtree('automation/extracted-files')
        if cls.folder_exist('data'):
            shutil.rmtree('data')