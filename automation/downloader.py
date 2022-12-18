import subprocess
import os

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
