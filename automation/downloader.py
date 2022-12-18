import subprocess
import os

class Downloader:

    def __init__(self):
        dirname = os.path.dirname(__file__)
        self._filename = os.path.join(dirname, 'zipfiles')
        self._command = f'wget --no-check -P {self._filename} -i downloader/urls.txt'

    def run_wget(self):
        print(self._command)
        process = subprocess.Popen(
            self._command,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            text = True,
            shell = True
        )
        std_out, std_err = process.communicate()
        print(std_out.strip(), std_err)
