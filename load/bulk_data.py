import pandas as pd
import os
import glob

def load():
    csv_files = glob.glob('../data/*.CSV')

    df_cursos = {}
    df_ies = {}

    for file in csv_files:
        try:
            if "CURSOS" in file:
                df_cursos[file] = pd.read_csv(file, sep=';')
            else:
                df_ies[file] = pd.read_csv(file, sep=';')
        except UnicodeDecodeError:
            if "CURSOS" in file:
                df_cursos[file] = pd.read_csv(file, sep=';', encoding='ISO-8859-1')
            else:
                df_ies[file] = pd.read_csv(file, sep=';', encoding='ISO-8859-1')

    for k in csv_files:
        if "CURSOS" in k:
            df = df_cursos[k]
        else:
            df = df_ies[k]

        replacements = {
            'object': 'varchar',
            'float64': 'float',
            'int64': 'int'
        }

        col_str = "; ".join("{} {}".format(n, d)
            for (n,d) in zip (df.columns, df.dtypes.replace(replacements)))
        print(col_str)

load()