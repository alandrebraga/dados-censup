import pandas as pd
import os
from loader.dbconfig import config
import psycopg2


replacements = {"object": "varchar", "float64": "decimal", "int64": "int"}


def concat_dfs(csv_files: list) -> None:
    df_cursos = []
    df_ies = []

    for file in csv_files:
        try:
            if "CURSOS" in file:
                df_cursos.append(pd.read_csv(f"data/{file}", sep=";", low_memory=False))
            else:
                df_ies.append(pd.read_csv(f"data/{file}", sep=";", low_memory=False))
        except UnicodeDecodeError:
            if "CURSOS" in file:
                df_cursos.append(
                    pd.read_csv(
                        f"data/{file}", sep=";", encoding="ISO-8859-1", low_memory=False
                    )
                )
            else:
                df_ies.append(
                    pd.read_csv(
                        f"data/{file}", sep=";", encoding="ISO-8859-1", low_memory=False
                    )
                )

    cursos = pd.concat(df_cursos)
    ies = pd.concat(df_ies)
    cursos.to_csv(
        "data/cursos.csv", header=cursos.columns, index=False, encoding="utf-8"
    )
    ies.to_csv(
        "data/instituicoes.csv", header=ies.columns, index=False, encoding="utf-8"
    )

def load_data(file_path: str) -> None:
    tbl_name = ""
    if "cursos" in file_path:
        tbl_name = "cursos"
    else:
        tbl_name = "instituicoes"
    df = pd.read_csv(f"data/{tbl_name}.csv")
    my_file = open(f"data/{tbl_name}.csv", encoding="utf-8")

    col_str = ",  ".join(
        "{} {}".format(n, d)
        for (n, d) in zip(df.columns, df.dtypes.replace(replacements))
    )

    conn = psycopg2.connect(**config())
    cursor = conn.cursor()

    cursor.execute("drop table if exists %s" % (tbl_name))

    cursor.execute("create table %s (%s);" % (tbl_name, col_str))

    print("create table %s (%s);" % (tbl_name, col_str))

    COPY_STATEMENT = """
    COPY %s FROM STDIN WITH
    CSV
    HEADER
    DELIMITER AS ','
    """
    cursor.copy_expert(sql=COPY_STATEMENT % tbl_name, file=my_file)
    conn.commit()
    cursor.close()
    my_file.close()


def load(file_list: list):
    concat_dfs(file_list)
    file_paths = [
        "data/cursos.csv",
        "data/instituicoes.csv",
    ]
    for path in file_paths:
        load_data(path)
