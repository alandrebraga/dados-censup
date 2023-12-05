from source.utils import find_file_by_suffix, convert_csv_to_parquet, delete_csv_files
import pyarrow.parquet as pq
from psycopg2.extensions import register_adapter, AsIs, Float
from sqlalchemy import create_engine, text, MetaData, Table
import pandas as pd
import numpy as np


URI = "postgresql+psycopg2://bi_academico:bi_academico@biunivates.univates.br:5432/censu_educacao"


def convert_and_delete_files():
    csv_list = find_file_by_suffix("data")
    convert_csv_to_parquet(csv_list)
    delete_csv_files(csv_list)


def generate_schema(file_list: list) -> list:
    unique_columns = set()
    for file in file_list:
        schema = pq.read_schema(f"data/{file}")
        current_fields = set((field.name, field.type) for field in schema)
        unique_columns.update(current_fields)

    return list(unique_columns)


def run_sql(sql: str):
    engine = create_engine(URI, execution_options={'executemany_mode': 'values'})
    connection = engine.connect()
    connection.execute(text(sql))
    connection.commit()
    connection.close()


def insert_values(file: str, table_name: str):
    engine = create_engine(URI, execution_options={'executemany_mode': 'values'})
    df = pd.read_parquet(f"data/{file}")

    df.columns = list(map(lambda x: x.lower(), df.columns))

    try:
        metadata = MetaData()
        table = Table(table_name, metadata,autoload_with=engine)

        with engine.connect() as conn:
            table_columns = [col.name for col in table.columns]
            df_columns = [col for col in df.columns if col in table_columns]
            df = df[df_columns]

            df = df.fillna(AsIs('NULL'))

            int_columns = [col for col in df.columns if df[col].dtype == 'float64']
            df[int_columns] = df[int_columns].astype('Int64')

            conn.execute(table.insert(), df.to_dict(orient="records"))

            conn.commit()
            conn.close()

    except Exception as e:
        print(f"Erro ao inserir dados na tabela {table_name}: {e}")


def create_table_from_schema(schema: list, table_name) -> list:
    type_mapping: dict[str, str] = {
        "int64": "int",
        "string": "varchar",
    }
    schema_len = len(schema) - 1

    sql_schema = ""
    for i in range(len(schema)):
        cname, ctype = schema[i][0], schema[i][1]
        mapped_type = type_mapping.get(ctype)
        if i == schema_len:
            sql_schema += f"{cname} {mapped_type} \n"
        else:
            sql_schema += f"{cname} {mapped_type}, \n"

    create_sql = f"""
            CREATE TABLE {table_name}(
                {sql_schema}
            )
    """
    run_sql(create_sql)
    return create_sql


def run():
    convert_and_delete_files()
    files = find_file_by_suffix("data", "parquet")
    cursos = [file for file in files if "CURSOS" in file]
    ies = [file for file in files if "IES" in file]

    print(ies)
    schema_curso = generate_schema(cursos)

    schema_ies = generate_schema(ies)

    create_table_from_schema(schema_curso, "cursos")

    create_table_from_schema(schema_ies, "ies")

    for file in cursos:
        insert_values(file, "cursos")

    for file in ies:
        insert_values(file, "ies")


if __name__ == "__main__":
    pass
