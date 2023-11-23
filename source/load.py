from source.utils import find_file_by_suffix, convert_csv_to_parquet, delete_csv_files
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
import polars as pl

URI = "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"


def convert_and_delete_files():
    csv_list = find_file_by_suffix("data", "csv")
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
    engine = create_engine(URI)

    connection = engine.connect()
    connection.execute(text(sql))
    connection.commit()
    connection.close()


def insert_values(file: str, table_name: str):
    print(f"data/{file}")
    df = pl.read_parquet(f"data/{file}")

    df.columns = list(map(lambda x: x.lower(), df.columns))

    try:
        df.write_database(table_name=table_name, if_exists="append", connection=URI)
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
    files = find_file_by_suffix("data", "parquet")
    cursos = [file for file in files if "CURSOS" in file]
    ies = [file for file in files if "IES" in file]

    schema_curso = generate_schema(cursos)

    schema_ies = generate_schema(ies)

    create_table_from_schema(schema_curso, "cursos")

    ies = create_table_from_schema(schema_ies, "ies")

    for file in cursos:
        insert_values(file, "cursos")

    for file in ies:
        insert_values(file, "ies")


if __name__ == "__main__":
    pass
