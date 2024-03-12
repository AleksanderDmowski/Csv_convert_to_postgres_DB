import os  # 3.11.3
import pandas as pd
import psycopg2 as pg2

# version 0.0.9


def convert_file_path_into_name(path="x", name_range=1) -> str:
    if not os.path.isfile(path):
        return (path).lower()
    name = path.split("\\")
    elements_in_list = len(name)
    if name_range > elements_in_list:
        print("Error, out of range.")
        return None
    return (" ".join(map(str, name[-name_range:]))).lower()


def list_of_string_elements_for_underscore_character() -> list:
    return ["-", " ", "\\", "/"]


def list_of_string_elements_for_blank_character() -> list:
    return ["csv", "xlsx", ".txt", ":", ".", "$", "(", ")", "%", "&"]


def replace_elements_for_correct_table_name(text) -> str:
    for elemnet in list_of_string_elements_for_underscore_character():
        text = text.replace(elemnet, "_")
    for elemnet in list_of_string_elements_for_blank_character():
        text = text.replace(elemnet, "")
    return text.lower()


def commit_and_close(conn, cursor, with_commit=1):
    if with_commit:
        conn.commit()
    conn.close()
    cursor.close()


def converter(file_bucket, host, dbname, user, password, sep):
    for file_name in file_bucket:
        print(file_name)
        table_name = replace_elements_for_correct_table_name(
            convert_file_path_into_name(file_name)
        )

        try:
            df = pd.read_csv(file_name, sep=sep, engine="python")
        except FileNotFoundError:
            print("File not found or inncorect name.")
            break

        df_copy_for_values = df.copy()

        for col in df_copy_for_values.columns:
            if df_copy_for_values[col].dropna().dtype == "object":
                try:
                    df_copy_for_values[col] = pd.to_datetime(
                        df_copy_for_values[col], format="%d.%m.%Y"
                    )
                except ValueError:
                    pass

        df.columns = [
            replace_elements_for_correct_table_name(columns) for columns in df.columns
        ]
        df_copy_for_values.columns = [
            replace_elements_for_correct_table_name(columns) for columns in df.columns
        ]

        replacements = {
            "timedelta64[ns]": "VARCHAR(45)",
            "object": "VARCHAR(45)",
            "float64": "float",
            "int64": "int",
            "datetime64": "timestamp",
            "datetime64[ns]": "timestamp",
        }

        replaced_columns_type = ", ".join(
            "{} {}".format(pandas_names, pg_sql_names)
            for (pandas_names, pg_sql_names) in zip(
                df.columns, df_copy_for_values.dtypes.replace(replacements)
            )
        )

        if host:
            conn = pg2.connect(host=host, dbname=dbname, user=user, password=password)
        else:
            conn = pg2.connect(dbname=dbname, user=user, password=password)
        cursor = conn.cursor()

        cursor.execute(("drop table if exists {}").format([table_name]))
        try:
            cursor.execute(
                ("create table {}({})").format(table_name, replaced_columns_type)
            )
        except Exception:
            print("SyntaxError, probabbly you use wrong sepparator.")
            commit_and_close(conn, cursor, 0)
            break

        df.to_csv(
            (("{}").format(convert_file_path_into_name(file_name))),
            header=df.columns,
            index=False,
            encoding="utf-8",
        )
        file_to_read = open(
            ("{}").format(convert_file_path_into_name(file_name)), encoding="utf-8"
        )

        SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"

        cursor.copy_expert(sql=SQL_STATEMENT % table_name, file=file_to_read)
        print("Copied to database")
        commit_and_close(conn, cursor)
