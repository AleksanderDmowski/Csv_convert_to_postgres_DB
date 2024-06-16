import os  # 3.11.3
import csv
from typing import NoReturn, Callable
import pandas as pd
import psycopg2 as pg2

# version 0.11.0


def allowed_file_types(path: str) -> str | NoReturn:
    """Custom type hint for allowed extensions that can been converted to the postgreSQL."""
    allowed_extensions = ["csv"]
    file_extension = path.split(".")[-1]
    if file_extension in allowed_extensions:
        return path
    if file_extension not in "csv":
        raise TypeError("allowed_file_types() must have the extesion .csv")
    return None


def convert_file_path_into_name(path: str, name_range: int = 1) -> str:  # to improve
    """Converting a path/'name".extansion into "name" of dataframe."""
    if not os.path.isfile(path):
        return (path).lower()
    name = path.split("\\")
    elements_in_list = len(name)
    if name_range > elements_in_list:
        print("Error, out of range.")
        return None
    return (" ".join(map(str, name[-name_range:]))).lower()


def list_of_string_elements_for_underscore_character() -> list:
    """Change elements of path into underscore _"""
    return ["-", " ", "\\", "/"]


def list_of_string_elements_for_blank_character() -> list:
    """Change elements of path into empty space""."""
    return ["csv", "xlsx", "txt", ":", ".", "$", "(", ")", "%", "&"]


def replace_elements_for_correct_table_name(text: str) -> str:
    """Loop to create name of dataframe by changing."""
    for elemnet in list_of_string_elements_for_underscore_character():
        text = text.replace(elemnet, "_")
    for elemnet in list_of_string_elements_for_blank_character():
        text = text.replace(elemnet, "")
    return text.lower()


def commit_and_close(connect_to_pgadmin, open_session, with_commit=1) -> Callable:
    """Connect and commit to pgAdmin."""
    if with_commit:
        connect_to_pgadmin.commit()
    connect_to_pgadmin.close()
    open_session.close()


def column_types_replacement() -> dict:
    """Dict of columns type replace to pgAdmin types"""

    return {
        "timedelta64[ns]": "VARCHAR(45)",
        "object": "VARCHAR(45)",
        "float64": "float",
        "int64": "int",
        "datetime64": "timestamp",
        "datetime64[ns]": "timestamp",
    }


# to add
# Class login
# def pgadmin_login(host: int, dbname: str, user: str, password: str, sep: None) -> list:
#     return host, dbname, user, password, sep


# to improve -> create class converter()
def converter(
    file_bucket: list[allowed_file_types], host, dbname, user, password, sepparator
):
    """Main function to convert to final dataframe from source path tables."""
    for file_name in file_bucket:
        print(file_name)
        table_name = replace_elements_for_correct_table_name(
            convert_file_path_into_name(file_name)
        )

        try:
            df = pd.read_csv(file_name, sep=sepparator, engine="python")
        except FileNotFoundError:
            print("File not found or inncorect name.")
            break

        copied_values_from_source_dataframe = df.copy()

        for col in copied_values_from_source_dataframe.columns:
            if copied_values_from_source_dataframe[col].dropna().dtype == "object":
                try:
                    copied_values_from_source_dataframe[col] = pd.to_datetime(
                        copied_values_from_source_dataframe[col], format="%d.%m.%Y"
                    )
                except ValueError:
                    pass

        df.columns = [
            replace_elements_for_correct_table_name(columns) for columns in df.columns
        ]
        copied_values_from_source_dataframe.columns = [
            replace_elements_for_correct_table_name(columns) for columns in df.columns
        ]

        replaced_columns_type = ", ".join(
            "{} {}".format(pandas_names, pg_sql_names)
            for (pandas_names, pg_sql_names) in zip(
                df.columns,
                copied_values_from_source_dataframe.dtypes.replace(
                    column_types_replacement()
                ),
            )  # what if column name is blank?
        )

        if host:
            connect_to_pgadmin = pg2.connect_to_pgadminect(
                host=host, dbname=dbname, user=user, password=password
            )
        if not host:
            connect_to_pgadmin = pg2.connect_to_pgadminect(
                dbname=dbname, user=user, password=password
            )

        """Cursor is a class to open session with pgAdmin."""
        open_session = connect_to_pgadmin.cursor()

        open_session.execute(("drop table if exists {}").format([table_name]))
        try:
            open_session.execute(
                ("create table {}({})").format(table_name, replaced_columns_type)
            )
        except Exception:
            print("SyntaxError, probabbly you use wrong sepparator.")
            commit_and_close(connect_to_pgadmin, open_session, 0)
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

        sql_statement = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"

        open_session.copy_expert(sql=sql_statement % table_name, file=file_to_read)
        print("Copied to database")
        commit_and_close(connect_to_pgadmin, open_session)


if __name__ == "__main__":
    pass
