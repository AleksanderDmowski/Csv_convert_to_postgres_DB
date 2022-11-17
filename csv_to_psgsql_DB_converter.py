import pandas as pd
import psycopg2 as pg2

file_bucket = [
    r'C:\Users\Aleksander\Desktop\1-Pandas-SQL\Nowy folder\Shops2_DB.csv']


def convert_file_path_into_name(path='x', name_range=1, direct=0):
    if direct:
        return (direct).lower()
    n = name_range
    name = path.split("\\")
    elements_in_list = len(name)
    if n >= elements_in_list:
        return print('error')
    return (' '.join(map(str, name[-n:]))).lower()


def replace_elements_in_table(text):
    replace_elemnts_1 = ['-', ' ', '\\', '/']
    replace_elemnts_2 = ['csv', 'xlsx', '.txt', ':', '.', '$', '(', ')', '%']
    for i in replace_elemnts_1:
        text = text.replace(i, '_')
    for i in replace_elemnts_2:
        text = text.replace(i, '')
    return text.lower()


def converter():
    for file_name in file_bucket:
        print(file_name)
        table_name = replace_elements_in_table(
            convert_file_path_into_name(file_name))

        df = pd.read_csv(file_name, sep=";")
        df.columns = [replace_elements_in_table(
            columns) for columns in df.columns]

        replacements = {
            'timedelta64[ns]': 'VARCHAR(45)',
            'object': 'VARCHAR(45)',
            'float64': 'float',
            'int64': 'int',
            'datetime64': 'timestamp'
        }

        replaced_columns_type = ', '.join("{} {}".format(pandas_names, pg_sql_names) for (
            pandas_names, pg_sql_names) in zip(df.columns, df.dtypes.replace(replacements)))

        conn = pg2.connect(dbname='fake_shops',
                           user='postgres', password='xxx')
        cursor = conn.cursor()
        print('db opened successfully')

        cursor.execute(("drop table if exists {}").format(table_name))
        cursor.execute(("create table {}({})").format(
            table_name, replaced_columns_type))
        df.to_csv((("{}").format(convert_file_path_into_name(file_name))),
                  header=df.columns, index=False, encoding='utf-8')
        file_to_read = open(("{}").format(
            convert_file_path_into_name(file_name)), encoding='utf-8')

        SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"

        cursor.copy_expert(sql=SQL_STATEMENT % table_name, file=file_to_read)
        print('file copied to db')

        conn.commit()
        conn.close()
        cursor.close()


run_app = True
if run_app:
    converter()