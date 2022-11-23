from csv_to_psgsql_DB_converter import converter

run_app = True
if run_app:
    # list of files to convert
    file_bucket = [
        r'C:\Users\path_to\file_name.csv']
    host = 0  # 0 is default value
    dbname = 'postgres'
    user = 'postgres'
    password = '1111'
    separator = None

    converter(
        file_bucket=file_bucket,
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        sep=separator)
