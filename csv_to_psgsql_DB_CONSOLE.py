from csv_to_psgsql_DB_converter import converter

run_app = True
if run_app:
    # list of files to convert
    file_bucket = [
        r'path_to_file']
    host = 0  # 0 is default value
    dbname = 'database_name'
    user = 'postgres'
    password = 'xxx'
    separator = ';'

    converter(file_bucket=file_bucket, host=host, dbname=dbname,
              user=user, password=password, sep=separator)
