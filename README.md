# Csv_convert_to_postgres_DB

Script for converting csv file into postgresSQL database.

## Requirements

```bash
pip install pandas
pip install psycopg2
pip install os
```

## Usage

```python
from csv_to_psgsql_DB_converter import converter
#run function converter
    converter(
        file_bucket=file_bucket, 
        host=host, 
        dbname=dbname,
        user=user, 
        password=password, 
        sep=separator)
#Or open and run csv_to_psgsql_DB_CONSOLE.py
```

## Contributting
Feel free to steal this code.

## Future
Folder selection as source of files  
Posibility to convert excel files and txt files  
Window app  
Even more automation  
Documentation  

## My Links
Check my [website](https://aleksanderdmowski.com/)
