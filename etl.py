from db_etl import extract_data_from_db, load_data_from_db
from csv_etl import extract_data_from_file, load_data_from_file
from datetime import datetime


date_str = input("Please enter a date (YYYY-MM-DD) or leave blank to use today's date: ")

if date_str:
    file_date = date_str
else:
    file_date = datetime.now().strftime("%Y-%m-%d")

etl_db_success = extract_data_from_db(file_date)
etl_file_success = extract_data_from_file(file_date)
if etl_db_success  == 0:
    load_data_from_db(file_date)

if etl_file_success == 0:
    load_data_from_file(file_date)