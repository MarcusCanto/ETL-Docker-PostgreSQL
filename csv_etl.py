from sqlalchemy import create_engine
import pandas as pd
import os
from db_etl import data_directory


original_file_type = "csv"
new_file_type = "parquet"
file_name = "order_details"


def extract_data_from_file(file_date):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))

        data_dir = os.path.join(script_dir, "data", f"{file_name}.{original_file_type}")

        to_save_dir = os.path.join(script_dir, "data", "csv", file_date)

        if not os.path.exists(to_save_dir):
            os.makedirs(to_save_dir)

        df = pd.read_csv(data_dir)

        file_path = os.path.join(to_save_dir, f"{file_name}.{new_file_type}")

        df.to_parquet(file_path, compression="brotli")

        return 0

    except Exception as e:
        print(f"Data extract from {original_file_type} file error: " + str(e))

def load_data_from_file(file_date):
    user="postgres"
    password= "0102"
    port="5433"
    database="db_lh_challenge"
    dialect = "postgresql"
    driver = "psycopg2"
    host="localhost"


    try:
        engine = create_engine(f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}")
        conn = engine.connect()

        data_dir = os.path.join(data_directory("data", "csv"), file_date)
        
        for subdir, dirs, files in os.walk(data_dir):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(subdir, file)
                    df = pd.read_parquet(file_path)
                    try:
                        table_df = pd.read_sql_table(file_name, engine)
                        new_rows = df[~df.isin(table_df)].dropna()
                        new_rows.to_sql(f"{file.split('.')[0]}", engine, index=False, if_exists="append")
                    except:
                        df.to_sql(f"{file.split('.')[0]}", engine, index=False, if_exists="append")

    except Exception as e:
         print("Data load from {original_file_type} error: " + str(e))
    finally:
        if conn:
           conn.close()    