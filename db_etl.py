from sqlalchemy import create_engine
import pandas as pd
import os


dialect = "postgresql"
driver = "psycopg2"
host="localhost"
table_names = ["customer_customer_demo", "customer_demographics", "employee_territories", 
               "orders", "customers", "products", "shippers", "suppliers","territories", "us_states", 
               "categories", "region", "employees"]


def data_directory(file_dir, file_subdir):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, file_dir, file_subdir)
    return data_dir


def extract_data_from_db(file_date):
    database="northwind"
    user="northwind_user"
    password="thewindisblowing"
    port="5432"
    file_type="parquet"

    try:
        engine = create_engine(f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}")

        conn = engine.connect()

        data_dir = data_directory("data", "postgres")
        
        for name in table_names:
            df = pd.read_sql(f"SELECT * FROM {name}", conn)
            file_path = os.path.join(data_dir, name, file_date, f"{name}.{file_type}")

            file_dir = os.path.dirname(file_path)
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            df.to_parquet(file_path, compression="brotli")

        return 0
    
    except Exception as e:
        print("Data extract from database error: " + str(e))
    finally:
        if conn:
            conn.close()


def load_data_from_db(file_date):
    user="postgres"
    password= "0102"
    port="5433"
    database="db_lh_challenge"

    try:
        engine = create_engine(f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}")
        conn = engine.connect()

        for name in table_names:
            data_dir = os.path.join(data_directory("data", "postgres"), name, file_date)
            for subdir, dirs, files in os.walk(data_dir):
                for file in files:
                    if file.endswith('.parquet'):
                        file_path = os.path.join(subdir, file)
                        df = pd.read_parquet(file_path)
                        try:
                            table_df = pd.read_sql_table(name, engine)
                            new_rows = df[~df.isin(table_df)].dropna()
                            new_rows.to_sql(f"{file.split('.')[0]}", engine, index=False, if_exists="append")
                        except:
                          df.to_sql(f"{file.split('.')[0]}", engine, index=False, if_exists="append")  

    except Exception as e:
         print("Data load from database error: " + str(e))
    finally:
        if conn:
           conn.close()    