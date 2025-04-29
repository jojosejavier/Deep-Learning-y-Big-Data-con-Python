import os
import sqlalchemy
import pandas as pd

def save_df_to_mysql(df):
    """
    Saves a DataFrame to a MySQL database in the 'comments' table.
    If the table already exists, new data will be appended without replacing it.

    Arguments:
    - df: pandas.DataFrame
    """

    # === MySQL connection configuration ===
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "rootpassword")
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = os.getenv("MYSQL_PORT", "3306")
    database = os.getenv("MYSQL_DB", "mydatabase")

    # === Create connection engine ===
    try:
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
        print("Successfully connected to MySQL")

        # Save DataFrame to 'comments' table (appending data if the table already exists)
        df.to_sql('comments', con=engine, if_exists='append', index=False)
        print("Data successfully loaded into MySQL database.")

    except sqlalchemy.exc.SQLAlchemyError as e:
        # Error handling for connection or write issues
        print("Error connecting to or saving data in MySQL:")
        print(e)
