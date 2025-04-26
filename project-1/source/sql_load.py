import os
import sqlalchemy
import pandas as pd

def save_df_to_mysql(df):
    """
    Guarda un DataFrame en una base de datos MySQL en la tabla 'comments'.
    Si la tabla existe, será reemplazada.

    Argumentos:
    - df: pandas.DataFrame
    """

    # === Configuración de conexión a MySQL ===
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "rootpassword")
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = os.getenv("MYSQL_PORT", "3306")
    database = os.getenv("MYSQL_DB", "mydatabase")

    # === Crear engine de conexión ===
    try:
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
        print("Conexión exitosa a MySQL")

        # Guardar DataFrame en la tabla 'comments' (reemplazándola si existe)
        df.to_sql('comments', con=engine, if_exists='replace', index=False)
        print("Datos cargados en la base de datos MySQL.")

    except sqlalchemy.exc.SQLAlchemyError as e:
        # Manejo de errores en la conexión o escritura
        print("Error al conectar o guardar en MySQL:")
        print(e)
