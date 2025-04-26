import happybase
import os
import pandas as pd
from transformers import pipeline
from thriftpy2.transport import TTransportException
from sql_load import save_df_to_mysql   

# === Configuración de entorno ===
hbase_host = os.getenv('HBASE_HOST', 'localhost')
table_name = 'comment'
column_family = 'cf'
sentiment_column = f'{column_family}:score'
csv_filename = 'comments_with_sentiment.csv'  # Archivo para evitar procesar duplicados

# === Cargar modelo de análisis de sentimientos ===
pipe = pipeline("text-classification", model="tabularisai/multilingual-sentiment-analysis")

# === Leer row_keys ya procesados del CSV (si existe) ===
existing_keys = set()
if os.path.exists(csv_filename):
    df_existing = pd.read_csv(csv_filename)
    existing_keys = set(df_existing['row_key'])

# === Conexión a HBase ===
try:
    connection = happybase.Connection(host=hbase_host, port=9090)
    connection.open()
except TTransportException as e:
    print(f"Error conectando a HBase: {e}")
    exit()

# Verificar existencia de la tabla
if table_name.encode() not in connection.tables():
    print(f"La tabla '{table_name}' no existe en HBase.")
    exit()

table = connection.table(table_name)

# === Procesar comentarios nuevos ===
new_data = []
for key, data in table.scan():
    row_key = key.decode('utf-8')
    if row_key in existing_keys:
        continue  # Saltar si ya fue procesado

    comment_bytes = data.get(f'{column_family}:comment'.encode())
    if comment_bytes:
        comment = comment_bytes.decode('utf-8')
        try:
            sentiment = pipe(comment)[0]['label']  # Inferir sentimiento
        except Exception as e:
            print(f"Error procesando comentario con row_key {row_key}: {e}")
            sentiment = 'Error'

        new_data.append({
            'row_key': row_key,
            'comment': comment,
            'score': sentiment
        })

        # Guardar resultado del análisis en HBase
        table.put(row_key.encode(), {
            sentiment_column.encode(): sentiment.encode()
        })

# === Crear DataFrame con nuevos datos ===
df_new = pd.DataFrame(new_data)

print(df_new)

# === Guardar resultados en MySQL ===
save_df_to_mysql(df_new)
