# ========================================================================================
# This script consumes messages from a Kafka topic 'comment'. Each message contains a
# user_id, comment text, and additional metadata. It performs sentiment analysis on the
# comment text using a pre-trained multilingual model and stores the results in HBase.
# The processed data (including row_key, comment, score, and user_id) is then saved to 
# MySQL for further tracking. If the comment has been processed previously, it is skipped.
# ========================================================================================

import os
import pandas as pd
from transformers import pipeline
from thriftpy2.transport import TTransportException
import happybase
from sql_load import save_df_to_mysql

# === Environment Configuration ===
hbase_host = os.getenv('HBASE_HOST', 'localhost')
table_name = 'comment'
column_family = 'cf'  
sentiment_column = f'{column_family}:score'

# === Load Sentiment Analysis Model ===
pipe = pipeline("text-classification", model="tabularisai/multilingual-sentiment-analysis")

# === Connect to MySQL to track processed row_keys ===
import sqlalchemy
from sqlalchemy import create_engine

# === MySQL connection configuration ===
user = os.getenv("MYSQL_USER", "root")
password = os.getenv("MYSQL_PASSWORD", "rootpassword")
host = os.getenv("MYSQL_HOST", "127.0.0.1")
port = os.getenv("MYSQL_PORT", "3306")
database = os.getenv("MYSQL_DB", "mydatabase")

# === Create MySQL connection engine ===
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

# === Connect to HBase ===
try:
    connection = happybase.Connection(host=hbase_host, port=9090)
    connection.open()
except TTransportException as e:
    print(f"Error connecting to HBase: {e}")
    exit()

# Check if the table exists
if table_name.encode() not in connection.tables():
    print(f"The table '{table_name}' does not exist in HBase.")
    exit()

table = connection.table(table_name)

# === Check if the data has already been processed ===
# Query MySQL to get already processed row_keys
processed_keys_query = f"SELECT row_key FROM comments"
try:
    processed_keys_df = pd.read_sql(processed_keys_query, engine)
    existing_keys = set(processed_keys_df['row_key'])
except Exception as e:
    # If the table doesn't exist yet (first run), we handle it here
    print(f"MySQL query failed, assuming first run")
    existing_keys = set()

# === Process New Comments ===
new_data = []

for key, data in table.scan():
    row_key = key.decode('utf-8')

    # Skip if already processed
    if row_key in existing_keys:
        continue

    comment_bytes = data.get(f'{column_family}:comment'.encode())
    if comment_bytes:
        comment = comment_bytes.decode('utf-8')
        try:
            # Perform sentiment analysis (multilingual)
            sentiment = pipe(comment)[0]['label']  # Infer sentiment
        except Exception as e:
            print(f"Error processing comment with row_key {row_key}: {e}")
            sentiment = 'Error'

        # Assuming the user_id is stored in the HBase row, otherwise, adjust as needed
        user_id = data.get(f'{column_family}:user_id'.encode()).decode('utf-8') if data.get(f'{column_family}:user_id'.encode()) else 'Unknown'

        # Add new processed comment data with user_id
        new_data.append({
            'row_key': row_key,
            'comment': comment,
            'score': sentiment,
            'user_id': user_id  # Include user_id in the DataFrame
        })

        # Save sentiment result back to HBase (stored in the 'cf:score' column)
        table.put(row_key.encode(), {
            f'{column_family}:score'.encode(): sentiment.encode()
        })

# === Create DataFrame with new data ===
df_new = pd.DataFrame(new_data)

# Print the new processed comments and their sentiment
print("Processed comments and sentiment:")
print(df_new)

# === Save results to MySQL ===
if not df_new.empty:
    save_df_to_mysql(df_new)
