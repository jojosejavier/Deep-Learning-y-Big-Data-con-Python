# ========================================================================================
# This script consumes messages from a Kafka topic named 'comment' and stores each 
# comment as a new row in an HBase table called 'comment'. It handles connection 
# errors with HBase by attempting to reconnect automatically if the connection is lost. 
# The messages include additional fields like user_id.
# ========================================================================================

import os
import uuid
import json
from kafka import KafkaConsumer
import happybase
from thriftpy2.transport import TTransportException

# Read environment variables to configure hosts
hbase_host = os.getenv('HBASE_HOST', 'localhost')
kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')


# Function to connect to HBase
def connect_to_hbase():
    try:
        connection = happybase.Connection(host=hbase_host, port=9090)
        connection.open()
        print("Successfully connected to HBase")
        return connection
    except TTransportException as e:
        print(f"Connection error with HBase: {e}")
        return None


# Initially connect to HBase
connection = connect_to_hbase()
if connection is None:
    exit()

table_name = 'comment'
column_family = 'cf'

# Check if the table exists; if not, create it
if table_name.encode() not in connection.tables():
    print(f"Table '{table_name}' does not exist. Creating it...")
    connection.create_table(table_name, {column_family: dict()})
else:
    print(f"Table '{table_name}' already exists.")

# Get the table object
table = connection.table(table_name)

# Create Kafka consumer for 'comment' topic
consumer = KafkaConsumer(
    'comment',
    bootstrap_servers=[kafka_bootstrap],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON data
)

print("Waiting for messages...")

# Loop to process incoming messages
while True:
    for msg in consumer:
        # Deserialize the JSON message
        message = msg.value
        user_id = message.get("user_id")
        comment = message.get("comment")
        
        # Generate unique row key
        row_key = str(uuid.uuid4())  # Unique identifier for the row
        print(f"Comment received: {message}")

        # Save the comment along with other fields in HBase
        try:
            table.put(row_key, {
                f'{column_family}:user_id'.encode(): user_id.encode(),
                f'{column_family}:comment'.encode(): comment.encode()
            })
            print(f"Saved to HBase with row key: {row_key}")
        except TTransportException:
            # Handle loss of connection to HBase
            print("Connection lost. Attempting to reconnect to HBase...")
            connection.close()
            connection = connect_to_hbase()
            if connection is None:
                print("Could not reconnect to HBase. Exiting...")
                exit()
            table = connection.table(table_name)
            try:
                table.put(row_key, {
                    f'{column_family}:user_id'.encode(): user_id.encode(),
                    f'{column_family}:comment'.encode(): comment.encode()
                })
                print(f"Saved to HBase after reconnection with row key: {row_key}")
            except Exception as e:
                print(f"Failed to save the comment after reconnecting: {e}")
