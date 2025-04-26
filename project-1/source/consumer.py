from kafka import KafkaConsumer
import happybase
import uuid
import os
import time
from thriftpy2.transport import TTransportException

# Leer variables de entorno para configurar hosts
hbase_host = os.getenv('HBASE_HOST', 'localhost')
kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Función para conectar a HBase
def connect_to_hbase():
    try:
        connection = happybase.Connection(host=hbase_host, port=9090)
        connection.open()
        print("Conexión a HBase exitosa")
        return connection
    except TTransportException as e:
        print(f"Error de conexión a HBase: {e}")
        return None

# Conectar inicialmente a HBase
connection = connect_to_hbase()
if connection is None:
    exit()

table_name = 'comment'
column_family = 'cf'

# Verificar si la tabla existe, si no, crearla
if table_name.encode() not in connection.tables():
    print(f"Tabla '{table_name}' no existe. Creándola...")
    connection.create_table(table_name, {column_family: dict()})
else:
    print(f"Tabla '{table_name}' ya existe.")

# Obtener el objeto tabla
table = connection.table(table_name)

# Crear consumidor de Kafka para el tópico 'comment'
consumer = KafkaConsumer(
    'comment',
    bootstrap_servers=[kafka_bootstrap],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("Esperando mensajes...")

# Bucle para procesar mensajes entrantes
while True:
    for msg in consumer:
        comment = msg.value
        row_key = str(uuid.uuid4())  # Generar clave única para la fila
        print(f"Comentario recibido: {comment}")

        # Guardar comentario en HBase
        try:
            table.put(row_key, {f'{column_family}:comment'.encode(): comment.encode()})
            print(f"Guardado en HBase con row key: {row_key}")
        except TTransportException as e:
            # Manejar pérdida de conexión con HBase
            print("Conexión perdida. Intentando reconectar a HBase...")
            connection.close()
            connection = connect_to_hbase()
            if connection is None:
                print("No se pudo reconectar a HBase. Saliendo...")
                exit()
            table = connection.table(table_name)
            try:
                table.put(row_key, {f'{column_family}:comment'.encode(): comment.encode()})
                print(f"Guardado en HBase tras reconexión con row key: {row_key}")
            except Exception as e:
                print(f"No se pudo guardar el comentario después de reconectar: {e}")
