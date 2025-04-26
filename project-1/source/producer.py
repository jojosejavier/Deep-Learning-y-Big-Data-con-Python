from kafka import KafkaProducer
import json
import time
import ast

# === Configurar productor de Kafka ===
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')  # Serializar mensajes como UTF-8
)

# === Bucle para enviar comentarios manualmente ===
while True:
    comment = input("Ingrese un comentario (o 'exit' para salir): ")
    if comment.lower() == 'exit':
        break  # Salir si el usuario escribe 'exit'
    if comment.strip(): 
        producer.send('comment', value=comment)  # Enviar comentario al tópico 'comment'
        print(f"Comentario enviado: {comment}")
    else:
        print("Comentario vacío, intente de nuevo.")

# === Finalizar productor ===
producer.flush()  # Asegurarse de que todos los mensajes se envíen
producer.close()  # Cerrar la conexión
