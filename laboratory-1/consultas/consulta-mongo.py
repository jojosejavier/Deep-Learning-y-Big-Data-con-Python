
#docker exec mongodb mongoimport --db=mongodb --collection=colection_mongo --type=csv --headerline --file=/datos/kz.csv --username=admin --password=admin123 --authenticationDatabase=admin 


from pymongo import MongoClient
import pandas as pd 
import time

# Conectarse al contenedor desde la máquina local
client = MongoClient("mongodb://admin:admin123@localhost:27017/?authSource=admin")


db = client["mongodb"]
coleccion = db["colection_mongo"]


def categoria_mas_vendida():   
    start_time = time.time()
    pipeline = [
    {"$group": {"_id": "$category_code", "total_ventas": {"$sum": 1}}},
    {"$sort": {"total_ventas": -1}}]
    resultado = list(coleccion.aggregate(pipeline))
    end_time = time.time()
    print(f"Consulta 1 (Categoría más vendida): {end_time - start_time:.4f} segundos")
    print("Resultado:", resultado[0] if resultado else "N/A")
    
def mejor_brand(): 
    start_time = time.time()
    pipeline = [
    {"$group": {"_id": "$brand", "total_ingresos": {"$sum": "$price"}}},
    {"$sort": {"total_ingresos": -1}}]
    resultado = list(coleccion.aggregate(pipeline))
    end_time = time.time()
    print(f"Consulta 2 (Brand con mayores ingresos): {end_time - start_time:.4f} segundos")
    print("Resultado:", resultado[0] if resultado else "N/A")
    
def mejor_mes():
    start_time = time.time()
    pipeline = [
        {"$project": {"mes": {"$month": {"$toDate": "$event_time"}}}},
        {"$group": {"_id":"$mes", "total_ventas": {"$sum": 1}}},
        {"$sort": {"total_ventas": -1}}]
    resultado = list(coleccion.aggregate(pipeline))
    end_time = time.time()
    print(f"Consulta 3 (Mes con mas ventas): {end_time - start_time:.4f} segundos")
    print("Resultado:", resultado[0] if resultado else "N/A")

categoria_mas_vendida()
mejor_brand()
mejor_mes()