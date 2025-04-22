from collections import Counter
import happybase
from datetime import datetime
import time


connection = happybase.Connection('localhost', port=9090)
table = connection.table('orders')


def categoria_mas_vendida():
    start_time = time.time()
    categorias = Counter()

    for _, data in table.scan(columns=['cf:category_code'], batch_size=1000, scan_batching=5000):
        categoria = data.get(b'cf:category_code')
        if categoria:
            categorias[categoria.decode()] += 1
        else:
            categorias['sin_categoria'] += 1

    resultado = categorias.most_common(1)
    end_time = time.time()

    print(f"Consulta 1 (Categoría más vendida): {end_time - start_time:.4f} segundos")
    print("Resultado:", resultado[0] if resultado else "N/A")


def mejor_brand():
    start_time = time.time()
    ingresos = Counter()

    for _, data in table.scan(columns=['cf:brand', 'cf:price'], batch_size=1000, scan_batching=5000):
        brand = data.get(b'cf:brand')
        price = data.get(b'cf:price')

        if brand:
            brand = brand.decode()
        else:
            brand = "sin_brand"

        try:
            price = float(price.decode()) if price else 0.0
        except ValueError:
            price = 0.0

        ingresos[brand] += price

    resultado = ingresos.most_common(1)
    end_time = time.time()

    print(f"Consulta 2 (Brand con mayores ingresos): {end_time - start_time:.4f} segundos")
    print("Resultado:", resultado[0] if resultado else "N/A")


def mes_mas_ventas():
    start_time = time.time()
    ventas_por_mes = Counter()

    for _, data in table.scan(columns=['cf:event_time'], batch_size=1000, scan_batching=5000):
        timestamp = data.get(b'cf:event_time')
        if timestamp:
            try:
                timestamp_str = timestamp.decode().replace(" UTC", "")
                dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                mes = dt.strftime('%m') 
                ventas_por_mes[mes] += 1
            except ValueError:
                continue

    resultado = ventas_por_mes.most_common(1)
    end_time = time.time()

    print(f"Consulta 3 (Mes con más ventas): {end_time - start_time:.4f} segundos")
    print("Resultado:", resultado[0] if resultado else "N/A")

categoria_mas_vendida()
mejor_brand()
mes_mas_ventas()


