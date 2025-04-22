import redis
import time


r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def categoria_mas_vendida():   
    start_time = time.time()
    lua_script = """
    local cats = {}
    for _,k in ipairs(redis.call('KEYS', 'orden:*')) do
        local cat = redis.call('HGET', k, 'category_code') or 'sin_categoria'
        cats[cat] = (cats[cat] or 0) + 1
    end
    local top_cat = nil
    local top_count = -1
    for cat,count in pairs(cats) do
        if count > top_count then
            top_cat = cat
            top_count = count
        end
    end
    return {top_cat, top_count}
    """
    resultado = r.eval(lua_script, 0)
    end_time = time.time()
    print(f"Consulta 1 (Categoría más vendida): {end_time - start_time:.4f} segundos")
    print(f"Categoría más vendida: {resultado[0]}: {resultado[1]}")

def mejor_brand():   
    start_time = time.time()
    lua_script2 = """
    local ingresos = {}
    for _,k in ipairs(redis.call('KEYS', 'orden:*')) do
        local brand = redis.call('HGET', k, 'brand') or 'sin_brand'
        local price = tonumber(redis.call('HGET', k, 'price') or '0')
        ingresos[brand] = (ingresos[brand] or 0) + price
    end
    local top_brand = nil
    local top_ingresos = -1
    for brand, total in pairs(ingresos) do
        if total > top_ingresos then
            top_brand = brand
            top_ingresos = total
        end
    end
    return {top_brand, top_ingresos}
    """
    resultado2 = r.eval(lua_script2, 0)
    end_time = time.time()
    print(f"Consulta 2 (Brand con mayores ingresos): {end_time - start_time:.4f} segundos")
    print(f"Brand con mayores ingresos: {resultado2[0]}: {resultado2[1]}")

def mejor_mes():   
    start_time = time.time()
    lua_script3 = """
    local meses = {}
    for _, k in ipairs(redis.call('KEYS', 'orden:*')) do
      local fecha = redis.call('HGET', k, 'event_time')
      
      if fecha then
        local mes = string.sub(fecha, 6, 7)
        meses[mes] = (meses[mes] or 0) + 1
      end
    end

    local top = {}
    for mes, ventas in pairs(meses) do
      table.insert(top, {mes, ventas})
    end

    table.sort(top, function(a, b) return a[2] > b[2] end)
    return top[1]
    """
    resultado3 = r.eval(lua_script3, 0)
    end_time = time.time()
    print(f"Consulta 3 (Mes con más ventas): {end_time - start_time:.4f} segundos")
    print(f"Mes con más ventas: {resultado3[0]}: {resultado3[1]}")

# Ejecutar las funciones
categoria_mas_vendida()
mejor_brand()
mejor_mes()
