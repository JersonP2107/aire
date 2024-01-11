import pandas as pd
import requests
import sqlite3
import json
def ej_1_cargar_datos_demograficos():
    url = "https://public.opendatasoft.com/explore/dataset/us-cities-demographics/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"
    data = pd.read_csv(url, sep=';')

    # Limpieza de datos
    data = data.drop(columns=['Race', 'Count', 'Number of Veterans'])
    data = data.drop_duplicates()

    return data

# Llamar a la función para obtener la tabla de datos demográficos
df_demograficos = ej_1_cargar_datos_demograficos()

def ej_2_cargar_calidad_aire(ciudades: set):
    # Crear una lista para almacenar los datos de calidad del aire
    calidad_aire_data = []

    for ciudad in ciudades:
        url = f"https://api-ninjas.com/api/airquality?city={ciudad}"
        response = requests.get(url)

        if response.status_code == 200:
            try:
                data = response.json()
                concentration = data.get("concentration", None)
                if concentration is not None:
                    calidad_aire_data.append({
                        "city": ciudad,
                        "concentration": concentration
                    })
             
             
             
            except json.decoder.JSONDecodeError:
                print(f"Error al decodificar JSON en la respuesta de la ciudad {ciudad}")
        else:
            print(f"Error en la respuesta de la API para la ciudad {ciudad}: {response.status_code}")

    # Resto del procesamiento
    # Crear una tabla de dimensiones de calidad del aire
    df_calidad_aire = pd.DataFrame(calidad_aire_data)

    # Guardar la tabla en un archivo CSV
    df_calidad_aire.to_csv("ciudades.csv", index=False)

# Obtener la lista de ciudades de la tabla demográfica
ciudades = set(df_demograficos["City"])

# Llamar a la función para cargar la calidad del aire
ej_2_cargar_calidad_aire(ciudades)

# Conectar a la base de datos SQLite
conn = sqlite3.connect("demografia.db")

# Cargar los datos demográficos en la base de datos
df_demograficos.to_sql("demografia", conn, if_exists="replace", index=False)

# Cargar los datos de calidad del aire en la base de datos
df_calidad_aire = pd.read_csv("ciudades.csv")
df_calidad_aire.to_sql("calidad_aire", conn, if_exists="replace", index=False)

# Realizar una consulta para verificar si las ciudades más pobladas tienen la peor calidad del aire
query = """
SELECT d.City, d.State, d.TotalPop, c.concentration
FROM demografia d
LEFT JOIN calidad_aire c ON d.City = c.city
ORDER BY d.TotalPop DESC
LIMIT 10;
"""

# Ejecutar la consulta y obtener los resultados
result = pd.read_sql_query(query, conn)
print(result)

# Cerrar la conexión a la base de datos
conn.close()
