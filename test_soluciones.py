from hashlib import sha256
import pandas as pd
from soluciones import ej_1_cargar_datos_demograficos, ej_2_cargar_calidad_aire

def _hash(data):
    return sha256(str(data).encode("utf-8")).hexdigest()

def test_sol_1():
    df = ej_1_cargar_datos_demograficos()
    idxs = [1995, 1360, 982, 2264, 2096, 1733, 1804, 2025, 2070, 507]

    # Obtiene las filas seleccionadas como una lista de diccionarios
    selected_rows = df.loc[idxs].to_dict(orient='records')

    # Calcula el hash de las filas seleccionadas
    hash_result = _hash(selected_rows)

    # Define el hash esperado (puedes calcularlo previamente)
    expected_hash = "567b67390efd8da8091f6f86da9f5e76b30d1b7dcb25bd7d9b87bcb757b2c571"

    assert hash_result == expected_hash

def test_sol_2():
    df = ej_1_cargar_datos_demograficos()
    ej_2_cargar_calidad_aire(set(df["City"].tolist()))

    ciudades_df = pd.read_csv("ciudades.csv")

    # Define los datos esperados en un diccionario
    expected_data = {
        'CO': {0: 250.34, 1: 287.06, 2: 247.0, 3: 280.38, 4: 323.77, 5: 243.66, 6: 173.57, 7: 211.95, 8: 263.69, 9: 260.35},
        'NO2': {0: 3.43, 1: 0.76, 2: 0.56, 3: 1.06, 4: 1.67, 5: 0.91, 6: 0.23, 7: 0.84, 8: 0.8, 9: 1.71},
        'O3': {0: 167.37, 1: 103.0, 2: 41.13, 3: 98.71, 4: 86.55, 5: 100.14, 6: 94.41, 7: 105.86, 8: 100.14, 9: 130.18},
        'SO2': {0: 2.92, 1: 2.3, 2: 0.19, 3: 1.1, 4: 6.32, 5: 1.27, 6: 0.38, 7: 0.24, 8: 0.4, 9: 5.36},
        'PM2.5': {0: 17.78, 1: 6.06, 2: 1.79, 3: 4.08, 4: 2.64, 5: 5.43, 6: 12.62, 7: 1.67, 8: 4.49, 9: 6.21},
        'PM10': {0: 26.26, 1: 6.37, 2: 1.85, 3: 4.47, 4: 2.95, 5: 5.68, 6: 48.06, 7: 1.79, 8: 4.66, 9: 6.76},
        'overall_aqi': {0: 220, 1: 170, 2: 34, 3: 159, 4: 128, 5: 162, 6: 148, 7: 177, 8: 162, 9: 205},
        'city': {0: 'Perris', 1: 'Mount Vernon', 2: 'Mobile', 3: 'Dale City', 4: 'Maple Grove', 5: 'Muncie', 6: 'San Clemente', 7: 'Providence', 8: 'Norman', 9: 'Hoover'}
    }

    # Convierte los datos reales en un diccionario similar al esperado
    actual_data = ciudades_df.loc[:9].to_dict()

    assert expected_data == actual_data

