import pandas as pd
import os

if os.path.isfile("capacidad_campo_media.txt"):
    os.rename("capacidad_campo_media.txt", "capacidad_campo.txt")

if os.path.isfile("punto_marchitez_medio.txt"):
    os.rename("punto_marchitez_medio.txt", "punto_marchitez.txt")

if os.path.isfile("porosidad_media.txt"):
    os.remove("porosidad_media.txt")

if os.path.isfile("umbral_escorrentia_intermedio.txt"):
    os.remove("umbral_escorrentia_intermedio.txt")

if not os.path.exists("parquet"):
    os.mkdir("parquet")

# Leer archivo
nombre_archivos = ["capacidad_campo", "punto_marchitez", "umbral_seco", "umbral_intermedio", "umbral_humedo"]

for nombre_archivo in nombre_archivos:
    with open(f"{nombre_archivo}.txt", "r") as f:
        lines = f.readlines()

    # Extraer la información del encabezado
    header_data = {}
    for i in range(6):
        key, value = lines[i].strip().split()
        header_data[key] = value

    # Extraer los valores de la matriz y aplanarla
    matrix_data = []
    for line in lines[6:]:
        row = [float(cell.replace(",", ".")) for cell in line.strip().split()]
        matrix_data.extend(row)

    # Crear el dataframe en Pandas
    data = {"id": list(range(len(matrix_data))), f"{nombre_archivo}": matrix_data}
    df = pd.DataFrame(data)
    df.set_index("id", inplace=True)
    # Eliminar filas que contengan -9999
    df = df[df[f"{nombre_archivo}"] != -9999]

    # Imprimir los primeros 25 registros
    print(df.info())
    ruta_parquet = os.path.join("parquet", f"{nombre_archivo}.parquet")
    df.to_parquet(ruta_parquet)



