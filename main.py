import pandas as pd
import os

# Renombrar archivos si es necesario
if os.path.isfile("capacidad_campo_media.txt"):
    os.rename("capacidad_campo_media.txt", "capacidad_campo.txt")

if os.path.isfile("punto_marchitez_medio.txt"):
    os.rename("punto_marchitez_medio.txt", "punto_marchitez.txt")

# Eliminar archivos si es necesario
if os.path.isfile("porosidad_media.txt"):
    os.remove("porosidad_media.txt")

if os.path.isfile("umbral_escorrentia_intermedio.txt"):
    os.remove("umbral_escorrentia_intermedio.txt")

# Crear carpeta para archivos parquet si no existe
if not os.path.exists("parquet"):
    os.mkdir("parquet")

# Leer archivos y unirlos
nombre_archivos = ["capacidad_campo", "punto_marchitez", "umbral_seco", "umbral_intermedio", "umbral_humedo"]
nombre_archivo_conjunto = "propiedades_hidricas"

# Lista para almacenar los dataframes individuales
dataframes = []
columnas = ["id"]

# Leer cada archivo y crear un dataframe
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
    
    # Agregar el dataframe a la lista
    dataframes.append(df)
    print(dataframes)

# Unir dataframes en función de la columna "Id"
df_final = dataframes[0]
for i in range(1, len(dataframes)):
    df_final = df_final.merge(dataframes[i], on=columnas)

# Imprimir información sobre el dataframe resultante
print(df_final.info())
ruta_parquet = os.path.join("parquet", f"{nombre_archivo_conjunto}.parquet")
df_final.to_parquet(ruta_parquet)
print("Archivo creado:", ruta_parquet)
print(df_final)
