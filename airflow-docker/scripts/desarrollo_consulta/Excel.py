import os
import pandas as pd
from openpyxl import load_workbook

def xlsx(columns, c1):
    carpeta = r'\\10.206.69.168\screenshot\desarrollo\airflow'
    ruta1 = os.path.join(carpeta, c1)

    # Lista para almacenar los DataFrames
    dataframes = []
    
    # Leer los archivos Excel en la carpeta especificada y combinarlos en un DataFrame
    for archivo in os.listdir(ruta1):
        if archivo.endswith('.xlsx'):
            ruta_archivo = os.path.join(ruta1, archivo)
            print(f"Leyendo archivo: {ruta_archivo}")  # Paso de depuración
            
            try:
                # Leer el archivo Excel en un DataFrame
                df = pd.read_excel(ruta_archivo, header=None, usecols=[0, 1, 2])
                df.columns = columns  # Asignar nombres de columnas
                dataframes.append(df)
            except Exception as e:
                print(f"Error al leer el archivo {ruta_archivo}: {e}")

    # Si no se encontraron archivos, devolver None
    if not dataframes:
        print("No se encontraron archivos .xlsx para procesar en la carpeta especificada.")
        return None
    
    # Combina todos los DataFrames en uno solo
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    return combined_df








