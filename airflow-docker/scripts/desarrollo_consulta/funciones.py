import os
import pandas as pd
from config import db_config, generate_sql_insert_update
from sqlalchemy import create_engine
import numpy as np
import pymysql
#-------------------- función para limpiar el df
def to_float (df,columna_tel):
    df['Cantidad_Dígitos'] = df[columna_tel].apply(lambda x: len(str(x)))
    df['Primer_dig'] = df[columna_tel].astype(str).str[0]
    df = df[df['Cantidad_Dígitos']==10]
    df = df[df['Primer_dig']=='3']
    df.drop(columns=['Cantidad_Dígitos','Primer_dig'],inplace=True)
    return df
#función para enviar a base de datos
def df_tosql(df,tabla1):
    host_local = db_config['host']
    port_local = db_config['port']
    user_local = db_config['user']
    password_local = db_config['password']
    database_local = db_config['database']
    charset_local = db_config['charset']
    cadena_conexion_local = f"mysql+pymysql://{user_local}:{password_local}@{host_local}:{port_local}/{database_local}?charset={charset_local}"
    engine_local = create_engine(cadena_conexion_local)
    try:
        df.to_sql(name= tabla1, con=engine_local, if_exists='append', index=False)
    except Exception as e:
        print(e)
#--------------- función para mantener integridad de los datos
def update(tabla2,sql):
    try:
        connn = pymysql.connect(
                    host = "10.206.69.138",
                    port = 11059,
                    user = "root",
                    password = "171819.L05",
                    database = "db_rr"
        )
        cursor = connn.cursor()        
        query = sql
        print("haciendo update....")        
        cursor.execute(query)
        connn.commit()        
        query2 = f"""TRUNCATE TABLE {tabla2}"""        
        cursor.execute(query2)
        connn.commit()        
        cursor.close()
        connn.close()        
        print("listo....")        
    except Exception as e:        
        print(f"error al actualizar {e}")


def txt(columns,c1,c2,tabla,tabla2):
    carpeta = r'\\10.206.69.168\screenshot\desarrollo\airflow'
    ruta1 = os.path.join(carpeta, c1)
    ruta2 = os.path.join(carpeta, c2)
    # Lista para almacenar los DataFrames
    dataframes = []   
    # Definir los nombres de las columnas
    column_names = columns  # Reemplaza con los nombres de columnas apropiados  
    # Recorre todos los archivos en la ruta especificada
    for filename in os.listdir(ruta1):
        if filename.endswith('.txt'):     
            file_path = os.path.join(ruta1, filename)
            # Leer el archivo .txt en un DataFrame sin encabezados y asignar los nombres de las columnas
            df = pd.read_csv(file_path, delimiter='\t', header=None, names=column_names)  # Ajusta el delimitador si es necesario
            dataframes.append(df)
    
    # Combina todos los DataFrames en uno solo
    combined_df = pd.concat(dataframes, ignore_index=True)
    df_cleaned = to_float(combined_df,str(columns[0]))
    
    consulta=generate_sql_insert_update(tabla2,tabla,columns)
    df_tosql(df_cleaned,tabla)
    update(tabla,consulta)




