def generate_sql_insert_update(table_target, table_source, fields):

    # Crear la sección de inserción de la consulta
    insert_query = f"INSERT INTO {table_target} ({', '.join(fields)})\nSELECT {', '.join(fields)}\nFROM {table_source}\n"
    
    # Crear la sección de actualización en caso de clave duplicada
    update_query = "ON DUPLICATE KEY UPDATE\n"
    updates = []
    con=0
    for field in fields:
        if con==0:
            con+=1
            continue
        
        update = f"{field} = IF({table_source}.{field} IS NOT NULL, {table_source}.{field}, {table_target}.{field})"
        updates.append(update)
    
    # Combinar todo en una sola consulta
    final_query = insert_query + update_query + ",\n".join(updates)
    
    return final_query

# Ejemplo de uso de la función
fields = ["numero",'plan','cd_plan','fecha_consulta']
table_target = 'cp1'
table_source = 'cp1_cargue'

sql_query = generate_sql_insert_update(table_target, table_source, fields)
print(sql_query)
