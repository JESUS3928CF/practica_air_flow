db_config = {
    
    'host': '10.206.69.138',
    'port': 11059,
    'user': 'mysqlsmo',
    'password': '171819.L05',
    'database': 'db_rr',
    'charset': 'utf8mb4',  
}

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