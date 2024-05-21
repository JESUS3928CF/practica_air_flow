from Excel import xlsx
columnas_smo = ['numero', 'consulta', 'estado']
combined_df = xlsx(columnas_smo, 'origen\\numeros_rr')

print(combined_df)