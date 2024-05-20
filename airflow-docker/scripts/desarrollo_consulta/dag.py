""" from scripts.desarrollo_consulta.funciones import txt
def tag_smo():
    columnas_smo=["numero",'plan','cd_plan','fecha_consulta']
    txt(columnas_smo,'origen\\smo','procesado\\smo','smo_cargue','smo')
 """

def tag_smo():
    print('Desde otra ubicación')