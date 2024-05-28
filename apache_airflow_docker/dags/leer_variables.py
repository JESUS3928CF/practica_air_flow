""" from airflow.models import Variable

variables = Variable.get('variables', deserialize_json=True)

#! Asi en un futuro no hay que cambiar code si la variable cambia
#/ Puedes poner las variables por defecto para que todos los operadores las tenga aca
default_args = {
        's3_bucket' : variables['s3_bucket'],
        'email': variables['email']
}

with DAG(dag_id,
        default_args=default_args,
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        tags=TAGS,
        catchup=False) as dag:

        #/ O puedes pasarle la variable solo al operador que la necesite 
        S3TToRedshiftOperator(task_id = 1, s3_bucket=variables['s3_bucket'])

        RedshiftTos3Operator(task_id = 2)

        GoogleAPIToS3Operator(task_id = 1) """