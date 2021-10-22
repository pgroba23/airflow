from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook
from datetime import datetime
from pandas.io.json import json_normalize
import pandas as pd
import json
import os


default_args={
    'start_date': datetime(2021,10,16)
}

def _get_response():
    hook = HttpHook(http_conn_id='usuario_api',method='GET')

    resp = hook.run('/api?results=5000')
    print('response= ',resp.text)
    with open('/usr/local/airflow/landing/response.json', 'w') as outfile:
        json.dump(json.loads(resp.text),outfile)

#def _save_response(json_string):
#    with open('/usr/local/airflow/landing/response.json', 'w') as outfile:
#        outfile.write(json_string)
#        outfile.close()

def _procesa_usuario(**context):
   # ti = context['ti']
   # print('context: ', context)
   # print('ti: ', ti)
    #usuarios = json.loads(ti.xcom_pull(task_ids=['extraer_usuario'])[0])
    with open('/usr/local/airflow/landing/response.json', 'r') as outfile:
        usuarios = json.load(outfile)
    #usuarios = json.loads(ti.xcom_pull(task_ids=['extraer_usuario']))
    #usuarios = ti.xcom_pull(task_ids=['extraer_usuario'])
    #print('usuarios= ',usuarios)
    if not len(usuarios) or 'results' not in usuarios:
        raise ValueError('Usuario vacio')
    for n in range(len(usuarios['results'])):
        usuario = usuarios['results'][n]
       # print('usuario= ',usuario)
        df_usuario_procesado = json_normalize({
            'Nombre':usuario['name']['first'],
            'Apellido':usuario['name']['last'],
            'Pais':usuario['location']['country'],
            'Usuario':usuario['login']['username'],
            'Contraseña':usuario['login']['password'],
            'Email':usuario['email']
        })
        #print(df_usuario_procesado)
        path = r'/usr/local/airflow/landing/usuario_procesado.csv'
        df_usuario_procesado.to_csv(path,index=None, mode="a", header=not os.path.isfile(path))
        #print(df_usuario_procesado)

def _almacenar_usuario():
    #Open Postgres Connection
    #pg_hook = PostgresHook(postgres_conn_id='postgres_db')

    postgres_hook = PostgresHook(postgres_conn_id='postgres_db')
    engine = postgres_hook.get_sqlalchemy_engine()
    #print(engine)

    # CSV loading to table.
    df = pd.read_csv(r'/usr/local/airflow/landing/usuario_procesado.csv', header=0)
    #print(df)

    df.to_sql('usuarios', con=engine, if_exists='append', index=False)



with DAG('process_user', schedule_interval='@daily',
default_args=default_args,
catchup=False) as dag:

#Definir tareas
    crear_tabla = PostgresOperator(
       task_id='crear_tabla',
       postgres_conn_id='postgres_db',
       sql='''
           CREATE TABLE IF NOT EXISTS usuarios(
           Nombre TEXT NOT NULL,
           Apellido TEXT NOT NULL,
           Pais TEXT NOT NULL,
           Usuario TEXT NOT NULL,
           Contraseña TEXT NOT NULL,
           Email TEXT NOT NULL PRIMARY KEY
       );
       '''
    )

    api_disponible = HttpSensor(
       task_id = 'api_disponible',
       http_conn_id = 'usuario_api',
       endpoint = 'api/'
    )

    #extraer_usuario = SimpleHttpOperator(
    #    task_id = 'extraer_usuario',
    #    http_conn_id = 'usuario_api',
    #    endpoint='api/?results=1',
    #    method='GET',
    #    response_filter=lambda response: print(json.loads(response.text)['results']),
        #response_filter=lambda response: _save_response(response.text),
        #response_filter=lambda response: response.json()['results'],
    #    log_response=True,
    #    xcom_push=True
    #)

    obtener_respuesta = PythonOperator(
        task_id='obtener_respuesta',
        python_callable=_get_response)

    procesa_usuario = PythonOperator(
        task_id='procesa_usuario',
        provide_context=True,
        python_callable=_procesa_usuario
    )

    almacenar_usuario = PythonOperator(
       task_id = 'almacenar_usuario',
       python_callable=_almacenar_usuario
    )


    archive_file_procesado = BashOperator(
        task_id='archive_file_procesado',
        bash_command='mv /usr/local/airflow/landing/usuario_procesado.csv /usr/local/airflow/archive/usuario_procesado_{{ execution_date.strftime("%Y%m%d_%H%M%S") }}.csv'
    )



    crear_tabla >> api_disponible >> obtener_respuesta >> procesa_usuario >> almacenar_usuario >> archive_file_procesado
