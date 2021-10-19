from airflow import DAG
#from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime
#from pandas import json_normalize
from pandas.io.json import json_normalize
import json

default_args={
    'start_date': datetime(2021,10,16)
}
#ti=task instance
def _procesa_usuario(**kwargs):
    ti = kwargs['ti']
    usuarios = ti.xcom_pull(task_ids=['extraer_usuario'])
    print(usuarios)
    if not len(usuarios) or 'results' not in usuarios[0]:
        raise ValueError('Usuario vacio')
    usuario = usuarios[0]['results'][0]
    usuario_procesado = json_normalize({
        'nombre':usuario['name']['first'],
        'apellido':usuario['name']['last'],
        'pais':usuario['location']['country'],
        'usuario':usuario['login']['username'],
        'contraseña':usuario['login']['password'],
        'email':usuario['email'],
    })
    usuario_procesado.to_csv('/tmp/usuario_procesado.csv',index=None, header=False)



with DAG('procesar_usuario', schedule_interval='@daily',
default_args=default_args,
catchup=False) as dag:

#Definir tareas
    crear_tabla = PostgresOperator(
       task_id='crear_tabla',
       postgres_conn_id='postgres_db',
       sql='''
           CREATE TABLE IF NOT EXISTS usuarios(
           nombre TEXT NOT NULL,
           apellido TEXT NOT NULL,
           pais TEXT NOT NULL,
           usuario TEXT NOT NULL,
           contraseña TEXT NOT NULL,
           email TEXT NOT NULL PRIMARY KEY
       );
       '''
    )

    api_disponible = HttpSensor(
       task_id = 'api_disponible',
       http_conn_id = 'usuario_api',
       endpoint = 'api/'
    )

    extraer_usuario = SimpleHttpOperator(
        task_id = 'extraer_usuario',
        http_conn_id = 'usuario_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
        xcom_push=True
    )

    procesa_usuario = PythonOperator(
        task_id='procesa_usuario',
        provide_context=True,
        python_callable=_procesa_usuario
    )
#sqlite3 no va a andar falta ruta
    almacenar_usuario = BashOperator(
        task_id='almacenar_usuario',
        #bash_command = 'echo -e ".separator ","\n.import /tmp/usuario_procesado.csv usuarios" | sqlite3 airflow.db'
        bash_command = 'cp /tmp/usuario_procesado.csv /home/pgroba/airflow/results'
    )

    crear_tabla >> api_disponible >> extraer_usuario >> procesa_usuario >> almacenar_usuario
