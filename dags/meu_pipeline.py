from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Importa a função que acabamos de testar com sucesso
from etl_municipios import processar_arquivos

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
    'pipeline_municipios_botucatu',
    default_args=default_args,
    schedule_interval='@daily', # Roda todo dia (pode mudar para None se for manual)
    catchup=False,
    description='ETL que le CSV de analytics, cruza com IBGE e salva no Postgres'
) as dag:

    # Esta tarefa chama a mesma função do teste, mas SEM o modo_teste.
    # Ou seja: ELA VAI GRAVAR NO BANCO DE DADOS.
    tarefa_etl = PythonOperator(
        task_id='processar_e_salvar_dados',
        python_callable=processar_arquivos,
        op_kwargs={'modo_teste': False} # False = Produção (Grava no Banco)
    )

    tarefa_etl