from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import polars as pl
import os
import json
from sqlalchemy import create_engine

# --- Configurações ---
# Caminhos internos do Docker (NÃO ALTERE PARA D:\...)
NODE_PROJECT_PATH = "/opt/node_project"
JSON_SOURCE = f"{NODE_PROJECT_PATH}/output/consolidado_municipios.DS0.json"
CSV_DESTINO = "/opt/airflow/data/consolidado_municipios_final.csv"

# Conexão com o Postgres (definida no docker-compose)
DB_CONNECTION_URI = "postgresql+psycopg2://admin_dados:senha_secreta@postgres:5432/dw_orquestrador"

def processar_json_para_csv():
    """Lê o JSON gerado pelo Node, trata com Polars e salva CSV"""
    print("🚀 Iniciando processamento com Polars...")
    
    try:
        with open(JSON_SOURCE, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            
        # Lógica de extração segura (baseada no seu código original)
        first_key = list(raw_data.keys())[0] if isinstance(raw_data, dict) else None
        list_data = raw_data[first_key] if first_key else raw_data
        
        df = pl.DataFrame(list_data)

        # Adiciona colunas de referência (Exemplo estático, idealmente dinâmico)
        df = df.with_columns([
            pl.lit(datetime.now().year).alias("Ano_Ref"),
            pl.lit("Outubro").alias("Mes_Ref") # Ajustar lógica de mês se necessário
        ])

        # Renomeia colunas
        if "Geográfico.Município" in df.columns:
            df = df.rename({"Geográfico.Município": "Município"})

        # Salva CSV
        df.write_csv(CSV_DESTINO, separator=";")
        print(f"✅ CSV salvo em: {CSV_DESTINO}")
        
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
        raise e

def carregar_para_banco():
    """Lê o CSV e insere na tabela SQL"""
    print("💾 Iniciando carga no Banco de Dados...")
    
    df = pl.read_csv(CSV_DESTINO, separator=";")
    
    # Cria conexão
    engine = create_engine(DB_CONNECTION_URI)
    
    # Escreve no banco (modo 'replace' recria a tabela, 'append' adiciona)
    # Vamos usar Pandas para o write_sql pela facilidade de compatibilidade inicial
    df_pandas = df.to_pandas()
    df_pandas.to_sql('tb_municipios', engine, if_exists='replace', index=False)
    
    print("✅ Dados carregados no PostgreSQL com sucesso!")

# --- Definição da DAG ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='orquestrador_municipios_v1',
    default_args=default_args,
    description='Pipeline completo: Node -> Polars -> Postgres',
    start_date=datetime(2023, 1, 1),
    schedule_interval='0 8 * * *', # Roda todo dia às 08:00 da manhã
    catchup=False
) as dag:

    # Tarefa 1: Executa o comando npm run start:json
    t1_extracao = BashOperator(
        task_id='extracao_node',
        bash_command=f"cd {NODE_PROJECT_PATH} && npm run start:json"
    )

    # Tarefa 2: Python trata os dados
    t2_transformacao = PythonOperator(
        task_id='transformacao_polars',
        python_callable=processar_json_para_csv
    )

    # Tarefa 3: Carrega no Banco
    t3_carga_banco = PythonOperator(
        task_id='carga_postgres',
        python_callable=carregar_para_banco
    )

    # Ordem de execução
    t1_extracao >> t2_transformacao >> t3_carga_banco