from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, date, timedelta
import json
import os
import glob
from sqlalchemy import create_engine, text

# Tenta importar Polars
try:
    import polars as pl
except ImportError:
    pl = None

# --- 1. CONFIGURAÇÕES ---
NODE_PROJECT_PATH = "/opt/node_project"
OUTPUT_DIR = "/opt/node_project/output"
DB_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

# NOME DA TABELA
NOME_TABELA = "tb_municipios_historico_final"

# --- 2. SQL CREATE TABLE ---
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {NOME_TABELA} (
    id SERIAL PRIMARY KEY,
    municipio VARCHAR(150),
    codigo_ibge INTEGER,
    grande_grupamento_atividade VARCHAR(255),
    atividade_economica VARCHAR(255),
    cnae_secao VARCHAR(255),
    cnae_divisao VARCHAR(255),
    cnae_grupo VARCHAR(255),
    cnae_classe VARCHAR(255),
    cnae_subclasse VARCHAR(255),
    admitidos INTEGER,
    desligados INTEGER,
    saldo INTEGER,
    estoque INTEGER,
    variacao_relativa FLOAT,
    tempo_emprego FLOAT,
    ano_ref INTEGER,         
    mes_ref INTEGER,         
    data_competencia DATE,   
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def garantir_tabela():
    print(f"--- 🛠️ Verificando Tabela Oficial: {NOME_TABELA} ---")
    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
    print("--- ✅ Estrutura verificada ---")

def processar_incremental_historico():
    if pl is None: 
        raise ImportError("Polars não instalado.")
    
    print("--- 🚀 Iniciando Carga Incremental ---")
    
    # 1. Busca JSON
    arquivos = glob.glob(f"{OUTPUT_DIR}/*.json")
    if not arquivos:
        print(f"⚠️ Nenhum JSON encontrado.")
        return
    
    arquivo_alvo = max(arquivos, key=os.path.getctime)
    print(f"📂 Lendo: {os.path.basename(arquivo_alvo)}")

    # 2. Leitura Blindada
    with open(arquivo_alvo, 'r', encoding='utf-8') as f:
        conteudo = f.read().replace('"INF"', 'null').replace('"-INF"', 'null').replace('"NaN"', 'null')
    
    data = json.loads(conteudo)
    first_key = list(data.keys())[0] if isinstance(data, dict) else None
    lista = data[first_key] if first_key else data
    
    df_new = pl.DataFrame(lista, infer_schema_length=None)

    # 3. Limpezas e Renomeação
    if "undefined" in df_new.columns: df_new = df_new.drop("undefined")
    if "Geográfico.Município" in df_new.columns: df_new = df_new.rename({"Geográfico.Município": "municipio"})
    elif "Município" in df_new.columns: df_new = df_new.rename({"Município": "municipio"})

    mapa_colunas = {
        "Grande Grupamento Atividade Econômica": "grande_grupamento_atividade",
        "Atividade Econômica": "atividade_economica",
        "CNAE 2.0 Seção": "cnae_secao",
        "CNAE 2.0 Divisão": "cnae_divisao",
        "CNAE 2.0 Grupo": "cnae_grupo",
        "CNAE 2.0 Classe": "cnae_classe",
        "CNAE 2.0 Subclasse": "cnae_subclasse",
        "Admitidos": "admitidos",
        "Desligados": "desligados",
        "Saldo": "saldo",
        "Estoque": "estoque",
        "Variação Relativa": "variacao_relativa",
        "Tempo de Emprego": "tempo_emprego"
    }
    cols_existentes = [c for c in mapa_colunas.keys() if c in df_new.columns]
    df_new = df_new.rename({k: v for k, v in mapa_colunas.items() if k in cols_existentes})

    # 4. Adiciona Chaves de Tempo
    ano_atual = datetime.now().year
    mes_atual_num = 10 
    data_cal = date(ano_atual, mes_atual_num, 1)

    df_new = df_new.with_columns([
        pl.lit(ano_atual).cast(pl.Int64).alias("ano_ref"),
        pl.lit(mes_atual_num).cast(pl.Int64).alias("mes_ref"),
        pl.lit(data_cal).alias("data_competencia")
    ])

    # 5. --- VALIDAÇÃO DE DUPLICIDADE (Anti-Join) ---
    print("--- 🔍 Comparando com o Histórico Existente ---")
    engine = create_engine(DB_URI)
    
    try:
        query = f"SELECT DISTINCT municipio, ano_ref, mes_ref FROM {NOME_TABELA}"
        df_banco = pl.read_database(query=query, connection=engine)
    except Exception:
        # Se tabela vazia/erro, cria schema vazio
        df_banco = pl.DataFrame(schema={"municipio": pl.Utf8, "ano_ref": pl.Int64, "mes_ref": pl.Int64})

    # === CORREÇÃO V22: FORÇAR CAST MESMO SE ESTIVER VAZIO ===
    # Isso resolve o erro: datatypes of join keys don't match
    df_banco = df_banco.with_columns([
        pl.col("municipio").cast(pl.Utf8),  # Força virar Texto (String)
        pl.col("ano_ref").cast(pl.Int64),
        pl.col("mes_ref").cast(pl.Int64)
    ])

    # Filtra: Mantém apenas o que NÃO existe no banco
    total_recebido = df_new.height
    
    df_final = df_new.join(
        df_banco, 
        on=["municipio", "ano_ref", "mes_ref"], 
        how="anti"
    )
    
    total_novos = df_final.height
    total_ignorados = total_recebido - total_novos
    
    print(f"📊 Relatório:")
    print(f"   📥 Recebidos: {total_recebido}")
    print(f"   ⏭️ Ignorados: {total_ignorados}")
    print(f"   💾 Novos: {total_novos}")

    if total_novos == 0:
        print("✅ Tudo atualizado. Nada a gravar.")
        return

    # 6. Tratamentos Finais
    CSV_IBGE = "/opt/airflow/data/de_para_ibge.csv"
    if os.path.exists(CSV_IBGE):
        try:
            df_ibge = pl.read_csv(CSV_IBGE, separator=";", ignore_errors=True)
            if len(df_ibge.columns) <= 1: df_ibge = pl.read_csv(CSV_IBGE, separator=",", ignore_errors=True)
            
            df_final = df_final.with_columns(pl.col("municipio").str.to_lowercase().str.strip_chars().alias("join_key"))
            col_nome = "NOME" if "NOME" in df_ibge.columns else df_ibge.columns[-1]
            col_cod = "COD" if "COD" in df_ibge.columns else df_ibge.columns[1]

            df_ibge = df_ibge.select([
                pl.col(col_nome).str.to_lowercase().str.strip_chars().alias("join_key"),
                pl.col(col_cod).alias("codigo_ibge_temp")
            ])
            
            df_final = df_final.join(df_ibge, on="join_key", how="left")
            df_final = df_final.rename({"codigo_ibge_temp": "codigo_ibge"}).drop("join_key")
        except:
            df_final = df_final.with_columns(pl.lit(0).alias("codigo_ibge"))
    else:
        df_final = df_final.with_columns(pl.lit(0).alias("codigo_ibge"))

    # Tipagem Segura
    df_final = df_final.with_columns([
        pl.col("grande_grupamento_atividade").fill_null("Indefinido"),
        pl.col("atividade_economica").fill_null("Indefinido"),
        pl.col("cnae_secao").fill_null("Indefinido"),
        pl.col("codigo_ibge").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("admitidos").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("desligados").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("saldo").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("estoque").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("variacao_relativa").cast(pl.Float64, strict=False).fill_null(0.0),
        pl.col("tempo_emprego").cast(pl.Float64, strict=False).fill_null(0.0),
    ])

    # 7. Insert Incremental
    colunas_permitidas = [
        "municipio", "codigo_ibge", "grande_grupamento_atividade", 
        "atividade_economica", "cnae_secao", "cnae_divisao", "cnae_grupo", 
        "cnae_classe", "cnae_subclasse", "admitidos", "desligados", 
        "saldo", "estoque", "variacao_relativa", "tempo_emprego", 
        "ano_ref", "mes_ref", "data_competencia"
    ]
    cols = [c for c in df_final.columns if c in colunas_permitidas]
    
    pdf = df_final.select(cols).to_pandas()
    pdf.to_sql(NOME_TABELA, engine, if_exists="append", index=False)
    
    print(f"--- ✅ Sucesso! {len(pdf)} registros adicionados. ---")

# --- DAG ---
default_args = { 'owner': 'airflow', 'retries': 0 }

with DAG(
    dag_id="orquestrador_municipios_v22_fix_join",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, 
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id="extracao_node", 
        bash_command=f"cd {NODE_PROJECT_PATH} && npm install && npm run start:json"
    )

    t2 = PythonOperator(
        task_id="verificar_tabela", 
        python_callable=garantir_tabela
    )

    t3 = PythonOperator(
        task_id="carga_incremental", 
        python_callable=processar_incremental_historico
    )

    t1 >> t2 >> t3