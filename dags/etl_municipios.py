import os
import glob
from datetime import datetime
from sqlalchemy import create_engine, text, inspect

try:
    import polars as pl
    import pandas as pd
except ImportError:
    pl = None
    pd = None

# --- CONFIGURAÇÕES ---
BASE_PATH = "/opt/airflow"
ANALYTICS_FOLDER = os.path.join(BASE_PATH, "analytics")
DATA_FOLDER = os.path.join(BASE_PATH, "data")
DE_PARA_IBGE = os.path.join(DATA_FOLDER, "de_para_ibge.csv")

DB_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
NOME_TABELA = "tb_municipios_historico_final"

def processar_arquivos(modo_teste=False, **kwargs):
    if pl is None: raise ImportError("Polars não instalado.")
    
    print(f"--- 🚀 Iniciando (Modo Teste: {modo_teste}) ---")
    
    # 1. Busca e Leitura
    padrao = os.path.join(ANALYTICS_FOLDER, "analise_*.csv")
    arquivos = glob.glob(padrao)
    if not arquivos:
        print("⚠️ Nenhum arquivo encontrado.")
        return

    arquivo = max(arquivos, key=os.path.getctime)
    print(f"📂 Lendo: {os.path.basename(arquivo)}")
    
    try:
        df = pl.read_csv(arquivo, separator=";", ignore_errors=True)
    except Exception as e:
        print(f"❌ Erro leitura: {e}")
        return

    # 2. Tratamentos
    data_hoje = datetime.now()
    if "grande_grupamento_atividade" not in df.columns:
        df = df.with_columns(pl.lit("Indefinido").alias("grande_grupamento_atividade"))

    df = df.with_columns([
        pl.lit(data_hoje.year).alias("ano_ref"),
        pl.lit(data_hoje.month).alias("mes_ref"),
        pl.lit(data_hoje).alias("data_processamento"),
        pl.col("municipio").cast(pl.Utf8).fill_null("N/A")
    ])

    # 3. Join IBGE
    if os.path.exists(DE_PARA_IBGE):
        try:
            df_ibge = pl.read_csv(DE_PARA_IBGE, separator=";", ignore_errors=True)
            df = df.with_columns(pl.col("municipio").str.to_lowercase().str.strip_chars().alias("k"))
            df_ibge = df_ibge.select([
                pl.col("NOME_MUNICIPIO").str.to_lowercase().str.strip_chars().alias("k"),
                pl.col("COD").alias("cod")
            ])
            df = df.join(df_ibge, on="k", how="left").rename({"cod": "codigo_ibge"}).drop("k")
            df = df.with_columns(pl.col("codigo_ibge").fill_null(0))
        except:
            df = df.with_columns(pl.lit(0).alias("codigo_ibge"))
    else:
        df = df.with_columns(pl.lit(0).alias("codigo_ibge"))

    if modo_teste:
        df.write_csv(os.path.join(DATA_FOLDER, "teste_resultado.csv"), separator=";")
        print("✅ Modo teste concluído.")
        return 

    # 4. GRAVAÇÃO NO BANCO
    print("--- 💾 Iniciando transação segura no Banco ---")
    engine = create_engine(DB_URI)
    df_pandas = df.to_pandas()

    cols_db = ["municipio", "codigo_ibge", "grande_grupamento_atividade", 
               "cnae_secao", "admitidos", "desligados", "saldo", 
               "variacao_relativa", "ano_ref", "mes_ref", "data_processamento"]
    cols_finais = [c for c in cols_db if c in df_pandas.columns]
    df_pandas = df_pandas[cols_finais]

    # Verifica se tabela existe
    inspector = inspect(engine)
    tabela_existe = inspector.has_table(NOME_TABELA)

    with engine.begin() as conn:
        # === AQUI ESTÁ A CORREÇÃO ===
        # Se a tabela não existir, criamos ela MANUALMENTE com o ID
        if not tabela_existe:
            print("   🛠️ Criando tabela nova com estrutura correta (ID)...")
            sql_create = text(f"""
                CREATE TABLE {NOME_TABELA} (
                    id SERIAL PRIMARY KEY,
                    municipio VARCHAR(150),
                    codigo_ibge INTEGER,
                    grande_grupamento_atividade VARCHAR(255),
                    cnae_secao VARCHAR(255),
                    admitidos INTEGER,
                    desligados INTEGER,
                    saldo INTEGER,
                    variacao_relativa FLOAT,
                    ano_ref INTEGER,         
                    mes_ref INTEGER,         
                    data_processamento TIMESTAMP
                );
            """)
            conn.execute(sql_create)
        else:
            # Se já existir, limpamos as duplicatas
            print("   🧹 Limpando dados antigos (evitar duplicidade)...")
            chaves_para_limpar = df_pandas[['municipio', 'ano_ref', 'mes_ref']].drop_duplicates()
            for i, row in chaves_para_limpar.iterrows():
                sql_delete = text(f"""
                    DELETE FROM {NOME_TABELA} 
                    WHERE municipio = :mun 
                      AND ano_ref = :ano 
                      AND mes_ref = :mes
                """)
                conn.execute(sql_delete, {
                    "mun": row['municipio'], 
                    "ano": row['ano_ref'], 
                    "mes": row['mes_ref']
                })

        print(f"   📥 Inserindo {len(df_pandas)} novas linhas...")
        # if_exists='append' agora vai respeitar a tabela que acabamos de criar
        df_pandas.to_sql(NOME_TABELA, conn, if_exists="append", index=False)
    
    print("✅ Processo concluído com sucesso!")