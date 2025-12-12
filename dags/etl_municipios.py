import os
import glob
from datetime import datetime
from sqlalchemy import create_engine, text

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

# Conexão Banco
DB_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
NOME_TABELA = "tb_municipios_historico_final"

def garantir_tabela():
    """Cria a tabela no banco (apenas estrutura)."""
    engine = create_engine(DB_URI)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {NOME_TABELA} (
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
        data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print(f"--- 🛠️ Estrutura da tabela {NOME_TABELA} verificada. ---")

def processar_arquivos(modo_teste=False, **kwargs):
    """
    Parâmetro:
    modo_teste (bool): Se True, SALVA CSV local e NÃO grava no banco.
                       Se False, GRAVA no banco de dados.
    """
    
    if pl is None: raise ImportError("Polars não instalado.")
    
    tipo_execucao = "TESTE (Visualização)" if modo_teste else "PRODUÇÃO (Banco de Dados)"
    print(f"--- 🚀 Iniciando Processamento: {tipo_execucao} ---")
    
    # 1. Busca Arquivo
    padrao = os.path.join(ANALYTICS_FOLDER, "analise_*.csv")
    arquivos = glob.glob(padrao)
    
    if not arquivos:
        print(f"⚠️ Nenhum arquivo em: {padrao}")
        return

    arquivo = max(arquivos, key=os.path.getctime)
    print(f"📂 Lendo: {os.path.basename(arquivo)}")

    # 2. Leitura e Tratamento
    try:
        df = pl.read_csv(arquivo, separator=";", ignore_errors=True)
    except Exception as e:
        print(f"❌ Erro leitura: {e}")
        return

    # Normalizações básicas
    data_hoje = datetime.now()
    if "grande_grupamento_atividade" not in df.columns:
        df = df.with_columns(pl.lit("Indefinido").alias("grande_grupamento_atividade"))

    df = df.with_columns([
        pl.lit(data_hoje.year).alias("ano_ref"),
        pl.lit(data_hoje.month).alias("mes_ref"),
        pl.col("municipio").cast(pl.Utf8).fill_null("N/A")
    ])

    # 3. Join IBGE
    if os.path.exists(DE_PARA_IBGE):
        print("--- 🗺️ Enriquecendo com IBGE... ---")
        try:
            df_ibge = pl.read_csv(DE_PARA_IBGE, separator=";", ignore_errors=True)
            
            # Normaliza chaves (lowercase + strip)
            df = df.with_columns(pl.col("municipio").str.to_lowercase().str.strip_chars().alias("k"))
            df_ibge = df_ibge.select([
                pl.col("NOME_MUNICIPIO").str.to_lowercase().str.strip_chars().alias("k"),
                pl.col("COD").alias("cod_ibge")
            ])
            
            df = df.join(df_ibge, on="k", how="left")
            df = df.rename({"cod_ibge": "codigo_ibge"}).drop("k")
            df = df.with_columns(pl.col("codigo_ibge").fill_null(0))
        except Exception as e:
            print(f"⚠️ Erro IBGE: {e}")
            df = df.with_columns(pl.lit(0).alias("codigo_ibge"))
    else:
        df = df.with_columns(pl.lit(0).alias("codigo_ibge"))

    # ====================================================
    # ✋ TRAVA DE SEGURANÇA: MODO TESTE
    # ====================================================
    if modo_teste:
        arquivo_saida = os.path.join(DATA_FOLDER, "resultado_teste_visualizacao.csv")
        df.write_csv(arquivo_saida, separator=";")
        
        print("\n" + "="*50)
        print(f"✅ SUCESSO! Modo Teste Finalizado.")
        print(f"📄 Arquivo gerado para conferência: {arquivo_saida}")
        print(f"🛑 O BANCO DE DADOS NÃO FOI TOCADO.")
        print("="*50 + "\n")
        return 

    # 4. Gravação no Banco (Só acontece se modo_teste=False)
    print("--- 💾 Gravando no Banco de Dados... ---")
    engine = create_engine(DB_URI)
    df_pandas = df.to_pandas()
    
    cols_db = ["municipio", "codigo_ibge", "grande_grupamento_atividade", 
               "cnae_secao", "admitidos", "desligados", "saldo", 
               "variacao_relativa", "ano_ref", "mes_ref"]
    
    cols_finais = [c for c in cols_db if c in df_pandas.columns]
    
    df_pandas[cols_finais].to_sql(NOME_TABELA, engine, if_exists="append", index=False)
    print(f"✅ Dados inseridos com sucesso: {len(df_pandas)} linhas.")