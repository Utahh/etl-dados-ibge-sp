import subprocess
import os
import json
import polars as pl
from datetime import datetime, date
from sqlalchemy import create_engine, text

# --- CONFIGURAÇÕES LOCAIS ---
# Como você está no Windows, os caminhos são relativos à pasta onde você roda o script
NODE_PROJECT_PATH = "."  # Assume que está na raiz
OUTPUT_DIR = "./output"
CSV_IBGE = "./data/de_para_ibge.csv" # Verifique se o arquivo está aqui

# Conexão Local (Docker exposto no localhost)
DB_URI = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
NOME_TABELA = "tb_municipios_sp_v18_final_fix"

def run_pipeline_local():
    print(f"[{datetime.now()}] 🚀 Iniciando Pipeline Local...")

    # 1. Executa o Node.js
    print("--- 1. Rodando Extração (Node) ---")
    try:
        # No Windows usamos shell=True
        subprocess.run("npm run start:json", shell=True, check=True)
        print("✅ Node finalizado.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro no Node: {e}")
        return

    # 2. Busca o Arquivo JSON
    arquivos = [f for f in os.listdir(OUTPUT_DIR) if f.endswith('.json')]
    if not arquivos:
        print("❌ Nenhum JSON gerado.")
        return
    
    arquivo_json = os.path.join(OUTPUT_DIR, arquivos[0])
    print(f"📂 Processando: {arquivo_json}")

    # 3. Leitura Blindada (FIX INF)
    try:
        with open(arquivo_json, 'r', encoding='utf-8') as f:
            conteudo_bruto = f.read()
        
        # --- A CORREÇÃO MÁGICA ---
        # Troca INF por null antes de processar
        conteudo_limpo = conteudo_bruto.replace('"INF"', 'null').replace('"-INF"', 'null').replace('"NaN"', 'null')
        
        data = json.loads(conteudo_limpo)
        
        first_key = list(data.keys())[0] if isinstance(data, dict) else None
        lista = data[first_key] if first_key else data
        
        # infer_schema_length=None obriga ler tudo para não errar tipo
        df = pl.DataFrame(lista, infer_schema_length=None)

    except Exception as e:
        print(f"❌ Erro ao ler JSON: {e}")
        return

    # 4. Tratamento
    print("--- 2. Tratando Dados (Polars) ---")
    
    if "undefined" in df.columns: df = df.drop("undefined")
    if "Geográfico.Município" in df.columns: df = df.rename({"Geográfico.Município": "municipio"})
    elif "Município" in df.columns: df = df.rename({"Município": "municipio"})

    # Renomeação
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
    cols_existentes = [c for c in mapa_colunas.keys() if c in df.columns]
    df = df.rename({k: v for k, v in mapa_colunas.items() if k in cols_existentes})

    # Join com IBGE (se existir)
    if os.path.exists(CSV_IBGE):
        try:
            try:
                df_ibge = pl.read_csv(CSV_IBGE, separator=";", ignore_errors=True)
                if len(df_ibge.columns) <= 1: df_ibge = pl.read_csv(CSV_IBGE, separator=",", ignore_errors=True)
            except:
                df_ibge = pl.read_csv(CSV_IBGE, separator=",", ignore_errors=True)

            df_main = df.with_columns(pl.col("municipio").str.to_lowercase().str.strip_chars().alias("join_key"))
            col_nome = "NOME" if "NOME" in df_ibge.columns else df_ibge.columns[-1]
            col_cod = "COD" if "COD" in df_ibge.columns else df_ibge.columns[1]
            
            df_ibge = df_ibge.select([
                pl.col(col_nome).str.to_lowercase().str.strip_chars().alias("join_key"),
                pl.col(col_cod).alias("codigo_ibge_temp")
            ])
            
            df = df_main.join(df_ibge, on="join_key", how="left")
            df = df.rename({"codigo_ibge_temp": "codigo_ibge"}).drop("join_key")
        except:
            df = df.with_columns(pl.lit(0).alias("codigo_ibge"))
    else:
        print("⚠️ Sem arquivo IBGE, código será 0")
        df = df.with_columns(pl.lit(0).alias("codigo_ibge"))

    # Colunas de Data
    ano_atual = datetime.now().year
    mes_atual_num = 10 
    data_cal = date(ano_atual, mes_atual_num, 1)

    df = df.with_columns([
        pl.lit(ano_atual).cast(pl.Int64).alias("ano_ref"),
        pl.lit(mes_atual_num).cast(pl.Int64).alias("mes_ref"),
        pl.lit(data_cal).alias("data_competencia")
    ])

    # 5. Tratamento de Tipos (FIX INTEGERS)
    print("--- 3. Convertendo Tipos ---")
    df = df.with_columns([
        pl.col("grande_grupamento_atividade").fill_null("Indefinido"),
        pl.col("atividade_economica").fill_null("Indefinido"),
        pl.col("cnae_secao").fill_null("Indefinido"),
        
        # Inteiros
        pl.col("codigo_ibge").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("admitidos").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("desligados").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("saldo").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("estoque").cast(pl.Int64, strict=False).fill_null(0),
        
        # Floats
        pl.col("variacao_relativa").cast(pl.Float64, strict=False).fill_null(0.0),
        pl.col("tempo_emprego").cast(pl.Float64, strict=False).fill_null(0.0),
    ])

    # 6. Carga no Banco
    print(f"--- 4. Carregando no Banco: {NOME_TABELA} ---")
    engine = create_engine(DB_URI)
    
    # Cria tabela se não existir
    create_sql = f"""
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
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        # Limpa dados do mês para evitar duplicidade
        conn.execute(text(f"DELETE FROM {NOME_TABELA} WHERE ano_ref = :ano AND mes_ref = :mes"), 
                     {"ano": ano_atual, "mes": mes_atual_num})

    # Seleciona colunas
    colunas_permitidas = [
        "municipio", "codigo_ibge", "grande_grupamento_atividade", 
        "atividade_economica", "cnae_secao", "cnae_divisao", "cnae_grupo", 
        "cnae_classe", "cnae_subclasse", "admitidos", "desligados", 
        "saldo", "estoque", "variacao_relativa", "tempo_emprego", 
        "ano_ref", "mes_ref", "data_competencia"
    ]
    cols = [c for c in df.columns if c in colunas_permitidas]
    
    pdf = df.select(cols).to_pandas()
    pdf.to_sql(NOME_TABELA, engine, if_exists="append", index=False)
    
    print(f"✅ Sucesso! {len(pdf)} registros inseridos.")

if __name__ == "__main__":
    run_pipeline_local()