import subprocess
import os
import glob
import json
import polars as pl
from datetime import datetime

# --- CONFIGURAÇÕES ---
NODE_PROJECT_PATH = r"D:\Projetos\Observatorio"
INPUT_JSON_DIR = os.path.join(NODE_PROJECT_PATH, "output")
ANALYTICS_DIR = os.path.join(os.getcwd(), "analytics")

# Limite de arquivos no histórico
MAX_HISTORICO = 5

def gerenciar_historico_arquivos():
    """Mantém apenas os 5 arquivos mais recentes na pasta analytics"""
    # Lista todos os CSVs na pasta
    arquivos = glob.glob(os.path.join(ANALYTICS_DIR, "analise_*.csv"))
    
    # Ordena por data de criação (mais antigo primeiro)
    arquivos.sort(key=os.path.getmtime)
    
    # Enquanto tiver mais que o limite (ex: 6 arquivos), apaga o primeiro (o mais velho)
    while len(arquivos) >= MAX_HISTORICO:
        arquivo_velho = arquivos.pop(0)
        os.remove(arquivo_velho)
        print(f"🗑️ Limpeza de histórico: Removido {os.path.basename(arquivo_velho)}")

def executar_pipeline_analise():
    print(f"[{datetime.now()}] 🧪 Iniciando Modo Laboratório (Análise)...")

    # 1. Cria pasta analytics se não existir
    if not os.path.exists(ANALYTICS_DIR): os.makedirs(ANALYTICS_DIR)

    # 2. Executa Node.js
    print(f"--- 1. Rodando Extração Node.js ---")
    try:
        subprocess.run("npm run start:json", shell=True, check=True, cwd=NODE_PROJECT_PATH)
        print("✅ Dados brutos extraídos.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro no Node: {e}")
        return

    # 3. Pega o JSON mais recente
    arquivos_json = [f for f in os.listdir(INPUT_JSON_DIR) if f.endswith('.json')]
    if not arquivos_json:
        print("❌ Nenhum JSON encontrado.")
        return
    
    arquivo_json = max([os.path.join(INPUT_JSON_DIR, f) for f in arquivos_json], key=os.path.getctime)
    print(f"📂 Processando: {os.path.basename(arquivo_json)}")

    # 4. Leitura e Limpeza (FIX INF)
    try:
        with open(arquivo_json, 'r', encoding='utf-8') as f:
            conteudo = f.read().replace('"INF"', 'null').replace('"-INF"', 'null').replace('"NaN"', 'null')
        
        data = json.loads(conteudo)
        first_key = list(data.keys())[0] if isinstance(data, dict) else None
        lista = data[first_key] if first_key else data
        
        df = pl.DataFrame(lista, infer_schema_length=None)

    except Exception as e:
        print(f"❌ Erro leitura JSON: {e}")
        return

    # 5. Tratamento (Simulando a DAG)
    print("--- 2. Aplicando Regras de Negócio ---")
    
    if "undefined" in df.columns: df = df.drop("undefined")
    if "Geográfico.Município" in df.columns: df = df.rename({"Geográfico.Município": "municipio"})
    elif "Município" in df.columns: df = df.rename({"Município": "municipio"})

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

    # Tratamento de Nulos e Tipos
    df = df.with_columns([
        pl.col("grande_grupamento_atividade").fill_null("Indefinido"),
        pl.col("atividade_economica").fill_null("Indefinido"),
        pl.col("cnae_secao").fill_null("Indefinido"),
        pl.col("admitidos").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("desligados").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("saldo").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("estoque").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("variacao_relativa").cast(pl.Float64, strict=False).fill_null(0.0),
        pl.col("tempo_emprego").cast(pl.Float64, strict=False).fill_null(0.0),
    ])

    # 6. Salvar na Pasta Analytics (Com Versionamento)
    print("--- 3. Gerando Arquivo de Análise ---")
    
    # Limpa arquivos antigos antes de salvar o novo
    gerenciar_historico_arquivos()
    
    # Gera nome com Data e Hora (ex: analise_20231210_213005.csv)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"analise_{timestamp}.csv"
    caminho_final = os.path.join(ANALYTICS_DIR, nome_arquivo)
    
    # Seleciona colunas principais para visualização rápida
    colunas_finais = [
        "municipio", "grande_grupamento_atividade", "cnae_secao", 
        "admitidos", "desligados", "saldo", "variacao_relativa"
    ]
    cols = [c for c in df.columns if c in colunas_finais]
    
    df.select(cols).write_csv(caminho_final, separator=";")
    
    print(f"✅ Arquivo salvo: {nome_arquivo}")
    print(f"📂 Local: {ANALYTICS_DIR}")
    print("-" * 50)
    print("⚠️ NOTA: A pasta 'data' (Produção) NÃO foi alterada.")
    print("Para atualizar o banco oficial, execute a DAG no Airflow.")

if __name__ == "__main__":
    executar_pipeline_analise()