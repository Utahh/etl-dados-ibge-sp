import sys
import os

# Adiciona diretório atual ao path para importar o etl
sys.path.append(os.path.dirname(__file__))

try:
    from etl_municipios import processar_arquivos
except ImportError:
    # Fallback se rodar da raiz
    sys.path.append(os.path.join(os.path.dirname(__file__), '../dags'))
    from etl_municipios import processar_arquivos

def executar_teste_manual():
    print(">>> INICIANDO PIPELINE MANUAL (SOMENTE LEITURA) <<<")
    
    # AQUI ESTÁ A SEGURANÇA: modo_teste=True
    # Isso garante que ele só vai gerar CSV e não grava no banco.
    processar_arquivos(modo_teste=True)

if __name__ == "__main__":
    executar_teste_manual()