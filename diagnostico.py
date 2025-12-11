import json
import os

# Caminho do arquivo (ajuste se necessário)
# Ele busca na pasta requests do projeto Observatorio
PATH_ARQUIVO = r"D:\Projetos\Observatorio\requests\consolidado_municipios.json"

def encontrar_contexto(node, termo, path=""):
    """Varre o JSON e imprime o pedaço onde o termo aparece"""
    if isinstance(node, dict):
        # Se achar a propriedade com o nome que queremos (Ano ou Mês)
        if node.get("Property") == termo:
            print(f"\n--- ENCONTRADO: {termo} ---")
            print(f"Caminho: {path}")
            print(f"Conteúdo do nó: {json.dumps(node, indent=2)}")
            return

        # Continua procurando...
        for k, v in node.items():
            encontrar_contexto(v, termo, path + f".{k}")
            
    elif isinstance(node, list):
        for i, item in enumerate(node):
            encontrar_contexto(item, termo, path + f"[{i}]")

if __name__ == "__main__":
    if not os.path.exists(PATH_ARQUIVO):
        print(f"❌ Arquivo não encontrado: {PATH_ARQUIVO}")
        # Tenta com extensão .DS0.json caso o nome seja diferente
        PATH_ARQUIVO = PATH_ARQUIVO.replace(".json", ".DS0.json")
    
    if os.path.exists(PATH_ARQUIVO):
        print(f"📂 Lendo arquivo: {PATH_ARQUIVO}")
        with open(PATH_ARQUIVO, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print("🔍 Procurando por 'Ano'...")
        encontrar_contexto(data, "Ano")
        
        print("🔍 Procurando por 'Mês'...")
        encontrar_contexto(data, "Mês")
    else:
        print("❌ Arquivo realmente não encontrado. Verifique o nome na pasta requests.")