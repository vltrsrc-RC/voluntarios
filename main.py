import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
from urllib.parse import unquote

@functions_framework.http
def converter_xlsx_para_bigquery(request):
    request_json = request.get_json(silent=True)
    if not request_json:
        return "OK", 200

    try:
        bucket_name = request_json.get("bucket")
        file_name = unquote(request_json.get("name"))

        # Filtro de pasta
        if not (file_name.startswith("entrada/horas/") and file_name.endswith(".xlsx")):
            return "Ignorado", 200

        print(f"--- Processando: {file_name} ---")

        # Download
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()

        # Leitura (Pula as 11 linhas iniciais)
        df = pd.read_excel(io.BytesIO(content), sheet_name="Listagem de Horas", skiprows=11, engine='openpyxl')

        # 1. Limpeza de Colunas: Remove espaços e caracteres especiais
        df.columns = [str(c).strip().replace(" ", "_").replace("/", "_").replace(".", "") for c in df.columns]

        # 2. FILTRO DE COLUNAS: Mantenha apenas as que existem na sua tabela 'horas'
        # Adicione aqui os nomes exatos das colunas que você criou no BigQuery
        colunas_que_eu_quero = ["Nome", "Data", "Horas", "Atividade"] 
        
        # Filtra apenas as colunas que batem com o que você quer e que existem no Excel
        df = df[[c for c in colunas_que_eu_quero if c in df.columns]]

        # 3. Remove linhas totalmente vazias ou sem o nome do voluntário
        df = df.dropna(how="all").dropna(subset=[df.columns[0]])

        # 4. Tratamento de Datas para JSON
        for col in df.select_dtypes(include=['datetime64']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # 5. Envio para o BigQuery (Dataset correto: voluntarios)
        client = bigquery.Client()
        table_id = "vltrs-rc.voluntarios.horas"  # Dataset corrigido conforme imagem

        registros = df.to_dict(orient='records')
        errors = client.insert_rows_json(table_id, registros)

        if errors == []:
            print(f"✅ Sucesso! {len(registros)} linhas inseridas em {table_id}")
            return "Sucesso", 200
        else:
            print(f"❌ Erro nas linhas: {errors}")
            return f"Erro: {errors}", 500

    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        return str(e), 500
