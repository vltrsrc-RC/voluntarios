import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
import json
from urllib.parse import unquote

@functions_framework.http
def converter_xlsx_para_bigquery(request):
    # 1. Captura os dados do evento (GCS enviando via POST)
    request_json = request.get_json(silent=True)
    if not request_json:
        return "Nenhum dado recebido", 400

    try:
        bucket_name = request_json.get("bucket")
        file_name = unquote(request_json.get("name"))

        print(f"--- Processando arquivo: {file_name} ---")

        # 2. Filtro de segurança (pasta entrada/horas/ e extensão .xlsx)
        if not (file_name.startswith("entrada/horas/") and file_name.endswith(".xlsx")):
            print(f"Ignorado: {file_name} não está na pasta correta ou não é XLSX.")
            return "Caminho ignorado", 200

        # 3. Download do arquivo
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()

        # 4. Leitura do Excel com Pandas
        # Usamos skiprows=11 para começar na linha 12 conforme sua estrutura
        df = pd.read_excel(
            io.BytesIO(content),
            sheet_name="Listagem de Horas",
            skiprows=11,
            engine='openpyxl'
        )

        # 5. Limpeza e Tratamento de Dados
        df = df.dropna(how="all")  # Remove linhas vazias

        # Limpeza de nomes de colunas (BigQuery não aceita espaços ou caracteres especiais)
        df.columns = [
            str(c).strip()
            .replace(" ", "_")
            .replace("/", "_")
            .replace(".", "")
            .replace("(", "")
            .replace(")", "")
            for c in df.columns
        ]

        # Tratamento de Datas: Converte para string no formato ISO para evitar erro de serialização JSON
        for col in df.select_dtypes(include=['datetime64']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # 6. Conversão para JSON (Dicionário)
        # Isso elimina a necessidade da biblioteca pandas-gbq
        registros = df.to_dict(orient='records')
        
        if not registros:
            print("Aviso: O arquivo parece estar vazio após o processamento.")
            return "Arquivo vazio", 200

        # 7. Inserção no BigQuery
        bq_client = bigquery.Client()
        table_id = "vltrs-rc.Voluntarios_RC.horas"

        print(f"Tentando inserir {len(registros)} linhas na tabela {table_id}...")
        
        # Inserção direta via JSON
        errors = bq_client.insert_rows_json(table_id, registros)

        if errors == []:
            print(f"✅ Sucesso! Dados de {file_name} carregados no BigQuery.")
            return "Upload concluído com sucesso", 200
        else:
            print(f"❌ Erro na inserção das linhas: {errors}")
            return f"Erro BigQuery: {errors}", 500

    except Exception as e:
        error_msg = f"❌ Erro Crítico no Processamento: {str(e)}"
        print(error_msg)
        return error_msg, 500
