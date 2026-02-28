import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
from datetime import datetime
from urllib.parse import unquote



@functions_framework.http
def converter_xlsx_para_bigquery(request):
    request_json = request.get_json(silent=True)
    if not request_json: return "OK", 200

    try:
        bucket_name = request_json.get("bucket")
        file_name = unquote(request_json.get("name"))

        if not (file_name.startswith("entrada/horas/") and file_name.endswith(".xlsx")):
            return "Ignorado", 200

        storage_client = storage.Client()
        content = storage_client.bucket(bucket_name).blob(file_name).download_as_bytes()

        # Lê ignorando as 12 linhas iniciais
        df = pd.read_excel(io.BytesIO(content), sheet_name="Listagem de Horas", skiprows=12, header=None, engine='openpyxl')

        # Remove linhas sem voluntário
        df = df.dropna(subset=[7])
        df = df[df[7].astype(str).str.lower() != 'nan']

        df_stg = pd.DataFrame()
        # Mapeamento de Colunas
        df_stg['localidade'] = df[0].astype(str).str.strip()
        df_stg['livro'] = df[2].astype(str).str.strip()
        df_stg['voluntario'] = df[7].astype(str).str.strip()
        df_stg['cpf'] = df[8].astype(str).str.strip()
        
        client = bigquery.Client()
        table_id = "vltrs-rc.voluntarios.STG_Listagem_Horas"
        
        registros = df_stg.to_dict(orient='records')
        errors = client.insert_rows_json(table_id, registros)

        if errors == []:
            return "Sucesso", 200
        else:
            return f"Erro BQ: {errors}", 500

    except Exception as e:
        return f"Erro: {str(e)}", 500
