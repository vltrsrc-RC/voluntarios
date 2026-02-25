import functions_framework
from google.cloud import storage, bigquery
import pandas as pd
import io
from urllib.parse import unquote

@functions_framework.http
def converter_xlsx_para_bigquery(request):
    # O GCS envia um JSON via POST quando o arquivo é criado
    data = request.get_json(silent=True)
    
    if not data:
        return "Nenhum dado recebido", 400

    bucket_name = data.get("bucket")
    file_name = unquote(data.get("name"))

    print(f"Recebido: {file_name} no bucket {bucket_name}")

    # Filtros de segurança
    if not file_name.startswith("entrada/horas/") or not file_name.endswith(".xlsx"):
        return "Ignorado: Caminho ou formato inválido", 200

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_bytes()

        # Lendo o Excel (ajustado para o seu print: Listagem de Horas)
        df = pd.read_excel(io.BytesIO(content), sheet_name="Listagem de Horas", skiprows=11)
        df = df.dropna(how="all")

        # Configuração BigQuery
        client = bigquery.Client()
        table_id = "vltrs-rc.Voluntarios_RC.horas"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        return f"Sucesso: {file_name} processado.", 200

    except Exception as e:
        print(f"Erro: {str(e)}")
        return f"Erro interno: {str(e)}", 500
