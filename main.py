import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
from urllib.parse import unquote

@functions_framework.http
def converter_xlsx_para_bigquery(request):
    # 1. Recebe a notificação do Eventarc/GCS
    request_json = request.get_json(silent=True)
    
    if not request_json:
        return "Nenhum dado recebido (JSON vazio)", 400

    try:
        bucket_name = request_json.get("bucket")
        file_name = unquote(request_json.get("name"))

        print(f"--- Iniciando processamento: {file_name} ---")

        # 2. Filtro de segurança (pasta e extensão)
        if not (file_name.startswith("entrada/horas/") and file_name.endswith(".xlsx")):
            print(f"Arquivo ignorado por não atender ao critério de caminho: {file_name}")
            return "Ignorado", 200

        # 3. Download do arquivo do Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_bytes()
        print("Download do arquivo concluído.")

        # 4. Leitura do Excel com Pandas
        # Usamos engine='openpyxl' para garantir compatibilidade com .xlsx
        df = pd.read_excel(
            io.BytesIO(content),
            sheet_name="Listagem de Horas",
            skiprows=11,
            engine='openpyxl'
        )

        # 5. Limpeza de dados
        # Remove linhas que estão totalmente vazias
        df = df.dropna(how="all")

        # TRATAMENTO DE COLUNAS: O BigQuery exige nomes sem espaços, acentos ou pontos.
        # Ex: "Data Início" vira "Data_Inicio"
        df.columns = [
            str(c).strip()
            .replace(" ", "_")
            .replace("/", "_")
            .replace(".", "")
            .replace("(", "")
            .replace(")", "")
            for c in df.columns
        ]
        
        print(f"Colunas tratadas: {df.columns.tolist()}")

        # 6. Envio para o BigQuery
        client = bigquery.Client()
        table_id = "vltrs-rc.Voluntarios_RC.horas"

        # Configuração da Carga:
        # WRITE_APPEND adiciona dados. Autodetect tenta resolver o schema.
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True,
            # Se você mudar o arquivo e adicionar colunas no futuro, isso permite:
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )

        print(f"Enviando {len(df)} linhas para a tabela {table_id}...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        
        # Espera o job terminar
        job.result()

        print(f"✅ Sucesso! Arquivo {file_name} carregado no BigQuery.")
        return f"Sucesso: {file_name} processado.", 200

    except Exception as e:
        # Erro detalhado nos logs do Cloud Run
        error_msg = f"❌ ERRO no processamento: {str(e)}"
        print(error_msg)
        return error_msg, 500
