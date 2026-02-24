import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
from urllib.parse import unquote

@functions_framework.cloud_event
def converter_xlsx_para_bigquery(cloud_event):

    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = unquote(data["name"])

    if not file_name.startswith("entrada/horas/"):
        return

    if not file_name.endswith(".xlsx"):
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    content = blob.download_as_bytes()

    df = pd.read_excel(
        io.BytesIO(content),
        sheet_name="Listagem de Horas",
        skiprows=11
    )

    df = df.dropna(how="all")

    # 🔥 Aqui enviamos para BigQuery
    client = bigquery.Client()

    table_id = "SEU_PROJETO.Voluntarios_RC.horas"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # adiciona novos registros
        autodetect=True
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

    job.result()

    print(f"Dados enviados para BigQuery: {table_id}")
