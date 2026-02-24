import functions_framework
from google.cloud import storage
import pandas as pd
import io

@functions_framework.cloud_event
def converter_xlsx_para_csv(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

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

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, sep=";")

    novo_nome = file_name.replace("entrada/", "processados/").replace(".xlsx", ".csv")

    novo_blob = bucket.blob(novo_nome)
    novo_blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    print(f"Convertido: {novo_nome}")
