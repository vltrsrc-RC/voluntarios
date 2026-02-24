import functions_framework
from google.cloud import storage
import pandas as pd
import io

@functions_framework.cloud_event
def converter_xlsx_para_csv(cloud_event):

    data = cloud_event.data
    print("EVENTO RECEBIDO:", data)

    bucket_name = data["bucket"]
    file_name = data["name"]

    # Processar apenas arquivos da pasta correta
    if not file_name.startswith("entrada/horas/"):
        print("Arquivo fora da pasta entrada/horas. Ignorando.")
        return

    if not file_name.endswith(".xlsx"):
        print("Não é XLSX. Ignorando.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # 🔥 VERIFICA SE O ARQUIVO EXISTE
    if not blob.exists():
        print("Arquivo não encontrado no bucket:", file_name)
        return

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

    bucket.blob(novo_nome).upload_from_string(
        csv_buffer.getvalue(),
        content_type="text/csv"
    )

    print(f"Convertido com sucesso: {novo_nome}")
