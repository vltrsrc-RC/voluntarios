import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
from urllib.parse import unquote

def converter_hora_para_decimal(valor_hora):
    """Transforma '01:30' em 1.5"""
    try:
        if pd.isna(valor_hora) or valor_hora == "":
            return 0.0
        if isinstance(valor_hora, (int, float)):
            return float(valor_hora)
        
        partes = str(valor_hora).split(':')
        if len(partes) == 2:
            horas = int(partes[0])
            minutos = int(partes[1])
            return horas + (minutos / 60)
        return float(valor_hora)
    except:
        return 0.0

@functions_framework.http
def converter_xlsx_para_bigquery(request):
    request_json = request.get_json(silent=True)
    if not request_json:
        return "OK", 200

    try:
        bucket_name = request_json.get("bucket")
        file_name = unquote(request_json.get("name"))

        if not (file_name.startswith("entrada/horas/") and file_name.endswith(".xlsx")):
            return "Ignorado", 200

        print(f"--- Processando: {file_name} ---")

        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()

        # Leitura
        df = pd.read_excel(io.BytesIO(content), sheet_name="Listagem de Horas", skiprows=11, engine='openpyxl')

        # 1. Limpeza de cabeçalhos
        df.columns = [str(c).strip().replace(" ", "_").replace("/", "_").replace(".", "") for c in df.columns]

        # 2. Mapeamento das colunas (Ajuste os nomes conforme seu Excel)
        # Supondo que as colunas no Excel chamem 'Data' e 'Horas' após a limpeza
        if 'Data' in df.columns:
            df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        
        if 'Horas' in df.columns:
            df['Horas'] = df['Horas'].apply(converter_hora_para_decimal)

        # 3. Filtro de colunas desejadas (devem bater com o seu BigQuery)
        colunas_finais = ["Nome", "Data", "Horas", "Atividade"]
        df = df[[c for c in colunas_finais if c in df.columns]]
        df = df.dropna(subset=[df.columns[0]]) # Remove linhas onde o primeiro campo está vazio

        # 4. Envio
        client = bigquery.Client()
        table_id = "vltrs-rc.voluntarios.horas"
        
        registros = df.to_dict(orient='records')
        errors = client.insert_rows_json(table_id, registros)

        if errors == []:
            print(f"✅ Sucesso! {len(registros)} linhas inseridas.")
            return "Sucesso", 200
        else:
            print(f"❌ Erro BigQuery: {errors}")
            return f"Erro: {errors}", 500

    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        return str(e), 500
