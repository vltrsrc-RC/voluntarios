import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
from datetime import datetime
from urllib.parse import unquote

def ajustar_seculo(dt):
    """Corrige anos interpretados como futuro (ex: 2064 para 1964)"""
    if pd.isna(dt):
        return None
    # Se o ano for maior que o atual, subtrai 100 anos
    if dt.year > datetime.now().year:
        return dt.replace(year=dt.year - 100)
    return dt

def converter_para_decimal(valor):
    """Converte HH:MM para float (ex: 01:30 -> 1.5)"""
    try:
        if pd.isna(valor) or str(valor).strip() == "": 
            return 0.0
        if isinstance(valor, (int, float)): 
            return float(valor)
        str_val = str(valor).strip()
        if ':' in str_val:
            partes = str_val.split(':')
            return round(int(partes[0]) + (int(partes[1]) / 60), 2)
        return float(str_val.replace(',', '.'))
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
            return "Arquivo ignorado", 200

        storage_client = storage.Client()
        content = storage_client.bucket(bucket_name).blob(file_name).download_as_bytes()

        # Leitura a partir da linha 13 (skiprows=12)
        df = pd.read_excel(io.BytesIO(content), sheet_name="Listagem de Horas", skiprows=12, header=None, engine='openpyxl')

        # Limpeza de linhas em branco baseada na coluna do voluntário (H=7)
        df = df.dropna(subset=[7])
        df = df[df[7].astype(str).str.lower() != 'nan']

        df_stg = pd.DataFrame()
        
        # Mapeamento de texto
        df_stg['localidade'] = df[0].astype(str).str.strip()
        df_stg['livro'] = df[2].astype(str).str.strip()
        df_stg['voluntario'] = df[7].astype(str).str.strip()
        df_stg['cpf'] = df[8].astype(str).str.strip()
        
        # CORREÇÃO DE DATAS
        # 1. Converte para objeto datetime primeiro
        data_nasc_dt = pd.to_datetime(df[9], dayfirst=True, errors='coerce')
        data_trabalho_dt = pd.to_datetime(df[12], dayfirst=True, errors='coerce')

        # 2. Aplica ajuste de século (para evitar 2064 em vez de 1964)
        df_stg['data_nascimento'] = data_nasc_dt.apply(ajustar_seculo).dt.strftime('%Y-%m-%d')
        df_stg['data'] = data_trabalho_dt.dt.strftime('%Y-%m-%d')
        
        # CONVERSÃO DE HORAS E VALORES
        df_stg['funcao'] = df[11].apply(converter_para_decimal)
        df_stg['horas'] = df[18].apply(converter_para_decimal)      # 01:30 -> 1.5
        df_stg['horas_descanso'] = df[19].apply(converter_para_decimal) # 00:00 -> 0.0
        df_stg['valor'] = df[20].apply(converter_para_decimal)

        # Início e Fim (String)
        df_stg['inicio'] = df[14].astype(str).str.strip()
        df_stg['fim'] = df[17].astype(str).str.strip()

        # Envio para BigQuery
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
