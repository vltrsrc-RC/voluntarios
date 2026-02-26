import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
from datetime import datetime
from urllib.parse import unquote

def ajustar_seculo(dt):
    """Corrige anos interpretados como 2064 para 1964"""
    if pd.isna(dt): return None
    if dt.year > datetime.now().year:
        return dt.replace(year=dt.year - 100)
    return dt

def converter_para_decimal(valor):
    """Converte valores do Excel para float (01:30 -> 1.5). Retorna 0.0 se vazio"""
    try:
        if pd.isna(valor) or str(valor).strip() == "" or str(valor).lower() == 'nan': 
            return 0.0
        if isinstance(valor, (int, float)): return float(valor)
        str_val = str(valor).strip().replace(',', '.')
        if ':' in str_val:
            partes = str_val.split(':')
            return round(int(partes[0]) + (int(partes[1]) / 60), 2)
        return float(str_val)
    except:
        return 0.0

def formatar_para_string_hh_mm(valor):
    """Formata apenas como HH:MM para colunas STRING"""
    try:
        if pd.isna(valor) or str(valor).strip() == "" or str(valor).lower() == 'nan':
            return "00:00"
        t = str(valor).strip()
        if ' ' in t: t = t.split(' ')[-1] # Remove data se houver
        partes = t.split(':')
        if len(partes) >= 2:
            return f"{partes[0].zfill(2)}:{partes[1].zfill(2)}"
        return "00:00"
    except:
        return "00:00"

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
        
        # Datas (DATE)
        df_stg['data_nascimento'] = pd.to_datetime(df[9], dayfirst=True, errors='coerce').apply(ajustar_seculo).dt.strftime('%Y-%m-%d')
        df_stg['data'] = pd.to_datetime(df[12], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Horários (STRING)
        df_stg['inicio'] = df[14].apply(formatar_para_string_hh_mm)
        df_stg['fim'] = df[17].apply(formatar_para_string_hh_mm)
        
        # Números (FLOAT64)
        df_stg['funcao'] = df[11].apply(converter_para_decimal)
        df_stg['horas'] = df[18].apply(converter_para_decimal)
        df_stg['horas_descanso'] = df[19].apply(converter_para_decimal)
        df_stg['valor'] = df[20].apply(converter_para_decimal)

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
