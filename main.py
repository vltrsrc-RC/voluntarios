import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
from urllib.parse import unquote

def converter_para_decimal(valor):
    """Converte formatos de hora HH:MM ou strings para float"""
    try:
        if pd.isna(valor) or valor == "": return 0.0
        if isinstance(valor, (int, float)): return float(valor)
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
    if not request_json: return "OK", 200

    try:
        bucket_name = request_json.get("bucket")
        file_name = unquote(request_json.get("name"))

        if not (file_name.startswith("entrada/horas/") and file_name.endswith(".xlsx")):
            return "Ignorado", 200

        storage_client = storage.Client()
        content = storage_client.bucket(bucket_name).blob(file_name).download_as_bytes()

        # Lê o Excel sem cabeçalho para usar índices numéricos (0, 1, 2...)
        # skiprows=12 para começar exatamente nos dados (abaixo da linha 12)
        df = pd.read_excel(io.BytesIO(content), sheet_name="Listagem de Horas", skiprows=12, header=None, engine='openpyxl')

        # MAPEAMENTO POR COLUNA DO EXCEL (A=0, B=1, C=2...)
        # Criamos um novo DataFrame apenas com o que você pediu
        df_stg = pd.DataFrame()
        df_stg['localidade'] = df[0].astype(str)           # Coluna A
        df_stg['livro'] = df[2].astype(str)                # Coluna C
        df_stg['voluntario'] = df[7].astype(str)           # Coluna H (Voluntário)
        df_stg['cpf'] = df[8].astype(str)                  # Coluna I
        
        # Datas (J e M)
        df_stg['data_nascimento'] = pd.to_datetime(df[9], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        df_stg['data'] = pd.to_datetime(df[12], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Números e Horas (L, S, T, U)
        df_stg['funcao'] = df[11].apply(converter_para_decimal)   # Coluna L
        df_stg['horas'] = df[18].apply(converter_para_decimal)    # Coluna S
        df_stg['horas_descanso'] = df[19].apply(converter_para_decimal) # Coluna T
        df_stg['valor'] = df[20].apply(converter_para_decimal)    # Coluna U

        # Horários como Texto (O e R)
        df_stg['inicio'] = df[14].astype(str).str.strip()         # Coluna O
        df_stg['fim'] = df[17].astype(str).str.strip()            # Coluna R

        # Limpeza: remove linhas onde o nome do voluntário é nulo ou "nan"
        df_stg = df_stg[df_stg['voluntario'] != 'nan'].dropna(subset=['voluntario'])

        # Envio para BigQuery
        client = bigquery.Client()
        table_id = "vltrs-rc.voluntarios.STG_Listagem_Horas"
        
        registros = df_stg.to_dict(orient='records')
        errors = client.insert_rows_json(table_id, registros)

        if errors == []:
            print(f"✅ STG carregada: {len(registros)} linhas.")
            return "Sucesso", 200
        else:
            print(f"❌ Erros BQ: {errors}")
            return str(errors), 500

    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        return str(e), 500
