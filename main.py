import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
from datetime import datetime
from urllib.parse import unquote

def ajustar_seculo(dt):
    """Corrige anos interpretados erroneamente (ex: 64 para 2064 em vez de 1964)"""
    if pd.isna(dt): 
        return None
    # Se a data resultante for maior que o ano atual, subtrai 100 anos
    if dt.year > datetime.now().year:
        return dt.replace(year=dt.year - 100)
    return dt

def converter_para_decimal(valor):
    """Converte formatos de hora HH:MM ou strings para float decimal"""
    try:
        if pd.isna(valor) or str(valor).strip() == "": 
            return 0.0
        if isinstance(valor, (int, float)): 
            return float(valor)
        str_val = str(valor).strip()
        if ':' in str_val:
            partes = str_val.split(':')
            # Ex: 01:30 -> 1 + (30/60) = 1.5
            return round(int(partes[0]) + (int(partes[1]) / 60), 2)
        return float(str_val.replace(',', '.'))
    except:
        return 0.0

def formatar_hh_mm_bq(valor):
    """Garante o formato HH:MM:00 exigido pelo tipo TIME do BigQuery"""
    try:
        if pd.isna(valor) or str(valor).strip() == "" or str(valor).lower() == 'nan':
            return None
        t = str(valor).strip()
        # Divide por ':' para isolar HH e MM e ignora segundos extras do Excel
        partes = t.split(':')
        if len(partes) >= 2:
            hh = partes[0].zfill(2)
            mm = partes[1].zfill(2)
            return f"{hh}:{mm}:00"
        return None
    except:
        return None

@functions_framework.http
def converter_xlsx_para_bigquery(request):
    request_json = request.get_json(silent=True)
    if not request_json: 
        return "OK", 200

    try:
        bucket_name = request_json.get("bucket")
        file_name = unquote(request_json.get("name"))

        # Filtro de segurança para processar apenas arquivos na pasta correta
        if not (file_name.startswith("entrada/horas/") and file_name.endswith(".xlsx")):
            return "Arquivo ignorado", 200

        # Download do arquivo do Cloud Storage
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()

        # Leitura do Excel: pula 12 linhas (cabeçalho está na 12, dados na 13)
        # header=None permite acessar colunas por índice (0=A, 1=B...)
        df = pd.read_excel(
            io.BytesIO(content), 
            sheet_name="Listagem de Horas", 
            skiprows=12, 
            header=None, 
            engine='openpyxl'
        )

        # 1. Limpeza de Linhas em Branco
        # Remove linhas onde a coluna Voluntário (Coluna H / Índice 7) está vazia
        df = df.dropna(subset=[7])
        df = df[df[7].astype(str).str.lower() != 'nan']

        # 2. Mapeamento para o DataFrame de Destino (STG)
        df_stg = pd.DataFrame()
        
        # Atributos de Texto
        df_stg['localidade'] = df[0].astype(str).str.strip()        # Coluna A
        df_stg['livro'] = df[2].astype(str).str.strip()             # Coluna C
        df_stg['voluntario'] = df[7].astype(str).str.strip()        # Coluna H
        df_stg['cpf'] = df[8].astype(str).str.strip()               # Coluna I
        
        # Datas (Tratamento de formato brasileiro e século)
        data_nasc_dt = pd.to_datetime(df[9], dayfirst=True, errors='coerce') # Coluna J
        df_stg['data_nascimento'] = data_nasc_dt.apply(ajustar_seculo).dt.strftime('%Y-%m-%d')
        
        data_trabalho_dt = pd.to_datetime(df[12], dayfirst=True, errors='coerce') # Coluna M
        df_stg['data'] = data_trabalho_dt.dt.strftime('%Y-%m-%d')
        
        # Números (Função, Horas, Valor)
        df_stg['funcao'] = df[11].apply(converter_para_decimal)     # Coluna L
        df_stg['horas'] = df[18].apply(converter_para_decimal)      # Coluna S (01:30 -> 1.5)
        df_stg['horas_descanso'] = df[19].apply(converter_para_decimal) # Coluna T
        df_stg['valor'] = df[20].apply(converter_para_decimal)      # Coluna U

        # Horários (Início e Fim como TIME)
        df_stg['inicio'] = df[14].apply(formatar_hh_mm_bq)          # Coluna O
        df_stg['fim'] = df[17].apply(formatar_hh_mm_bq)             # Coluna R

        # 3. Inserção no BigQuery
        client = bigquery.Client()
        table_id = "vltrs-rc.voluntarios.STG_Listagem_Horas"
        
        registros = df_stg.to_dict(orient='records')
        
        if not registros:
            return "Nenhum dado válido encontrado no Excel", 200

        errors = client.insert_rows_json(table_id, registros)

        if errors == []:
            print(f"✅ Sucesso: {len(registros)} linhas inseridas em {table_id}")
            return "Sucesso", 200
        else:
            print(f"❌ Erro BigQuery: {errors}")
            return f"Erro BQ: {errors}", 500

    except Exception as e:
        print(f"❌ Erro Crítico: {str(e)}")
        return f"Erro Geral: {str(e)}", 500
