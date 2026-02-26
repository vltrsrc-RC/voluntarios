import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
import re
from urllib.parse import unquote

def converter_hora_para_decimal(valor_hora):
    """Converte '01:30' para 1.5"""
    try:
        if pd.isna(valor_hora) or valor_hora == "":
            return 0.0
        if isinstance(valor_hora, (int, float)):
            return float(valor_hora)
        
        partes = str(valor_hora).strip().split(':')
        if len(partes) >= 2:
            horas = int(partes[0])
            minutos = int(partes[1])
            return round(horas + (minutos / 60), 2)
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

        # Download
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()

        # Leitura do Excel (Cabeçalho na linha 12)
        df = pd.read_excel(io.BytesIO(content), sheet_name="Listagem de Horas", skiprows=11, engine='openpyxl')

        # 1. Limpeza rigorosa dos nomes das colunas vindas do Excel
        df.columns = [str(c).strip() for c in df.columns]

        # 2. DICIONÁRIO DE MAPEAMENTO (Excel -> BigQuery)
        # O lado esquerdo deve ser idêntico ao que está na imagem 2 do seu Excel
        mapeamento = {
            'Localidade': 'localidade',
            'Livro': 'livro',
            'Voluntário': 'voluntario',
            'Data Nasc.': 'data_nascimento',
            'Função': 'funcoes',
            'Data': 'data',
            'Início': 'inicio',
            'Fim': 'fim',
            'Horas': 'horas',
            'Valor': 'valor'
        }

        # Renomeia as colunas
        df = df.rename(columns=mapeamento)

        # 3. Tratamento de Dados Específicos
        # Datas (Trata tanto a data do trabalho quanto a de nascimento)
        for col_data in ['data', 'data_nascimento']:
            if col_data in df.columns:
                df[col_data] = pd.to_datetime(df[col_data], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Horas (HH:MM -> Decimal)
        if 'horas' in df.columns:
            df['horas'] = df['horas'].apply(converter_hora_para_decimal)

        # Campos de Hora (Início/Fim) - BigQuery espera STRING ou TIME, garantimos String HH:MM
        for col_time in ['inicio', 'fim']:
            if col_time in df.columns:
                df[col_time] = df[col_time].astype(str).str.strip()

        # 4. Criação de colunas que existem no BQ mas não no Excel (como nome_normalizado)
        if 'nome' in df.columns:
            df['nome_normalizado'] = df['nome'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

        # 5. Filtragem Final: Manter apenas o que o BigQuery aceita
        colunas_destino = [
            'localidade', 'livro', 'nome', 'nome_normalizado', 
            'data_nascimento', 'funcoes', 'data', 'inicio', 'fim', 'horas', 'valor'
        ]
        
        # Filtra e remove linhas onde o nome é nulo (sujeira do rodapé do Excel)
        df_final = df[[c for c in colunas_destino if c in df.columns]].copy()
        df_final = df_final.dropna(subset=['nome'])

        # 6. Envio para o BigQuery
        client = bigquery.Client()
        table_id = "vltrs-rc.voluntarios.horas"

        registros = df_final.to_dict(orient='records')
        
        if not registros:
            return "Nenhum dado válido", 200

        errors = client.insert_rows_json(table_id, registros)

        if errors == []:
            print(f"✅ SUCESSO: {len(registros)} linhas em {table_id}")
            return "Sucesso", 200
        else:
            print(f"❌ ERRO BQ: {errors}")
            return str(errors), 500

    except Exception as e:
        print(f"❌ ERRO CRÍTICO: {str(e)}")
        return str(e), 500
