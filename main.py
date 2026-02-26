import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io
from urllib.parse import unquote

def converter_hora_para_decimal(valor_hora):
    """Transforma '01:30' em 1.5 para o BigQuery aceitar como FLOAT"""
    try:
        if pd.isna(valor_hora) or valor_hora == "":
            return 0.0
        # Se já for número, apenas retorna
        if isinstance(valor_hora, (int, float)):
            return float(valor_hora)
        
        # Se for string formato HH:MM
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
        return "Sem dados no trigger", 200

    try:
        bucket_name = request_json.get("bucket")
        file_name = unquote(request_json.get("name"))

        # Filtro de pasta conforme sua estrutura
        if not (file_name.startswith("entrada/horas/") and file_name.endswith(".xlsx")):
            return "Arquivo ignorado (fora da pasta entrada/horas/)", 200

        print(f"--- Iniciando processamento: {file_name} ---")

        # Download do arquivo do GCS
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()

        # Leitura do Excel: Pula 11 linhas, cabeçalho está na 12 (index 11 do pandas)
        df = pd.read_excel(
            io.BytesIO(content), 
            sheet_name="Listagem de Horas", 
            skiprows=11, 
            engine='openpyxl'
        )

        # 1. Limpeza de espaços nos nomes das colunas do Excel
        df.columns = [str(c).strip() for c in df.columns]

        # 2. MAPEAMENTO DE COLUNAS (DE: Excel -> PARA: BigQuery)
        # Ajuste o lado esquerdo para bater EXATAMENTE com a imagem 2 que você enviou
        mapeamento = {
            'Voluntário': 'nome',
            'Data': 'data',
            'Horas': 'horas',
            'Livro': 'livro',
            'Localidade': 'localidade',
            'Função': 'funcoes',
            'Início': 'inicio',
            'Fim': 'fim',
            'Valor': 'valor'
        }
        
        df = df.rename(columns=mapeamento)

        # 3. Tratamento de Dados
        # Converter Data: de 01/01/16 para 2016-01-01
        if 'data' in df.columns:
            df['data'] = pd.to_datetime(df['data'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Converter Horas: de 01:30 para 1.5
        if 'horas' in df.columns:
            df['horas'] = df['horas'].apply(converter_hora_para_decimal)

        # 4. Seleção de Colunas (Somente o que existe na tabela 'horas' do seu BigQuery)
        colunas_validas = ['nome', 'data', 'horas', 'livro', 'localidade', 'funcoes', 'inicio', 'fim', 'valor']
        df_final = df[[c for c in colunas_validas if c in df.columns]].copy()

        # Remove linhas onde o nome está nulo (sujeira do Excel)
        df_final = df_final.dropna(subset=['nome'])

        # 5. Envio para o BigQuery
        client = bigquery.Client()
        table_id = "vltrs-rc.voluntarios.horas" # Dataset 'voluntarios' conforme imagem 1

        registros = df_final.to_dict(orient='records')
        
        if not registros:
            return "Nenhum registro válido encontrado no Excel", 200

        errors = client.insert_rows_json(table_id, registros)

        if errors == []:
            print(f"✅ SUCESSO: {len(registros)} linhas inseridas em {table_id}")
            return "Processamento concluído", 200
        else:
            print(f"❌ ERRO BigQuery: {errors}")
            return f"Erro na inserção: {errors}", 500

    except Exception as e:
        print(f"❌ ERRO CRÍTICO: {str(e)}")
        return f"Erro: {str(e)}", 500
