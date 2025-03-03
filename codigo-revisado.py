import re
import pandas as pd
import csv
from datetime import datetime
import json
import uuid
import os

def extract_metadata_fields(content):
    """Extrai campos específicos do metadata"""
    metadata = {
        'metadata_type': '',
        'metadata_service_type': '',
        'metadata_status': '',
        'metadata_name': '',
        'metadata_person_type': '',
        'metadata_msisdn': '',
        'metadata_cpf': ''
    }
    
    try:
        # Procura pelo objeto metadata no conteúdo
        metadata_match = re.search(r'metadata:\s*({.*})', content)
        if metadata_match:
            metadata_str = metadata_match.group(1)
            
            # Padrões de regex para cada campo
            service_type_pattern = r'"serviceType"":""([^""]+)""'
            type_pattern = r'"type"":""([^""]+)""'
            status_pattern = r'"status"":""([^""]+)""'
            name_pattern = r'"name"":""([^""]+)""'
            person_type_pattern = r'"personType"":""([^""]+)""'
            msisdn_pattern = r'"msisdn"":""([^""]+)""'
            cpf_pattern = r'"cpf"":""([^""]+)""'
            
            # Procura os campos
            service_type_match = re.search(service_type_pattern, metadata_str)
            type_match = re.search(type_pattern, metadata_str)
            status_match = re.search(status_pattern, metadata_str)
            name_match = re.search(name_pattern, metadata_str)
            person_type_match = re.search(person_type_pattern, metadata_str)
            msisdn_match = re.search(msisdn_pattern, metadata_str)
            cpf_match = re.search(cpf_pattern, metadata_str)
            
            # Extrai os valores encontrados
            if service_type_match:
                metadata['metadata_service_type'] = service_type_match.group(1)
            if type_match:
                metadata['metadata_type'] = type_match.group(1)
            if status_match:
                metadata['metadata_status'] = status_match.group(1)
            if name_match:
                metadata['metadata_name'] = name_match.group(1)
            if person_type_match:
                metadata['metadata_person_type'] = person_type_match.group(1)
            if msisdn_match:
                metadata['metadata_msisdn'] = msisdn_match.group(1)
            if cpf_match:
                metadata['metadata_cpf'] = cpf_match.group(1)
            
            print("Campos extraídos do metadata:", metadata)
            
    except Exception as e:
        print(f"Erro ao processar metadata: {str(e)}")
    
    return metadata

def sanitize_string(text):
    if text is None:
        return ""
    # Converte para string e remove caracteres nulos
    text = str(text)
    text = text.replace('\x00', '')
    
    # Se o texto parece ser JSON, tenta formatá-lo de maneira mais legível
    if text.startswith('{') and text.endswith('}'):
        try:
            # Remove aspas duplas escapadas e outros caracteres problemáticos
            text = text.replace('\\"', '"')  # Remove escape de aspas
            text = text.replace('""', '"')   # Corrige aspas duplas
            json_obj = json.loads(text)
            return json.dumps(json_obj, ensure_ascii=False, indent=2)
        except:
            pass
    
    # Remove caracteres que o Excel não suporta
    text = ''.join(char for char in text if ord(char) >= 32)
    
    # Limita o tamanho do texto
    if len(text) > 32000:
        text = text[:32000] + "..."
    
    return text

def split_message_content(message):
    """Separa a mensagem em duas partes usando | como delimitador"""
    if not message:
        return "", ""
    parts = message.split("|", 1)  # Split apenas na primeira ocorrência de |
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return message.strip(), ""

def extract_log_fields(line):
    # Regex para extrair os campos do log
    log_pattern = (
        r"date: (?P<datetime>[\d\-T:.Z]+)\s+\|\s+"
        r"(?P<message>[\w\s]+):\s+(?P<product>[\w\-]+)\s+\|\s+"
        r"product:\s+(?P<product_name>[^|]+)\|\s+"
        r"endpoint:\s+(?P<endpoint>[^|]+)\|\s+"
        r"transaction_id:\s+(?P<transaction_id>[^|]+)\|\s+"
        r"flow_id:\s+(?P<flow_id>[^|]+)\|\s+"
        r"message:\s+(?P<message_content>.*)"
    )
    
    match = re.search(log_pattern, line, re.DOTALL)
    if match:
        data = match.groupdict()
        # Limpa espaços em branco extras
        return {k: v.strip() if isinstance(v, str) else v for k, v in data.items()}
    return None

def process_log_line(line):
    try:
        # Remove espaços em branco extras e quebras de linha
        line = line.strip()
        
        # Ignora linhas vazias ou cabeçalho
        if not line or line.startswith("timestamp,message"):
            return None
        
        # Extrai os campos do log
        data = extract_log_fields(line)
        if data:
            # Extrai campos adicionais do message_content
            message = data['message_content']
            
            # Separa a mensagem de erro do resto do conteúdo
            message_error, remaining_content = split_message_content(message)
            data['message_error'] = message_error
            data['message_content'] = remaining_content
            
            # Tenta extrair service_type, type e status do conteúdo restante
            service_type_match = re.search(r"serviceType:\s*([\w\-]+)", remaining_content)
            type_match = re.search(r"type:\s*([\w\-]+)", remaining_content)
            status_match = re.search(r"status:\s*([\w\-]+)", remaining_content)
            
            # Adiciona os campos extraídos
            data['service_type'] = service_type_match.group(1) if service_type_match else ''
            data['type'] = type_match.group(1) if type_match else ''
            data['status'] = status_match.group(1) if status_match else ''
            
            # Extrai campos do metadata
            metadata_fields = extract_metadata_fields(remaining_content)
            data.update(metadata_fields)
            
            # Sanitiza apenas o conteúdo da mensagem
            data['message_content'] = sanitize_string(data['message_content'])
            
            return data
            
        return None
    except Exception as e:
        print(f"Erro ao processar linha: {str(e)}")
        return None

def process_log_file(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            processed_line = process_log_line(line)
            if processed_line:
                data.append(processed_line)
    
    if not data:
        print("Nenhuma linha correspondente foi encontrada no arquivo de log.")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    
    # Reordenar as colunas para uma melhor visualização
    column_order = ['datetime', 'message', 'product', 'product_name', 'endpoint', 
                   'transaction_id', 'flow_id', 'service_type', 'type', 'status',
                   'metadata_service_type', 'metadata_type', 'metadata_status',
                   'metadata_name', 'metadata_person_type', 'metadata_msisdn', 'metadata_cpf',
                   'message_error', 'message_content']
    
    # Garantir que todas as colunas existam
    for col in column_order:
        if col not in df.columns:
            df[col] = ''
    
    return df[column_order]

def convert_datetime_format(df):
    try:
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce')
    except Exception as e:
        print(f"Erro ao converter datas: {str(e)}")
    return df

def save_to_excel(df, output_file):
    try:
        # Cria uma cópia do DataFrame para não modificar o original
        df_save = df.copy()
        
        # Sanitiza apenas a coluna message_content
        df_save['message_content'] = df_save['message_content'].apply(sanitize_string)
        
        # Converte datetime para string no formato ISO
        if 'datetime' in df_save.columns:
            df_save['datetime'] = df_save['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Salva o arquivo
        df_save.to_excel(output_file, index=False, engine='openpyxl')
        print(f"Arquivo salvo com sucesso em: {output_file}")
    except Exception as e:
        print(f"Erro ao salvar arquivo Excel: {str(e)}")
        try:
            # Tenta salvar em CSV como fallback
            csv_file = output_file.replace('.xlsx', '.csv')
            df_save.to_csv(csv_file, index=False, encoding='utf-8')
            print(f"Arquivo salvo em CSV como fallback: {csv_file}")
        except Exception as csv_error:
            print(f"Erro ao salvar arquivo CSV: {str(csv_error)}")

# Caminho do arquivo de log
log_file_path = '/Users/work/Documents/logs.csv'

# Gera um ID único de 8 caracteres
unique_id = str(uuid.uuid4())[:8]

# Extrai o nome base do arquivo de log (sem extensão e caminho)
base_name = os.path.splitext(os.path.basename(log_file_path))[0]

# Cria o nome do arquivo de saída com o formato: nome_base-id_unico.xlsx
output_excel_file = f"{base_name}-{unique_id}.xlsx"

# Processar o arquivo de log
df_logs = process_log_file(log_file_path)

# Se o DataFrame não estiver vazio, prossiga com a exportação
if not df_logs.empty:
    # Converter o formato de data
    df_logs = convert_datetime_format(df_logs)

    # Salvar em Excel com o novo nome de arquivo
    save_to_excel(df_logs, output_excel_file)
else:
    print("Nenhuma linha foi processada com sucesso.")
