#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMS Analytics ETL Pipeline - Versão Final
Solução definitiva para EMS Project - Campinas
"""

import os
import sys
import logging
from io import BytesIO, StringIO
from datetime import datetime, timezone
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# Configurar encoding UTF-8 para Windows
if sys.platform.startswith('win'):
    import locale
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
        except locale.Error:
            pass  # Usar configuração padrão do sistema

# Carregar variáveis de ambiente
load_dotenv('config/.env')

# Configuração de logging sem emojis
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ems_etl.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

class Config:
    """Configurações específicas para EMS Project"""
    
    # Google Cloud
    PROJECT_ID = os.getenv('PROJECT_ID', 'dados-ems-project')
    DATASET_RAW = os.getenv('DATASET_RAW', 'ems_raw')
    DATASET_STAGING = os.getenv('DATASET_STAGING', 'ems_staging')
    DATASET_ANALYTICS = os.getenv('DATASET_ANALYTICS', 'ems_analytics')
    
    # Google Drive
    DRIVE_PASTA_NOVOS = os.getenv('DRIVE_PASTA_NOVOS')
    DRIVE_PASTA_ARMAZENADOS = os.getenv('DRIVE_PASTA_ARMAZENADOS')
    
    # Credenciais
    GCP_CREDENTIALS_PATH = 'config/gcp-credentials.json'
    
    # Cliente EMS Project
    CLIENTE_RAZAO_SOCIAL = os.getenv('CLIENTE_RAZAO_SOCIAL', 'EMS Project LTDA')
    CLIENTE_CNPJ = os.getenv('CLIENTE_CNPJ')
    CLIENTE_INSCRICAO_MUNICIPAL = os.getenv('CLIENTE_INSCRICAO_MUNICIPAL')

class GoogleDriveManager:
    """Gerenciador do Google Drive para EMS Project"""
    
    def __init__(self, credentials_path):
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/drive']
            )
            self.service = build('drive', 'v3', credentials=self.credentials)
            logger.info("Google Drive conectado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao conectar Google Drive: {e}")
            raise

    def list_files(self, folder_id):
        """Listar arquivos na pasta especificada"""
        try:
            query = f"'{folder_id}' in parents and trashed=false"
            fields = "files(id, name, mimeType, size, modifiedTime)"
            
            results = self.service.files().list(
                q=query,
                fields=fields,
                pageSize=1000
            ).execute()
            
            files = results.get('files', [])
            logger.info(f"Encontrados {len(files)} arquivos na pasta")
            return files
            
        except HttpError as e:
            logger.error(f"Erro ao listar arquivos: {e}")
            return []

    def download_file(self, file_id, file_name):
        """Baixar arquivo do Google Drive"""
        try:
            request = self.service.files().get_media(fileId=file_id)
            file_buffer = BytesIO()
            
            # Download do arquivo
            import googleapiclient.http
            downloader = googleapiclient.http.MediaIoBaseDownload(file_buffer, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            file_buffer.seek(0)
            logger.info(f"Arquivo baixado: {file_name}")
            return file_buffer
            
        except Exception as e:
            logger.error(f"Erro ao baixar arquivo {file_name}: {e}")
            return None

    def move_file(self, file_id, source_folder_id, destination_folder_id):
        """Mover arquivo entre pastas"""
        try:
            # Remover da pasta origem e adicionar na pasta destino
            self.service.files().update(
                fileId=file_id,
                addParents=destination_folder_id,
                removeParents=source_folder_id
            ).execute()
            
            logger.info(f"Arquivo movido para pasta de armazenados")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao mover arquivo: {e}")
            return False

class DataProcessor:
    """Processador de dados específico para EMS Project"""
    
    def __init__(self):
        self.config = Config()
    
    def identify_file_type(self, file_name):
        """Identificar tipo de arquivo baseado no nome"""
        file_name_lower = file_name.lower()
        
        if 'faturamento' in file_name_lower and any(year in file_name_lower for year in ['2006', '2007', '2008', '2009', '2010']):
            return 'faturamento_historico'
        elif 'empresas antigas' in file_name_lower:
            return 'empresas_antigas'
        elif 'todos clientes' in file_name_lower:
            return 'clientes_atuais'
        else:
            return 'generico'
    
    def normalize_column_names(self, df):
        """Normalizar nomes das colunas para BigQuery"""
        import unicodedata
        
        new_columns = []
        for col in df.columns:
            # Limpar caracteres especiais e espaços
            clean_col = str(col).strip().replace('\n', ' ').replace('\r', '').replace('  ', ' ')
            
            # Remover acentos e caracteres especiais
            clean_col = unicodedata.normalize('NFD', clean_col)
            clean_col = ''.join(char for char in clean_col if unicodedata.category(char) != 'Mn')
            
            # Substituir caracteres problemáticos para BigQuery
            clean_col = clean_col.replace('.', '_')
            clean_col = clean_col.replace('º', '_num')
            clean_col = clean_col.replace('ª', '_a')
            clean_col = clean_col.replace(' ', '_')
            clean_col = clean_col.replace('/', '_')
            clean_col = clean_col.replace('-', '_')
            clean_col = clean_col.replace('(', '')
            clean_col = clean_col.replace(')', '')
            clean_col = clean_col.replace('&', '_e_')
            clean_col = clean_col.replace('%', '_pct')
            clean_col = clean_col.replace('#', '_num')
            clean_col = clean_col.replace('°', '_grau')
            
            # Garantir que comece com letra ou underscore
            if clean_col and clean_col[0].isdigit():
                clean_col = 'col_' + clean_col
            
            # Remover underscores consecutivos
            while '__' in clean_col:
                clean_col = clean_col.replace('__', '_')
            
            # Remover underscore no final
            clean_col = clean_col.rstrip('_')
            
            # Tratar colunas vazias ou problemáticas
            if not clean_col or clean_col == 'Unnamed' or 'unnamed' in clean_col.lower():
                clean_col = f'coluna_{len(new_columns)}'
            
            new_columns.append(clean_col)
        
        df.columns = new_columns
        return df
    
    def clean_dataframe(self, df):
        """Limpeza completa do DataFrame"""
        if df.empty:
            return df
        
        # Fazer cópia para evitar warnings
        df = df.copy()
        
        # Remover linhas completamente vazias
        df = df.dropna(how='all')
        
        # Normalizar nomes das colunas
        df = self.normalize_column_names(df)
        
        # Remover colunas completamente vazias
        df = df.dropna(axis=1, how='all')
        
        # Resolver colunas duplicadas
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            indices = cols[cols == dup].index.values.tolist()
            for i, idx in enumerate(indices):
                if i > 0:
                    cols.iloc[idx] = f"{dup}_{i}"
        df.columns = cols
        
        # Limpeza de dados
        for col in df.columns:
            # Converter tudo para string primeiro
            df[col] = df[col].astype(str)
            
            # Limpar valores problemáticos
            df[col] = df[col].str.strip()
            df[col] = df[col].replace(['nan', 'None', 'NaT', '.', ''], '')
            
            # Converter frações para decimais
            def convert_fraction(val):
                try:
                    if isinstance(val, str) and '/' in val and val.count('/') == 1:
                        parts = val.split('/')
                        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                            return str(round(float(parts[0]) / float(parts[1]), 6))
                    return val
                except:
                    return val
            
            df[col] = df[col].apply(convert_fraction)
        
        # Adicionar metadados EMS
        df['cliente_nome'] = self.config.CLIENTE_RAZAO_SOCIAL
        df['data_processamento'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        df['origem_arquivo'] = 'google_drive'
        
        return df
    
    def process_excel_file(self, file_buffer, file_name):
        """Processar arquivo Excel específico da EMS"""
        try:
            logger.info(f"Processando: {file_name}")
            
            # Identificar tipo do arquivo
            file_type = self.identify_file_type(file_name)
            logger.info(f"Tipo identificado: {file_type}")
            
            # Ler arquivo Excel
            xls = pd.ExcelFile(file_buffer)
            logger.info(f"Abas encontradas: {len(xls.sheet_names)}")
            
            all_data = []
            
            for sheet_name in xls.sheet_names:
                try:
                    # Ler aba
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    
                    if not df.empty:
                        # Limpeza específica
                        df = self.clean_dataframe(df)
                        
                        # Adicionar metadados da aba
                        df['nome_aba'] = sheet_name
                        df['nome_arquivo'] = file_name
                        df['tipo_arquivo'] = file_type
                        
                        all_data.append(df)
                        
                except Exception as e:
                    logger.warning(f"Erro ao processar aba {sheet_name}: {e}")
                    continue
            
            if all_data:
                # Combinar todos os dados
                combined_df = pd.concat(all_data, ignore_index=True, sort=False)
                logger.info(f"Processadas {len(combined_df)} linhas de {len(xls.sheet_names)} abas")
                return combined_df, file_type
            else:
                logger.warning(f"Nenhum dado válido encontrado em {file_name}")
                return pd.DataFrame(), file_type
                
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {file_name}: {e}")
            return pd.DataFrame(), 'erro'

class BigQueryManager:
    """Gerenciador do BigQuery para EMS Project"""
    
    def __init__(self, credentials_path):
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/bigquery']
            )
            self.client = bigquery.Client(
                credentials=self.credentials,
                project=Config.PROJECT_ID
            )
            logger.info("BigQuery conectado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao conectar BigQuery: {e}")
            raise
    
    def load_data_insert_method(self, df, file_type='generico'):
        """Inserção direta usando tabelas existentes com schema mapping"""
        if df.empty:
            logger.warning("DataFrame vazio, nada para carregar")
            return False
        
        try:
            # Usar tabelas existentes
            table_map = {
                'faturamento_historico': 'faturamento_historico',
                'empresas_antigas': 'empresas_antigas', 
                'clientes_atuais': 'clientes_atuais',
                'generico': 'dados_genericos'
            }
            
            table_name = table_map.get(file_type, 'dados_genericos')
            table_id = f"{Config.PROJECT_ID}.{Config.DATASET_RAW}.{table_name}"
            
            # Obter schema da tabela existente
            table = self.client.get_table(table_id)
            existing_columns = {field.name: field.field_type for field in table.schema}
            
            logger.info(f"Mapeando dados para tabela existente: {table_name}")
            logger.info(f"Colunas na tabela: {list(existing_columns.keys())}")
            
            # Converter DataFrame para match com schema existente
            rows_to_insert = []
            for _, row in df.iterrows():
                row_dict = {}
                
                # Mapear apenas colunas que existem na tabela de destino
                for col_name, col_type in existing_columns.items():
                    value = None
                    
                    # Tentar encontrar coluna correspondente no DataFrame
                    df_col = None
                    for df_column in df.columns:
                        if df_column.lower() == col_name.lower():
                            df_col = df_column
                            break
                        # Tentar match parcial (sem acentos, underscores)
                        clean_df = df_column.lower().replace('_', ' ').replace('º', '').replace('ª', '')
                        clean_table = col_name.lower().replace('_', ' ')
                        if clean_df == clean_table:
                            df_col = df_column
                            break
                    
                    if df_col and df_col in row:
                        value = row[df_col]
                        
                        # Converter para tipo correto
                        if pd.isna(value) or value == '' or str(value) == 'nan':
                            value = None
                        else:
                            if col_type == 'STRING':
                                value = str(value).strip()[:1000]  # Limitar tamanho
                            elif col_type == 'FLOAT':
                                try:
                                    value = float(str(value).replace(',', '.'))
                                except:
                                    value = None
                            elif col_type == 'DATE':
                                try:
                                    if isinstance(value, str):
                                        parsed_date = pd.to_datetime(value, errors='coerce')
                                        value = parsed_date.strftime('%Y-%m-%d') if not pd.isna(parsed_date) else None
                                    else:
                                        value = str(value)[:10] if value else None
                                except:
                                    value = None
                            elif col_type == 'DATETIME':
                                try:
                                    parsed_datetime = pd.to_datetime(value, errors='coerce')
                                    value = parsed_datetime.isoformat() if not pd.isna(parsed_datetime) else None
                                except:
                                    value = None
                            else:
                                value = str(value)
                    
                    row_dict[col_name] = value
                
                rows_to_insert.append(row_dict)
            
            # Inserir em lotes
            batch_size = 50  # Menor para evitar timeouts
            total_inserted = 0
            total_errors = 0
            
            for i in range(0, len(rows_to_insert), batch_size):
                batch = rows_to_insert[i:i + batch_size]
                
                try:
                    errors = self.client.insert_rows_json(table_id, batch)
                    
                    if errors:
                        total_errors += len(errors)
                        logger.warning(f"Lote {i//batch_size + 1}: {len(errors)} erros")
                        for error in errors[:3]:  # Mostrar só primeiros 3 erros
                            logger.warning(f"Erro: {error}")
                    else:
                        total_inserted += len(batch)
                    
                    if (i // batch_size + 1) % 5 == 0:
                        logger.info(f"Processados {total_inserted} registros...")
                        
                except Exception as batch_error:
                    logger.error(f"Erro no lote {i//batch_size + 1}: {batch_error}")
                    total_errors += len(batch)
            
            success_rate = (total_inserted / len(rows_to_insert)) * 100 if rows_to_insert else 0
            
            logger.info(f"Dados carregados na tabela {table_name}")
            logger.info(f"Total inserido: {total_inserted} de {len(rows_to_insert)} linhas ({success_rate:.1f}%)")
            
            if total_errors > 0:
                logger.warning(f"Total de erros: {total_errors}")
            
            return total_inserted > 0  # Sucesso se pelo menos 1 linha foi inserida
            
        except Exception as e:
            logger.error(f"Erro na inserção direta: {e}")
            return False

class EMSETLPipeline:
    """Pipeline ETL principal para EMS Project"""
    
    def __init__(self):
        self.config = Config()
        
        # Validar configurações obrigatórias
        if not self.config.DRIVE_PASTA_NOVOS:
            raise ValueError("DRIVE_PASTA_NOVOS não configurado no .env")
        if not self.config.DRIVE_PASTA_ARMAZENADOS:
            raise ValueError("DRIVE_PASTA_ARMAZENADOS não configurado no .env")
        
        # Inicializar componentes
        self.drive = GoogleDriveManager(self.config.GCP_CREDENTIALS_PATH)
        self.bq = BigQueryManager(self.config.GCP_CREDENTIALS_PATH)
        self.processor = DataProcessor()
    
    def process_file(self, file_info):
        """Processar um arquivo individual"""
        file_name = file_info['name']
        file_id = file_info['id']
        
        try:
            logger.info("=" * 60)
            logger.info(f"PROCESSANDO: {file_name}")
            logger.info("=" * 60)
            
            # Baixar arquivo
            file_buffer = self.drive.download_file(file_id, file_name)
            if not file_buffer:
                return False
            
            # Processar dados
            data, file_type = self.processor.process_excel_file(file_buffer, file_name)
            if data.empty:
                logger.warning(f"Arquivo {file_name} não contém dados válidos")
                return False
            
            # Carregar no BigQuery usando inserção direta
            success = self.bq.load_data_insert_method(data, file_type)
            
            if success:
                # Mover arquivo para pasta de armazenados
                self.drive.move_file(
                    file_id,
                    self.config.DRIVE_PASTA_NOVOS,
                    self.config.DRIVE_PASTA_ARMAZENADOS
                )
                logger.info(f"Processamento concluído: {file_name}")
                return True
            else:
                logger.error(f"Falha no processamento: {file_name}")
                return False
                
        except Exception as e:
            logger.error(f"Erro no processamento de {file_name}: {e}")
            return False
    
    def run(self):
        """Executar pipeline completo"""
        try:
            logger.info("EMS ANALYTICS - ETL PIPELINE")
            logger.info("=" * 60)
            
            # Listar arquivos novos
            files = self.drive.list_files(self.config.DRIVE_PASTA_NOVOS)
            
            if not files:
                logger.info("Nenhum arquivo novo para processar")
                return
            
            # Processar cada arquivo
            success_count = 0
            error_count = 0
            
            for file_info in files:
                if self.process_file(file_info):
                    success_count += 1
                else:
                    error_count += 1
            
            # Relatório final
            logger.info("=" * 60)
            logger.info(f"CONCLUÍDO: {success_count} sucesso | {error_count} erros")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Erro no pipeline: {e}")
            raise

def main():
    """Função principal"""
    try:
        # Criar diretório de logs se não existir
        os.makedirs('logs', exist_ok=True)
        
        # Executar pipeline
        pipeline = EMSETLPipeline()
        pipeline.run()
        
    except Exception as e:
        logger.error(f"Erro crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()