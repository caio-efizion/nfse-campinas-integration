#!/usr/bin/env python3
"""
Script para mover arquivos de volta para pasta de novos
"""

import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv('config/.env')

def move_files_back():
    """Mover arquivos de armazenados para novos"""
    
    # Configurações
    DRIVE_PASTA_NOVOS = os.getenv('DRIVE_PASTA_NOVOS')
    DRIVE_PASTA_ARMAZENADOS = os.getenv('DRIVE_PASTA_ARMAZENADOS')
    GCP_CREDENTIALS_PATH = 'config/gcp-credentials.json'
    
    # Conectar ao Google Drive
    credentials = service_account.Credentials.from_service_account_file(
        GCP_CREDENTIALS_PATH,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    service = build('drive', 'v3', credentials=credentials)
    
    # Listar arquivos na pasta de armazenados
    query = f"'{DRIVE_PASTA_ARMAZENADOS}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    
    print(f"Encontrados {len(files)} arquivos na pasta armazenados")
    
    # Mover cada arquivo de volta
    for file_info in files:
        file_id = file_info['id']
        file_name = file_info['name']
        
        try:
            # Mover arquivo
            service.files().update(
                fileId=file_id,
                addParents=DRIVE_PASTA_NOVOS,
                removeParents=DRIVE_PASTA_ARMAZENADOS
            ).execute()
            
            print(f"Movido: {file_name}")
            
        except Exception as e:
            print(f"Erro ao mover {file_name}: {e}")
    
    print("Processo concluído!")

if __name__ == "__main__":
    move_files_back()