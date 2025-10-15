"""Teste de conexão com GCP"""

import os
from google.cloud import bigquery
from google.cloud import storage

# Configurar credenciais
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/gcp-credentials.json'

def test_bigquery():
    """Testa conexão com BigQuery"""
    print("🔍 Testando BigQuery...")
    
    client = bigquery.Client()
    
    # Listar datasets
    datasets = list(client.list_datasets())
    print(f"✅ Conectado! Datasets encontrados: {len(datasets)}")
    
    for dataset in datasets:
        print(f"  - {dataset.dataset_id}")
    
    # Listar tabelas
    tables = list(client.list_tables('fiscal_data'))
    print(f"✅ Tabelas no fiscal_data: {len(tables)}")
    
    for table in tables:
        print(f"  - {table.table_id}")

def test_storage():
    """Testa conexão com Cloud Storage"""
    print("\n🗄️  Testando Cloud Storage...")
    
    client = storage.Client()
    
    # Listar buckets
    buckets = list(client.list_buckets())
    print(f"✅ Conectado! Buckets encontrados: {len(buckets)}")
    
    for bucket in buckets:
        print(f"  - {bucket.name}")

if __name__ == "__main__":
    print("="*60)
    print("🧪 TESTANDO CONEXÃO COM GCP")
    print("="*60)
    print()
    
    try:
        test_bigquery()
        test_storage()
        
        print()
        print("="*60)
        print("✅ TUDO FUNCIONANDO!")
        print("="*60)
        
    except Exception as e:
        print()
        print("="*60)
        print(f"❌ ERRO: {e}")
        print("="*60)