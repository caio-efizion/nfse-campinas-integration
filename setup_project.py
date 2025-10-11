#!/usr/bin/env python3
"""
Script para criar TODOS os arquivos do projeto NFSe Campinas
Salve este arquivo como: setup_project.py
Execute: python setup_project.py
"""

from pathlib import Path

def criar_arquivo(caminho, conteudo):
    """Cria arquivo com conteúdo"""
    arquivo = Path(caminho)
    arquivo.parent.mkdir(parents=True, exist_ok=True)
    arquivo.write_text(conteudo, encoding='utf-8')
    print(f"✅ {caminho}")

# Dicionário com TODOS os arquivos
arquivos = {
    # ==================== ROOT ====================
    ".gitignore": """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
env/
.venv

# GCP Credentials
*.json
!schemas/**/*.json

# Certificados
*.pfx
*.pem
*.p12
config/certificados/*

# Logs
logs/*.log
*.log

# IDEs
.vscode/
.idea/
*.swp

# Environment
.env
.env.local

# OS
.DS_Store
Thumbs.db

# Terraform
terraform/.terraform/
terraform/*.tfstate
terraform/.terraform.lock.hcl
""",

    "README.md": """# Pipeline NFSe Campinas

Sistema de integração automatizada para extração de Notas Fiscais Eletrônicas (NFSe) da Prefeitura de Campinas/SP.

## 🎯 Objetivo

Automatizar a extração, processamento e armazenamento de notas fiscais eletrônicas emitidas via webservice da Prefeitura Municipal de Campinas (padrão ABRASF 2.03).

## 🏗️ Arquitetura

```
Prefeitura Campinas (WebService SOAP)
    ↓
n8n Workflow (Orquestração)
    ↓
Script Python (Parse XML)
    ↓
Google Cloud BigQuery (Data Lake)
    ↓
Power BI (Visualização)
```

## 📦 Stack Tecnológico

- **Orquestração:** n8n workflows
- **Integração:** Python + Zeep (SOAP)
- **Storage:** GCP Cloud Storage + BigQuery
- **Padrão:** ABRASF 2.03 (NFSe nacional)
- **Autenticação:** Certificado Digital A1/A3

## 🚀 Setup

```bash
# 1. Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\\Scripts\\activate     # Windows

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Configurar credenciais GCP
gcloud auth application-default login

# 4. Executar
python src/main.py --data-inicio 2025-10-01 --data-fim 2025-10-11
```

## 📝 Status do Projeto

- [x] Estrutura de pastas criada
- [ ] Schemas BigQuery definidos
- [ ] Parser XML implementado
- [ ] Cliente API NFSe implementado
- [ ] Integração BigQuery

## 🔐 Segurança

- Certificados digitais **NUNCA** commitados no Git
- Credenciais em Secret Manager (produção)
""",

    "requirements.txt": """# API SOAP
zeep==4.2.1
lxml==5.1.0
cryptography==41.0.7

# Google Cloud
google-cloud-bigquery==3.14.1
google-cloud-storage==2.10.0
google-cloud-secret-manager==2.16.4

# Utilities
PyYAML==6.0.1
python-dotenv==1.0.0
requests==2.31.0
pandas==2.1.4

# Logging
structlog==23.2.0

# Testing
pytest==7.4.3
pytest-mock==3.12.0
pytest-cov==4.1.0
""",

    ".env.example": """# Certificado Digital
CERT_PASSWORD=sua_senha_certificado_aqui

# GCP
GCP_PROJECT_ID=seu-projeto-gcp
GCP_DATASET_ID=fiscal_data
GCP_BUCKET_RAW=seu-bucket-raw-data

# NFSe Campinas
NFSE_AMBIENTE=homologacao

# Cliente
CLIENTE_CNPJ=12345678000199
CLIENTE_INSCRICAO_MUNICIPAL=123456
""",

    # ==================== SRC ====================
    "src/__init__.py": '"""Pipeline NFSe Campinas"""',
    
    "src/api/__init__.py": "",
    
    "src/api/nfse_client.py": '''"""Cliente API NFSe Campinas - ABRASF 2.03"""

from zeep import Client
from zeep.wsse.signature import Signature
import logging

logger = logging.getLogger(__name__)


class NfseClient:
    """Cliente para WebService NFSe Campinas"""
    
    def __init__(self, config):
        """
        Inicializa cliente SOAP
        
        Args:
            config: Dicionário com configurações
        """
        self.config = config
        # TODO: Implementar inicialização do cliente Zeep
        # TODO: Configurar certificado digital
        pass
    
    def consultar_nfse_periodo(self, data_inicio: str, data_fim: str):
        """
        Consulta NFSe por período
        
        Args:
            data_inicio: Data início (YYYY-MM-DD)
            data_fim: Data fim (YYYY-MM-DD)
            
        Returns:
            XML com as NFSe encontradas
        """
        # TODO: Implementar consulta
        logger.info(f"Consultando NFSe de {data_inicio} até {data_fim}")
        pass
''',

    "src/api/auth.py": '''"""Autenticação com certificado digital"""

from cryptography import x509
from cryptography.hazmat.backends import default_backend
import logging

logger = logging.getLogger(__name__)


def carregar_certificado(caminho: str, senha: str):
    """
    Carrega certificado digital .pfx
    
    Args:
        caminho: Caminho do arquivo .pfx
        senha: Senha do certificado
        
    Returns:
        Certificado carregado
    """
    # TODO: Implementar carregamento de certificado
    logger.info(f"Carregando certificado de {caminho}")
    pass
''',

    "src/parsers/__init__.py": "",
    
    "src/parsers/xml_parser.py": '''"""Parser XML ABRASF 2.03 para Python Dict"""

from lxml import etree
import logging

logger = logging.getLogger(__name__)


class NfseXmlParser:
    """Parser de XML NFSe para estrutura Python"""
    
    def parse_nfse_response(self, xml_string: str) -> list:
        """
        Parse XML de resposta da API
        
        Args:
            xml_string: String XML da resposta
            
        Returns:
            Lista de dicionários com dados das NFSe
        """
        # TODO: Implementar parse do XML
        logger.info("Parseando XML de resposta")
        return []
    
    def extrair_nfse(self, nfse_element) -> dict:
        """
        Extrai dados de um elemento NFSe
        
        Args:
            nfse_element: Elemento XML da NFSe
            
        Returns:
            Dicionário com dados extraídos
        """
        # TODO: Implementar extração
        pass
''',

    "src/parsers/validators.py": '''"""Validadores de dados extraídos"""

import re
import logging

logger = logging.getLogger(__name__)


def validar_cnpj(cnpj: str) -> bool:
    """
    Valida CNPJ com dígitos verificadores
    
    Args:
        cnpj: CNPJ (apenas números)
        
    Returns:
        True se válido
    """
    # TODO: Implementar validação CNPJ
    pass


def validar_cpf(cpf: str) -> bool:
    """
    Valida CPF com dígitos verificadores
    
    Args:
        cpf: CPF (apenas números)
        
    Returns:
        True se válido
    """
    # TODO: Implementar validação CPF
    pass
''',

    "src/storage/__init__.py": "",
    
    "src/storage/bigquery_loader.py": '''"""Loader para Google BigQuery"""

from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Gerenciador de carregamento no BigQuery"""
    
    def __init__(self, config):
        """
        Inicializa cliente BigQuery
        
        Args:
            config: Dicionário com configurações
        """
        self.config = config
        # TODO: Inicializar cliente BigQuery
        pass
    
    def inserir_nfse_batch(self, nfse_list: list):
        """
        Insere lote de NFSe no BigQuery
        
        Args:
            nfse_list: Lista de dicionários com dados das NFSe
        """
        # TODO: Implementar insert batch
        logger.info(f"Inserindo {len(nfse_list)} NFSe no BigQuery")
        pass
''',

    "src/storage/cloud_storage.py": '''"""Manager para Cloud Storage"""

from google.cloud import storage
import logging

logger = logging.getLogger(__name__)


class CloudStorageManager:
    """Gerenciador de Cloud Storage"""
    
    def __init__(self, config):
        """
        Inicializa cliente Cloud Storage
        
        Args:
            config: Dicionário com configurações
        """
        self.config = config
        # TODO: Inicializar cliente Storage
        pass
    
    def salvar_xml(self, xml_string: str, nome_arquivo: str):
        """
        Salva XML bruto no bucket
        
        Args:
            xml_string: Conteúdo XML
            nome_arquivo: Nome do arquivo no bucket
        """
        # TODO: Implementar upload
        logger.info(f"Salvando XML: {nome_arquivo}")
        pass
''',

    "src/utils/__init__.py": "",
    
    "src/utils/logger.py": '''"""Configuração de logging estruturado"""

import logging
import structlog


def setup_logger(config: dict):
    """
    Configura logging estruturado
    
    Args:
        config: Dicionário com configurações
        
    Returns:
        Logger configurado
    """
    # TODO: Configurar structlog
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)
''',

    "src/utils/helpers.py": '''"""Funções auxiliares e helpers"""

import yaml
from pathlib import Path


def load_config(config_path: str) -> dict:
    """
    Carrega arquivo de configuração YAML
    
    Args:
        config_path: Caminho do arquivo config.yaml
        
    Returns:
        Dicionário com configurações
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config
''',

    "src/main.py": '''"""Script principal de extração NFSe"""

import argparse
import logging

# TODO: Importar módulos quando implementados
# from src.api.nfse_client import NfseClient
# from src.parsers.xml_parser import NfseXmlParser
# from src.storage.bigquery_loader import BigQueryLoader
# from src.utils.logger import setup_logger
# from src.utils.helpers import load_config

logger = logging.getLogger(__name__)


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description='Extração NFSe Campinas')
    parser.add_argument('--data-inicio', required=True, help='Data início (YYYY-MM-DD)')
    parser.add_argument('--data-fim', required=True, help='Data fim (YYYY-MM-DD)')
    parser.add_argument('--config', default='config/config.yaml', help='Arquivo config')
    args = parser.parse_args()
    
    print(f"Iniciando extração de {args.data_inicio} até {args.data_fim}")
    # TODO: Implementar fluxo completo
    

if __name__ == "__main__":
    main()
''',

    # ==================== CONFIG ====================
    "config/config.yaml": """# Configuração Cliente NFSe Campinas
cliente:
  nome: "Cliente Exemplo LTDA"
  cnpj: "12345678000199"
  inscricao_municipal: "123456"

gcp:
  project_id: "cliente-exemplo-project"
  dataset_id: "fiscal_data"
  location: "us-east1"
  bucket_raw_data: "cliente-exemplo-nfse-raw"

nfse:
  ambiente: "homologacao"
  wsdl_url_homologacao: "https://homol-rps.ima.sp.gov.br/notafiscal-abrasfv203-ws/NotaFiscalSoap?wsdl"
  wsdl_url_producao: "https://issdigital.campinas.sp.gov.br/notafiscal-abrasfv203-ws/NotaFiscalSoap?wsdl"
  certificado_path: "config/certificados/certificado.pfx"
  certificado_senha: "${CERT_PASSWORD}"

processamento:
  consulta_periodo_dias: 7
  retry_tentativas: 3
  lote_tamanho: 100

logging:
  level: "INFO"
  formato: "json"
""",

    "config/README.md": """# Configuração

Coloque o certificado digital (.pfx) na pasta `certificados/`

**ATENÇÃO:** Esta pasta está no .gitignore para segurança!
""",

    # ==================== SCHEMAS ====================
    "schemas/bigquery/README.md": """# Schemas BigQuery

Arquivos JSON com definição das tabelas do BigQuery.

Os schemas serão adicionados em breve.
""",

    "schemas/xml_samples/README.md": """# Exemplos XML

Exemplos de XML retornados pela API NFSe Campinas.

Os exemplos serão adicionados em breve.
""",

    # ==================== TESTS ====================
    "tests/__init__.py": "",
    
    "tests/test_parser.py": '''"""Testes do parser XML"""

import pytest


def test_parse_nfse_valida():
    """Testa parse de XML válido"""
    # TODO: Implementar teste
    pass
''',

    "tests/test_bigquery.py": '''"""Testes do BigQuery loader"""

import pytest


def test_insert_nfse():
    """Testa insert no BigQuery"""
    # TODO: Implementar teste
    pass
''',

    # ==================== DOCS ====================
    "docs/ARCHITECTURE.md": """# Arquitetura do Sistema

Documentação detalhada da arquitetura será adicionada em breve.
""",

    "docs/API_REFERENCE.md": """# Referência da API

Documentação das APIs utilizadas será adicionada em breve.
""",

    "docs/DEPLOYMENT.md": """# Guia de Deploy

Instruções de deployment serão adicionadas em breve.
""",

    # ==================== TERRAFORM ====================
    "terraform/README.md": """# Infraestrutura GCP

Terraform para provisionamento de recursos no GCP.

Os arquivos terraform serão adicionados em breve.
""",

    # ==================== N8N ====================
    "n8n/README.md": """# Workflows n8n

Workflows de orquestração do n8n.

Os workflows serão exportados e adicionados em breve.
""",

    # ==================== LOGS ====================
    "logs/.gitkeep": "",
}


def main():
    print("=" * 60)
    print("🏗️  CRIANDO ESTRUTURA COMPLETA DO PROJETO")
    print("=" * 60)
    print()
    
    total = len(arquivos)
    
    for i, (caminho, conteudo) in enumerate(arquivos.items(), 1):
        criar_arquivo(caminho, conteudo)
    
    print()
    print("=" * 60)
    print(f"✅ {total} arquivos criados com sucesso!")
    print("=" * 60)
    print()
    print("📝 Próximos passos:")
    print("1. git add .")
    print("2. git commit -m 'feat: estrutura completa do projeto'")
    print("3. Receber os arquivos completos via artifacts")
    print()


if __name__ == "__main__":
    main()