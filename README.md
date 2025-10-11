# Pipeline NFSe Campinas

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
venv\Scripts\activate     # Windows

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
