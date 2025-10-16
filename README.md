# EMS Project - Sistema de Recuperação Tributária

Sistema completo de análise e recuperação tributária que combina validação de CNPJ, coleta de NFSe e inteligência comercial para maximizar oportunidades de negócio.

## Arquitetura do Sistema

### Coleta de Dados
- **APIs da Receita Federal**: Validação automática de situação cadastral
- **NFSe Campinas**: Integração com certificado digital (A1/A3)
- **Base histórica**: Arquivos Excel com faturamento 2006-2025

### Processamento
- **BigQuery**: Data warehouse com 3 datasets (raw, staging, analytics)
- **Python**: Pipeline de ETL e validação
- **n8n**: Automação de workflows

### Análise
- **Scores automáticos**: 0-100 baseado em faturamento e situação cadastral
- **Classificações**: PREMIUM, ALTA, MÉDIA, BAIXO_POTENCIAL, RECUPERÁVEL
- **Recomendações**: Ações comerciais priorizadas

## Estrutura do Projeto

```
nfse-campinas-integration/
├── scripts/
│   ├── pipeline_final_corrigido.py    # Pipeline principal
│   ├── cnpj_validator_fixed.py        # Validação CNPJ
│   └── nfse_campinas_integration_fixed.py  # Integração NFSe
├── schemas/
│   └── sql/
│       ├── 02-create-flexible-schema.sql   # Schema BigQuery
│       └── ems_analytics_views.sql         # Views de análise
├── config/
│   ├── .env                               # Variáveis de ambiente
│   ├── gcp-credentials.json              # Credenciais BigQuery
│   └── certificados/certificado.pfx      # Certificado digital
├── n8n/
│   └── workflows/                         # Automação n8n
└── logs/                                  # Logs de execução
```

## Instalação e Configuração

### Pré-requisitos
- Python 3.8+
- Conta Google Cloud Platform
- Certificado digital A1/A3 (para NFSe)
- n8n (para automação)

### 1. Configurar Ambiente
```bash
# Clonar repositório
git clone <repository-url>
cd nfse-campinas-integration

# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows

# Instalar dependências
pip install -r requirements.txt
```

### 2. Configurar BigQuery
```bash
# Criar projeto no Google Cloud
# Habilitar BigQuery API
# Criar service account e baixar credenciais
# Salvar como config/gcp-credentials.json
```

### 3. Configurar Variáveis de Ambiente
```bash
# Copiar e editar arquivo de configuração
cp config/.env.example config/.env

# Editar config/.env com suas credenciais:
PROJECT_ID=seu-projeto-gcp
DATASET_RAW=ems_raw
CERT_PASSWORD=senha-do-certificado
CLIENTE_CNPJ=cnpj-da-empresa
CLIENTE_INSCRICAO_MUNICIPAL=inscricao-municipal
```

### 4. Criar Schema BigQuery
```bash
# Executar script de criação do schema
bq query --use_legacy_sql=false < schemas/sql/02-create-flexible-schema.sql
bq query --use_legacy_sql=false < schemas/sql/ems_analytics_views.sql
```

## Uso do Sistema

### Comandos Principais

#### Validação de CNPJ Individual
```bash
python scripts/cnpj_validator_fixed.py validar 12345678000100
```

#### Validação em Lote
```bash
# Processar 150 CNPJs (recomendado para uso diário)
python scripts/cnpj_validator_fixed.py processar_base 150

# Processar todos os CNPJs pendentes
python scripts/cnpj_validator_fixed.py processar_base
```

#### Pipeline Completo
```bash
# Executar análise completa e gerar relatório
python scripts/pipeline_final_corrigido.py

# Apenas gerar relatório
python scripts/pipeline_final_corrigido.py relatorio
```

#### Integração NFSe
```bash
# Coleta incremental (últimas 72h)
python scripts/nfse_campinas_integration_fixed.py

# Teste de conectividade
python scripts/nfse_campinas_integration_fixed.py teste

# Simulação para desenvolvimento
python scripts/nfse_campinas_integration_fixed.py simulacao
```

### Consultas Úteis

#### Status da Validação
```sql
SELECT 
  COUNT(*) as total,
  COUNT(CASE WHEN `Razão Social` IS NOT NULL THEN 1 END) as validados,
  ROUND(COUNT(CASE WHEN `Razão Social` IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as percentual
FROM `seu-projeto.ems_raw.empresas_antigas` 
WHERE Status = 'OK'
```

#### Top Oportunidades
```sql
SELECT 
  cnpj, razao_social_nfse, score_oportunidade, 
  valor_total_nfses, classificacao_cliente
FROM `seu-projeto.ems_raw.analise_situacao_clientes`
WHERE classificacao_cliente IN ('OPORTUNIDADE_PREMIUM', 'OPORTUNIDADE_ALTA')
ORDER BY valor_total_nfses DESC
```

## Automação n8n

### Workflows Configurados

1. **NFSe Incremental** (a cada 3 horas)
   - Coleta novas NFSes automaticamente
   - Notificações Slack de status

2. **Validação CNPJ Diária** (2h da manhã)
   - Processa 150 CNPJs por dia
   - Relatório de progresso

3. **Relatório Executivo** (8h da manhã)
   - Email automático para diretoria
   - Dashboard atualizado

### Importar Workflows
```bash
# Importar workflows do diretório n8n/
# Configurar credenciais: BigQuery, Slack, SMTP
# Ativar execução automática
```

## Monitoramento

### Logs
```bash
# Verificar logs de execução
tail -f logs/cnpj_validator.log
tail -f logs/nfse_integration.log
tail -f logs/pipeline.log
```

### Métricas Importantes
- Taxa de sucesso na validação CNPJ: ~52%
- Tempo médio por CNPJ: 3-4 segundos
- CNPJs processados por dia: 150-200
- Tempo estimado para base completa: 3-4 semanas

## Classificação de Clientes

### Scores (0-100)
- **Situação Cadastral**: ATIVA (40 pts), INAPTA (20 pts)
- **Faturamento**: R$ 1M+ (30 pts), R$ 500K+ (25 pts), R$ 200K+ (20 pts)
- **Frequência**: 50+ NFSes (20 pts), 20+ NFSes (15 pts)

### Classificações
- **OPORTUNIDADE_PREMIUM**: Score 80+ com faturamento R$ 1M+
- **OPORTUNIDADE_ALTA**: Score 60+ com faturamento R$ 500K+
- **OPORTUNIDADE_MÉDIA**: Score 40+ com faturamento R$ 100K+
- **CLIENTE_ATIVO_BAIXO_POTENCIAL**: Ativo com baixo faturamento
- **CLIENTE_INATIVO_RECUPERÁVEL**: Inativo mas com histórico

### Ações Recomendadas
- **CONTATO_IMEDIATO_PREMIUM**: Clientes R$ 1M+ (máxima prioridade)
- **CONTATO_PRIORITARIO**: Clientes R$ 500K+ (alta prioridade)
- **INCLUIR_CAMPANHA_COMERCIAL**: Clientes R$ 100K+
- **CAMPANHA_REATIVACAO**: Clientes inativos recuperáveis

## Troubleshooting

### Problemas Comuns

#### Erro de Certificado Digital
```bash
# Verificar se certificado existe
ls config/certificados/certificado.pfx

# Testar conectividade
python scripts/nfse_campinas_integration_fixed.py teste
```

#### Erro BigQuery - Permissões
```bash
# Verificar credenciais
export GOOGLE_APPLICATION_CREDENTIALS="config/gcp-credentials.json"

# Testar conexão
bq ls --project_id=seu-projeto
```

#### Rate Limit APIs
```bash
# Reduzir velocidade de processamento
python scripts/cnpj_validator_fixed.py processar_base 50
```

### Logs de Debug
```python
# Adicionar debug detalhado
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Desenvolvimento

### Contribuindo
1. Criar branch para feature
2. Implementar testes
3. Documentar mudanças
4. Pull request com revisão

### Testes
```bash
# Teste de validação individual
python scripts/cnpj_validator_fixed.py validar 11222333000181

# Teste de pipeline com dados limitados
python scripts/pipeline_final_corrigido.py
```

### Extensões Futuras
- Dashboard web interativo
- Integração com CRM
- Machine Learning para scores
- API REST para integrações
- Mobile app para equipe comercial

## Suporte

### Contatos
- Desenvolvedor: [seu-email]
- Documentação: Este README
- Issues: GitHub Issues

### Performance
- Sistema processa ~150 CNPJs/dia automaticamente
- Base completa (732 CNPJs) processada em 3-4 semanas
- Taxa de sucesso esperada: 50-60%
- Uptime esperado: 99%+ com automação n8n

## Licença

Proprietary - EMS Consultoria 2025