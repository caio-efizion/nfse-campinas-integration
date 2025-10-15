-- =====================================================
-- EMS ANALYTICS - SCHEMA FINAL CORRIGIDO
-- Data: 15/10/2025
-- Projeto: dados-ems-project
-- Versão: 3.0 (corrigida)
-- =====================================================

-- =====================================================
-- DATASET: ems_raw (Dados Brutos)
-- =====================================================

-- Tabela principal com schema JSON flexível
CREATE TABLE IF NOT EXISTS `dados-ems-project.ems_raw.arquivos_importados` (
  -- Identificação do arquivo
  arquivo_nome STRING NOT NULL,
  arquivo_tipo STRING,
  aba_nome STRING,
  
  -- Dados brutos em JSON (aceita qualquer estrutura)
  dados_json JSON,
  
  -- Campos extraídos (quando possível)
  cnpj STRING,
  data_emissao DATE,
  valor FLOAT64,
  cliente STRING,
  
  -- Controle
  linha_original INT64,
  hash_linha STRING,
  
  -- Metadados (evitando prefixos proibidos _FILE_, _TABLE_)
  _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _ingestion_batch_id STRING,
  _source_bucket STRING DEFAULT 'drive',
  _processing_status STRING DEFAULT 'pending',
  _processing_error STRING
)
PARTITION BY DATE(_ingestion_timestamp)
CLUSTER BY arquivo_tipo, cnpj, data_emissao
OPTIONS (
  description = "Tabela flexível que aceita qualquer estrutura de Excel"
);

-- Tabela de metadados dos arquivos
CREATE TABLE IF NOT EXISTS `dados-ems-project.ems_raw.arquivos_metadata` (
  arquivo_nome STRING NOT NULL,
  arquivo_hash STRING,
  tamanho_bytes INT64,
  data_modificacao TIMESTAMP,
  
  -- Estrutura detectada
  total_abas INT64,
  abas_nomes ARRAY<STRING>,
  total_linhas INT64,
  colunas_detectadas ARRAY<STRING>,
  
  -- Classificação
  tipo_arquivo STRING,
  periodo_inicio DATE,
  periodo_fim DATE,
  
  -- Status
  status STRING DEFAULT 'novo',
  erro_mensagem STRING,
  
  -- Auditoria
  criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  processado_em TIMESTAMP,
  movido_para_armazenados BOOLEAN DEFAULT FALSE
)
PARTITION BY DATE(criado_em)
OPTIONS (
  description = "Metadados de controle dos arquivos importados"
);

-- =====================================================
-- DATASET: ems_staging (Views de Normalização)
-- =====================================================

-- View: Faturamento normalizado
CREATE OR REPLACE VIEW `dados-ems-project.ems_staging.vw_faturamento_normalizado` AS
WITH dados_extraidos AS (
  SELECT
    arquivo_nome,
    aba_nome,
    linha_original,
    
    -- Cliente (tenta vários nomes)
    COALESCE(
      JSON_VALUE(dados_json, '$.cliente'),
      JSON_VALUE(dados_json, '$.Cliente'),
      JSON_VALUE(dados_json, '$.CLIENTE'),
      JSON_VALUE(dados_json, '$.razao_social'),
      JSON_VALUE(dados_json, '$.empresa'),
      JSON_VALUE(dados_json, '$.EMPRESA'),
      cliente
    ) AS cliente,
    
    -- Data (tenta vários formatos)
    COALESCE(
      SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(dados_json, '$.data_emissao')),
      SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(dados_json, '$.data')),
      SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(dados_json, '$.Data')),
      -- Serial do Excel para data
      CASE 
        WHEN SAFE_CAST(JSON_VALUE(dados_json, '$.emissao') AS INT64) IS NOT NULL
        THEN DATE_ADD(DATE('1899-12-30'), INTERVAL SAFE_CAST(JSON_VALUE(dados_json, '$.emissao') AS INT64) DAY)
      END,
      CASE 
        WHEN SAFE_CAST(JSON_VALUE(dados_json, '$.Emissão') AS INT64) IS NOT NULL
        THEN DATE_ADD(DATE('1899-12-30'), INTERVAL SAFE_CAST(JSON_VALUE(dados_json, '$.Emissão') AS INT64) DAY)
      END,
      data_emissao
    ) AS data_emissao,
    
    -- CNPJ (limpa formatação)
    COALESCE(
      REGEXP_REPLACE(JSON_VALUE(dados_json, '$.cnpj'), r'[^0-9]', ''),
      REGEXP_REPLACE(JSON_VALUE(dados_json, '$.CNPJ'), r'[^0-9]', ''),
      REGEXP_REPLACE(cnpj, r'[^0-9]', '')
    ) AS cnpj,
    
    -- Número da nota
    COALESCE(
      JSON_VALUE(dados_json, '$.nota_fiscal'),
      JSON_VALUE(dados_json, '$.numero_nota'),
      JSON_VALUE(dados_json, '$.Num.'),
      JSON_VALUE(dados_json, '$.Nota Fiscal nº'),
      JSON_VALUE(dados_json, '$.Nota_Fiscal_nº')
    ) AS numero_nota,
    
    -- Valor líquido
    COALESCE(
      SAFE_CAST(JSON_VALUE(dados_json, '$.valor_liquido') AS FLOAT64),
      SAFE_CAST(JSON_VALUE(dados_json, '$.Liquido') AS FLOAT64),
      SAFE_CAST(JSON_VALUE(dados_json, '$.Líquido') AS FLOAT64),
      SAFE_CAST(JSON_VALUE(dados_json, '$.valor_nota_fiscal') AS FLOAT64),
      SAFE_CAST(JSON_VALUE(dados_json, '$.VALOR_NOTA_FISCAL') AS FLOAT64),
      SAFE_CAST(JSON_VALUE(dados_json, '$["VALOR NOTA FISCAL"]') AS FLOAT64),
      SAFE_CAST(valor AS FLOAT64)
    ) AS valor_liquido,
    
    -- Valor bruto
    COALESCE(
      SAFE_CAST(JSON_VALUE(dados_json, '$.valor_bruto') AS FLOAT64),
      SAFE_CAST(JSON_VALUE(dados_json, '$.Bruto') AS FLOAT64)
    ) AS valor_bruto,
    
    -- Honorários
    COALESCE(
      SAFE_CAST(JSON_VALUE(dados_json, '$.honorarios') AS FLOAT64),
      SAFE_CAST(JSON_VALUE(dados_json, '$.Honorários') AS FLOAT64)
    ) AS honorarios,
    
    -- Outros campos
    JSON_VALUE(dados_json, '$.tipo') AS tipo_servico,
    JSON_VALUE(dados_json, '$.situacao') AS situacao,
    JSON_VALUE(dados_json, '$.consultor') AS consultor,
    
    -- Metadados
    _ingestion_timestamp,
    _ingestion_batch_id
    
  FROM `dados-ems-project.ems_raw.arquivos_importados`
  WHERE arquivo_tipo IN ('faturamento_historico', 'clientes_atuais', 'desconhecido')
    AND _processing_status = 'processed'
)
SELECT *
FROM dados_extraidos
WHERE cnpj IS NOT NULL 
  AND data_emissao IS NOT NULL
  AND valor_liquido IS NOT NULL;

-- View: Empresas normalizadas
CREATE OR REPLACE VIEW `dados-ems-project.ems_staging.vw_empresas_normalizado` AS
SELECT
  arquivo_nome,
  linha_original,
  
  -- CNPJ limpo
  REGEXP_REPLACE(COALESCE(
    JSON_VALUE(dados_json, '$.cnpj'),
    JSON_VALUE(dados_json, '$.CNPJ'),
    cnpj
  ), r'[^0-9]', '') AS cnpj,
  
  -- Razão Social
  COALESCE(
    JSON_VALUE(dados_json, '$.razao_social'),
    JSON_VALUE(dados_json, '$.Razão_Social'),
    JSON_VALUE(dados_json, '$["Razão Social"]'),
    JSON_VALUE(dados_json, '$.razão_social')
  ) AS razao_social,
  
  -- Nome fantasia
  COALESCE(
    JSON_VALUE(dados_json, '$.nome_fantasia'),
    JSON_VALUE(dados_json, '$.Nome_Fantasia'),
    JSON_VALUE(dados_json, '$["Nome Fantasia"]')
  ) AS nome_fantasia,
  
  -- Situação cadastral
  COALESCE(
    JSON_VALUE(dados_json, '$.situacao_cadastral'),
    JSON_VALUE(dados_json, '$.Situação_Cadastral'),
    JSON_VALUE(dados_json, '$["Situação Cadastral"]')
  ) AS situacao_cadastral,
  
  -- Data situação
  SAFE.PARSE_DATE('%Y-%m-%d', 
    COALESCE(
      JSON_VALUE(dados_json, '$.data_situacao'),
      JSON_VALUE(dados_json, '$.Data_Situação'),
      JSON_VALUE(dados_json, '$["Data Situação"]')
    )
  ) AS data_situacao,
  
  -- Endereço
  COALESCE(
    JSON_VALUE(dados_json, '$.endereco'),
    JSON_VALUE(dados_json, '$.Endereço')
  ) AS endereco,
  
  _ingestion_timestamp
  
FROM `dados-ems-project.ems_raw.arquivos_importados`
WHERE arquivo_tipo IN ('empresas_antigas', 'desconhecido')
  AND _processing_status = 'processed'
  AND (
    JSON_VALUE(dados_json, '$.cnpj') IS NOT NULL 
    OR JSON_VALUE(dados_json, '$.CNPJ') IS NOT NULL
    OR cnpj IS NOT NULL
  );

-- =====================================================
-- DATASET: ems_analytics (Data Mart)
-- =====================================================

-- Dimensão: Clientes
CREATE OR REPLACE TABLE `dados-ems-project.ems_analytics.dim_clientes` AS
WITH empresas_unicas AS (
  SELECT DISTINCT
    cnpj,
    FIRST_VALUE(razao_social) OVER (PARTITION BY cnpj ORDER BY _ingestion_timestamp DESC) AS razao_social,
    FIRST_VALUE(nome_fantasia) OVER (PARTITION BY cnpj ORDER BY _ingestion_timestamp DESC) AS nome_fantasia,
    FIRST_VALUE(situacao_cadastral) OVER (PARTITION BY cnpj ORDER BY _ingestion_timestamp DESC) AS situacao_cadastral,
    FIRST_VALUE(data_situacao) OVER (PARTITION BY cnpj ORDER BY _ingestion_timestamp DESC) AS data_situacao,
    FIRST_VALUE(endereco) OVER (PARTITION BY cnpj ORDER BY _ingestion_timestamp DESC) AS endereco
  FROM `dados-ems-project.ems_staging.vw_empresas_normalizado`
  WHERE cnpj IS NOT NULL AND cnpj != ''
),
faturamento_agregado AS (
  SELECT
    cnpj,
    MIN(data_emissao) AS primeira_nota,
    MAX(data_emissao) AS ultima_nota,
    COUNT(*) AS total_notas,
    SUM(valor_liquido) AS faturamento_total,
    AVG(valor_liquido) AS ticket_medio
  FROM `dados-ems-project.ems_staging.vw_faturamento_normalizado`
  WHERE cnpj IS NOT NULL AND cnpj != ''
  GROUP BY cnpj
)
SELECT
  ROW_NUMBER() OVER (ORDER BY COALESCE(f.faturamento_total, 0) DESC) AS cliente_key,
  COALESCE(e.cnpj, f.cnpj) AS cnpj,
  COALESCE(e.razao_social, 'Nome não identificado') AS razao_social,
  e.nome_fantasia,
  e.situacao_cadastral,
  e.data_situacao,
  e.endereco,
  
  -- Extrai cidade e UF do endereço
  TRIM(SPLIT(REGEXP_EXTRACT(e.endereco, r',\s*([^,]+)$'), '/')[SAFE_OFFSET(0)]) AS cidade,
  TRIM(SPLIT(REGEXP_EXTRACT(e.endereco, r',\s*([^,]+)$'), '/')[SAFE_OFFSET(1)]) AS uf,
  
  -- Dados de faturamento
  f.primeira_nota,
  f.ultima_nota,
  f.total_notas,
  f.faturamento_total,
  f.ticket_medio,
  
  -- Status do cliente
  CASE
    WHEN f.ultima_nota >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) THEN 'ATIVO'
    WHEN f.ultima_nota >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH) THEN 'INATIVO_RECENTE'
    WHEN f.ultima_nota >= DATE_SUB(CURRENT_DATE(), INTERVAL 36 MONTH) THEN 'INATIVO_MEDIO'
    WHEN f.ultima_nota IS NOT NULL THEN 'INATIVO_ANTIGO'
    ELSE 'SEM_NOTAS'
  END AS status_cliente,
  
  DATE_DIFF(CURRENT_DATE(), f.ultima_nota, DAY) AS dias_sem_nota,
  
  -- Segmento por valor
  CASE
    WHEN f.faturamento_total > 50000 THEN 'VIP'
    WHEN f.faturamento_total > 20000 THEN 'PREMIUM'
    WHEN f.faturamento_total > 5000 THEN 'STANDARD'
    ELSE 'LOW_TICKET'
  END AS segmento_valor,
  
  CURRENT_TIMESTAMP() AS ultima_atualizacao

FROM empresas_unicas e
FULL OUTER JOIN faturamento_agregado f
  ON e.cnpj = f.cnpj
WHERE COALESCE(e.cnpj, f.cnpj) IS NOT NULL;

-- Fato: Faturamento
CREATE OR REPLACE TABLE `dados-ems-project.ems_analytics.fato_faturamento`
PARTITION BY data_emissao
CLUSTER BY cnpj, cliente_key
AS
SELECT
  f.cnpj,
  c.cliente_key,
  f.data_emissao,
  FORMAT_DATE('%Y%m%d', f.data_emissao) AS data_key,
  f.numero_nota,
  f.tipo_servico,
  f.situacao,
  f.valor_liquido,
  f.valor_bruto,
  f.honorarios,
  f.consultor,
  EXTRACT(YEAR FROM f.data_emissao) AS ano,
  EXTRACT(MONTH FROM f.data_emissao) AS mes,
  EXTRACT(QUARTER FROM f.data_emissao) AS trimestre,
  f.arquivo_nome AS source_file,
  f._ingestion_timestamp,
  f._ingestion_batch_id
FROM `dados-ems-project.ems_staging.vw_faturamento_normalizado` f
LEFT JOIN `dados-ems-project.ems_analytics.dim_clientes` c
  ON f.cnpj = c.cnpj;

-- Dimensão: Tempo
CREATE TABLE IF NOT EXISTS `dados-ems-project.ems_analytics.dim_tempo` AS
WITH datas AS (
  SELECT date_day
  FROM UNNEST(GENERATE_DATE_ARRAY('2006-01-01', '2030-12-31')) AS date_day
)
SELECT
  FORMAT_DATE('%Y%m%d', date_day) AS data_key,
  date_day AS data,
  EXTRACT(YEAR FROM date_day) AS ano,
  EXTRACT(QUARTER FROM date_day) AS trimestre,
  EXTRACT(MONTH FROM date_day) AS mes,
  FORMAT_DATE('%B', date_day) AS nome_mes,
  EXTRACT(WEEK FROM date_day) AS semana,
  EXTRACT(DAYOFWEEK FROM date_day) AS dia_semana,
  FORMAT_DATE('%A', date_day) AS nome_dia_semana,
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE 
    ELSE FALSE 
  END AS eh_fim_de_semana
FROM datas;

-- =====================================================
-- VIEWS AUXILIARES PARA VALIDAÇÃO
-- =====================================================

-- View: Status da ingestão
CREATE OR REPLACE VIEW `dados-ems-project.ems_raw.vw_status_ingestao` AS
SELECT 
  arquivo_tipo,
  _processing_status,
  COUNT(*) AS total_linhas,
  COUNT(DISTINCT arquivo_nome) AS total_arquivos,
  MIN(data_emissao) AS data_mais_antiga,
  MAX(data_emissao) AS data_mais_recente,
  SUM(valor) AS valor_total
FROM `dados-ems-project.ems_raw.arquivos_importados`
GROUP BY arquivo_tipo, _processing_status
ORDER BY arquivo_tipo, _processing_status;

-- View: Resumo de clientes
CREATE OR REPLACE VIEW `dados-ems-project.ems_analytics.vw_resumo_clientes` AS
SELECT 
  status_cliente,
  segmento_valor,
  COUNT(*) AS total_clientes,
  SUM(faturamento_total) AS faturamento_total,
  AVG(ticket_medio) AS ticket_medio,
  AVG(dias_sem_nota) AS media_dias_sem_nota
FROM `dados-ems-project.ems_analytics.dim_clientes`
GROUP BY status_cliente, segmento_valor
ORDER BY status_cliente, segmento_valor;

-- =====================================================
-- FIM DO SCRIPT
-- =====================================================