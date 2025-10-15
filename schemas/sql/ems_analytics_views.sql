-- ===================================================================
-- EMS ANALYTICS - VIEWS PARA DASHBOARDS DE RECUPERAÇÃO TRIBUTÁRIA
-- ===================================================================

-- VIEW 1: Análise de Clientes Ativos (Foco em Faturamento)
-- ===================================================================
CREATE OR REPLACE VIEW `dados-ems-project.ems_analytics.v_clientes_performance` AS
WITH cliente_metrics AS (
  SELECT 
    n.tomador_cnpj,
    n.tomador_razao_social,
    n.tomador_uf,
    n.tomador_municipio,
    
    -- Métricas de faturamento
    COUNT(*) as total_nfses,
    SUM(n.valor_liquido) as faturamento_total,
    AVG(n.valor_liquido) as ticket_medio,
    MAX(n.data_emissao) as ultima_nfse,
    MIN(n.data_emissao) as primeira_nfse,
    
    -- Métricas temporais
    DATE_DIFF(CURRENT_DATE(), MAX(n.data_emissao), DAY) as dias_sem_servico,
    DATE_DIFF(MAX(n.data_emissao), MIN(n.data_emissao), DAY) as relacionamento_dias,
    
    -- Análise de frequência
    COUNT(DISTINCT EXTRACT(MONTH FROM n.data_emissao)) as meses_ativos,
    COUNT(DISTINCT EXTRACT(YEAR FROM n.data_emissao)) as anos_ativos
    
  FROM `dados-ems-project.ems_raw.nfse_campinas` n
  WHERE n.tomador_cnpj IS NOT NULL
  GROUP BY 1,2,3,4
),

cliente_classification AS (
  SELECT *,
    -- Score de reativação (0-100)
    CASE 
      WHEN dias_sem_servico <= 30 THEN 100
      WHEN dias_sem_servico <= 90 THEN 80
      WHEN dias_sem_servico <= 180 THEN 60
      WHEN dias_sem_servico <= 365 THEN 40
      ELSE 20
    END as score_reativacao,
    
    -- Classificação de valor
    CASE 
      WHEN faturamento_total >= 50000 THEN 'PREMIUM'
      WHEN faturamento_total >= 20000 THEN 'ALTO_VALOR'
      WHEN faturamento_total >= 5000 THEN 'MEDIO_VALOR'
      ELSE 'BAIXO_VALOR'
    END as categoria_valor,
    
    -- Status do cliente
    CASE 
      WHEN dias_sem_servico <= 90 THEN 'ATIVO'
      WHEN dias_sem_servico <= 365 THEN 'INATIVO_RECENTE'
      ELSE 'INATIVO_LONGO'
    END as status_cliente,
    
    -- Potencial de cross-sell
    CASE 
      WHEN meses_ativos >= 12 AND ticket_medio >= 5000 THEN 'ALTO'
      WHEN meses_ativos >= 6 AND ticket_medio >= 2000 THEN 'MEDIO'
      ELSE 'BAIXO'
    END as potencial_crosssell
    
  FROM cliente_metrics
)

SELECT *,
  -- LTV estimado (Lifetime Value)
  (faturamento_total / relacionamento_dias) * 365 * 2 as ltv_estimado,
  
  -- Score final de oportunidade
  (score_reativacao * 0.4 + 
   CASE categoria_valor 
     WHEN 'PREMIUM' THEN 100 
     WHEN 'ALTO_VALOR' THEN 80 
     WHEN 'MEDIO_VALOR' THEN 60 
     ELSE 40 
   END * 0.6) as score_oportunidade

FROM cliente_classification;

-- ===================================================================
-- VIEW 2: Análise Geográfica para Expansão
-- ===================================================================
CREATE OR REPLACE VIEW `dados-ems-project.ems_analytics.v_expansao_geografica` AS
WITH geo_analysis AS (
  SELECT 
    tomador_uf,
    tomador_municipio,
    
    -- Métricas por localidade
    COUNT(DISTINCT tomador_cnpj) as total_clientes,
    COUNT(*) as total_nfses,
    SUM(valor_liquido) as faturamento_total,
    AVG(valor_liquido) as ticket_medio_local,
    
    -- Análise temporal
    COUNT(DISTINCT CASE WHEN data_emissao >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) THEN tomador_cnpj END) as clientes_ultimos_12m,
    SUM(CASE WHEN data_emissao >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) THEN valor_liquido ELSE 0 END) as faturamento_12m,
    
    -- Densidade de mercado
    COUNT(DISTINCT tomador_cnpj) / 
    NULLIF(COUNT(DISTINCT tomador_municipio), 0) as densidade_clientes_municipio
    
  FROM `dados-ems-project.ems_raw.nfse_campinas`
  WHERE tomador_uf IS NOT NULL
  GROUP BY 1,2
),

market_potential AS (
  SELECT *,
    -- Score de potencial por UF
    CASE 
      WHEN faturamento_total >= 100000 AND total_clientes >= 10 THEN 'MERCADO_MADURO'
      WHEN faturamento_total >= 50000 AND total_clientes >= 5 THEN 'OPORTUNIDADE_ALTA'
      WHEN faturamento_total >= 20000 AND total_clientes >= 3 THEN 'OPORTUNIDADE_MEDIA'
      WHEN total_clientes >= 1 THEN 'MERCADO_EMERGENTE'
      ELSE 'NAO_ATENDIDO'
    END as classificacao_mercado,
    
    -- ROI estimado para expansão
    (faturamento_12m / NULLIF(clientes_ultimos_12m, 0)) * 10 as roi_estimado_expansao
    
  FROM geo_analysis
)

SELECT *,
  -- Ranking de prioridade para expansão
  ROW_NUMBER() OVER (
    ORDER BY 
      CASE classificacao_mercado 
        WHEN 'OPORTUNIDADE_ALTA' THEN 4
        WHEN 'OPORTUNIDADE_MEDIA' THEN 3  
        WHEN 'MERCADO_EMERGENTE' THEN 2
        WHEN 'MERCADO_MADURO' THEN 1
        ELSE 0
      END DESC,
      roi_estimado_expansao DESC
  ) as prioridade_expansao

FROM market_potential;

-- ===================================================================
-- VIEW 3: Dashboard Executivo (KPIs Principais)
-- ===================================================================
CREATE OR REPLACE VIEW `dados-ems-project.ems_analytics.v_dashboard_executivo` AS
WITH kpis_base AS (
  SELECT 
    -- Período de análise
    EXTRACT(YEAR FROM data_emissao) as ano,
    EXTRACT(MONTH FROM data_emissao) as mes,
    
    -- Métricas principais
    COUNT(*) as nfses_emitidas,
    COUNT(DISTINCT tomador_cnpj) as clientes_unicos,
    SUM(valor_liquido) as receita_liquida,
    AVG(valor_liquido) as ticket_medio,
    
    -- Métricas de impostos recuperados
    SUM(valor_pis + valor_cofins + valor_inss + valor_ir + valor_csll) as impostos_retidos,
    SUM(valor_iss) as iss_total,
    
    -- Análise de segmentos de serviço
    COUNT(DISTINCT item_lista_servico) as tipos_servico
    
  FROM `dados-ems-project.ems_raw.nfse_campinas`
  GROUP BY 1,2
),

kpis_comparativos AS (
  SELECT *,
    -- Comparativo com mês anterior
    LAG(receita_liquida) OVER (ORDER BY ano, mes) as receita_mes_anterior,
    LAG(clientes_unicos) OVER (ORDER BY ano, mes) as clientes_mes_anterior,
    
    -- Growth rate
    ROUND(
      ((receita_liquida - LAG(receita_liquida) OVER (ORDER BY ano, mes)) / 
       NULLIF(LAG(receita_liquida) OVER (ORDER BY ano, mes), 0)) * 100, 2
    ) as growth_rate_receita,
    
    -- Média móvel (3 meses)
    AVG(receita_liquida) OVER (
      ORDER BY ano, mes 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as receita_media_movel_3m
    
  FROM kpis_base
)

SELECT *,
  -- Classificação de performance
  CASE 
    WHEN growth_rate_receita >= 20 THEN 'EXCELENTE'
    WHEN growth_rate_receita >= 10 THEN 'BOM'
    WHEN growth_rate_receita >= 0 THEN 'ESTAVEL'
    ELSE 'DECLINIO'
  END as performance_classificacao,
  
  -- Meta de recuperação tributária (exemplo: 15% da receita)
  receita_liquida * 0.15 as meta_recuperacao_tributaria,
  
  -- Score de eficiência tributária
  ROUND((impostos_retidos / NULLIF(receita_liquida, 0)) * 100, 2) as eficiencia_tributaria_pct

FROM kpis_comparativos;

-- ===================================================================
-- VIEW 4: Oportunidades de Cross-sell e Up-sell
-- ===================================================================
CREATE OR REPLACE VIEW `dados-ems-project.ems_analytics.v_oportunidades_vendas` AS
WITH cliente_servicos AS (
  SELECT 
    tomador_cnpj,
    tomador_razao_social,
    tomador_uf,
    
    -- Análise de serviços utilizados
    COUNT(DISTINCT item_lista_servico) as tipos_servicos_utilizados,
    STRING_AGG(DISTINCT item_lista_servico) as lista_servicos,
    
    -- Valor e frequência
    SUM(valor_liquido) as valor_total_cliente,
    COUNT(*) as total_servicos,
    MAX(data_emissao) as ultimo_servico,
    
    -- Análise de sazonalidade
    COUNT(DISTINCT CONCAT(EXTRACT(YEAR FROM data_emissao), '-', EXTRACT(MONTH FROM data_emissao))) as meses_ativos,
    
    -- Análise de potencial
    AVG(valor_liquido) as ticket_medio_cliente
    
  FROM `dados-ems-project.ems_raw.nfse_campinas`
  WHERE tomador_cnpj IS NOT NULL
  GROUP BY 1,2,3
),

oportunidades AS (
  SELECT *,
    -- Score de up-sell (baseado em valor e frequência)
    CASE 
      WHEN valor_total_cliente >= 50000 AND meses_ativos >= 12 THEN 100
      WHEN valor_total_cliente >= 20000 AND meses_ativos >= 6 THEN 80
      WHEN valor_total_cliente >= 10000 AND meses_ativos >= 3 THEN 60
      WHEN valor_total_cliente >= 5000 THEN 40
      ELSE 20
    END as score_upsell,
    
    -- Score de cross-sell (baseado em diversificação de serviços)
    CASE 
      WHEN tipos_servicos_utilizados = 1 THEN 100  -- Alta oportunidade
      WHEN tipos_servicos_utilizados = 2 THEN 70
      WHEN tipos_servicos_utilizados = 3 THEN 50
      ELSE 30
    END as score_crosssell,
    
    -- Classificação de urgência para ação comercial
    CASE 
      WHEN DATE_DIFF(CURRENT_DATE(), ultimo_servico, DAY) <= 30 THEN 'URGENTE'
      WHEN DATE_DIFF(CURRENT_DATE(), ultimo_servico, DAY) <= 90 THEN 'ALTA'
      WHEN DATE_DIFF(CURRENT_DATE(), ultimo_servico, DAY) <= 180 THEN 'MEDIA'
      ELSE 'BAIXA'
    END as prioridade_acao,
    
    -- Potencial de receita adicional estimado
    ticket_medio_cliente * 3 as potencial_receita_adicional
    
  FROM cliente_servicos
)

SELECT *,
  -- Score final de oportunidade
  (score_upsell * 0.6 + score_crosssell * 0.4) as score_oportunidade_final,
  
  -- Recomendação de ação
  CASE 
    WHEN score_crosssell >= 80 AND score_upsell >= 60 THEN 'CROSS_SELL_PRIORITARIO'
    WHEN score_upsell >= 80 THEN 'UP_SELL_PRIORITARIO' 
    WHEN score_crosssell >= 60 THEN 'CROSS_SELL_SECUNDARIO'
    WHEN prioridade_acao = 'URGENTE' THEN 'RETENCAO_URGENTE'
    ELSE 'MANUTENCAO'
  END as recomendacao_acao

FROM oportunidades
ORDER BY score_oportunidade_final DESC;

-- ===================================================================
-- VIEW 5: Relatório de Recuperação Tributária
-- ===================================================================
CREATE OR REPLACE VIEW `dados-ems-project.ems_analytics.v_recuperacao_tributaria` AS
WITH analise_tributaria AS (
  SELECT 
    tomador_cnpj,
    tomador_razao_social,
    tomador_uf,
    
    -- Valores de serviços
    SUM(valor_servicos) as valor_servicos_total,
    SUM(valor_liquido) as valor_liquido_total,
    
    -- Impostos e deduções
    SUM(valor_pis) as total_pis,
    SUM(valor_cofins) as total_cofins,
    SUM(valor_inss) as total_inss,
    SUM(valor_ir) as total_ir,
    SUM(valor_csll) as total_csll,
    SUM(valor_iss) as total_iss,
    SUM(valor_deducoes) as total_deducoes,
    
    -- Métricas de recuperação
    SUM(valor_pis + valor_cofins + valor_inss + valor_ir + valor_csll) as impostos_federais_retidos,
    
    COUNT(*) as total_nfses,
    MAX(data_emissao) as ultima_nfse
    
  FROM `dados-ems-project.ems_raw.nfse_campinas`
  WHERE tomador_cnpj IS NOT NULL
  GROUP BY 1,2,3
)

SELECT *,
  -- Cálculo do potencial de recuperação
  impostos_federais_retidos * 1.2 as potencial_recuperacao_estimado,  -- 20% adicional estimado
  
  -- Taxa de impostos retidos sobre faturamento
  ROUND((impostos_federais_retidos / NULLIF(valor_servicos_total, 0)) * 100, 2) as taxa_retencao_pct,
  
  -- Classificação do cliente para recuperação
  CASE 
    WHEN impostos_federais_retidos >= 10000 THEN 'ALTO_POTENCIAL'
    WHEN impostos_federais_retidos >= 5000 THEN 'MEDIO_POTENCIAL'
    WHEN impostos_federais_retidos >= 1000 THEN 'BAIXO_POTENCIAL'
    ELSE 'SEM_POTENCIAL'
  END as classificacao_recuperacao,
  
  -- ROI estimado do serviço de recuperação (honorários de 30%)
  impostos_federais_retidos * 0.30 as honorarios_estimados,
  
  -- Score de prioridade para oferta de recuperação
  CASE 
    WHEN impostos_federais_retidos >= 10000 AND DATE_DIFF(CURRENT_DATE(), ultima_nfse, DAY) <= 90 THEN 100
    WHEN impostos_federais_retidos >= 5000 AND DATE_DIFF(CURRENT_DATE(), ultima_nfse, DAY) <= 180 THEN 80
    WHEN impostos_federais_retidos >= 1000 AND DATE_DIFF(CURRENT_DATE(), ultima_nfse, DAY) <= 365 THEN 60
    ELSE 30
  END as score_prioridade_recuperacao

FROM analise_tributaria
ORDER BY impostos_federais_retidos DESC;