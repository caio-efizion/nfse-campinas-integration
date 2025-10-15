#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMS Analytics - Integração NFSe Campinas
Específico para Recuperação Tributária
"""

import os
import sys
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12
import base64
import hashlib
from dotenv import load_dotenv

# Carregar configurações
load_dotenv('config/.env')

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/nfse_integration.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class NFSeCampinasIntegration:
    """Integração com NFSe Campinas para EMS Project"""
    
    def __init__(self):
        self.config = {
            'PROJECT_ID': os.getenv('PROJECT_ID', 'dados-ems-project'),
            'DATASET_RAW': os.getenv('DATASET_RAW', 'ems_raw'),
            'CERT_PATH': 'config/certificados/certificado.pfx',
            'CERT_PASSWORD': os.getenv('CERT_PASSWORD'),
            'CLIENTE_CNPJ': os.getenv('CLIENTE_CNPJ'),
            'CLIENTE_INSCRICAO': os.getenv('CLIENTE_INSCRICAO_MUNICIPAL'),
            'WSDL_URL': 'https://issdigital.campinas.sp.gov.br/notafiscal-abrasfv203-ws/NotaFiscalSoap?wsdl'
        }
        
        # Inicializar BigQuery
        self.bq_client = bigquery.Client.from_service_account_json(
            'config/gcp-credentials.json',
            project=self.config['PROJECT_ID']
        )
        
        logger.info("NFSe Campinas Integration inicializado")
    
    def load_certificate(self):
        """Carregar certificado digital"""
        try:
            with open(self.config['CERT_PATH'], 'rb') as cert_file:
                cert_data = cert_file.read()
            
            private_key, certificate, additional_certificates = pkcs12.load_key_and_certificates(
                cert_data, 
                self.config['CERT_PASSWORD'].encode()
            )
            
            logger.info("Certificado digital carregado com sucesso")
            return private_key, certificate
            
        except Exception as e:
            logger.error(f"Erro ao carregar certificado: {e}")
            raise
    
    def create_soap_envelope(self, cnpj, data_inicio, data_fim):
        """Criar envelope SOAP para consulta NFSe"""
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                       xmlns:nfse="http://www.betha.com.br/e-nota-contribuinte-ws">
            <soap:Header/>
            <soap:Body>
                <nfse:ConsultarNfseEnvio>
                    <ConsultarNfseEnvio xmlns="http://www.betha.com.br/e-nota-contribuinte-ws">
                        <Prestador>
                            <CpfCnpj>
                                <Cnpj>{cnpj}</Cnpj>
                            </CpfCnpj>
                            <InscricaoMunicipal>{self.config['CLIENTE_INSCRICAO']}</InscricaoMunicipal>
                        </Prestador>
                        <PeriodoEmissao>
                            <DataInicial>{data_inicio}</DataInicial>
                            <DataFinal>{data_fim}</DataFinal>
                        </PeriodoEmissao>
                    </ConsultarNfseEnvio>
                </nfse:ConsultarNfseEnvio>
            </soap:Body>
        </soap:Envelope>"""
        
        return soap_body
    
    def consultar_nfse_periodo(self, data_inicio, data_fim):
        """Consultar NFSe por período"""
        try:
            # Criar envelope SOAP
            soap_envelope = self.create_soap_envelope(
                self.config['CLIENTE_CNPJ'], 
                data_inicio.strftime('%Y-%m-%d'),
                data_fim.strftime('%Y-%m-%d')
            )
            
            # Carregar certificado
            private_key, certificate = self.load_certificate()
            
            # Headers da requisição
            headers = {
                'Content-Type': 'text/xml; charset=utf-8',
                'SOAPAction': 'ConsultarNfse'
            }
            
            # Fazer requisição SOAP
            response = requests.post(
                self.config['WSDL_URL'],
                data=soap_envelope,
                headers=headers,
                cert=(certificate, private_key),
                timeout=30
            )
            
            if response.status_code == 200:
                return self.parse_nfse_response(response.text)
            else:
                logger.error(f"Erro na consulta NFSe: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Erro ao consultar NFSe: {e}")
            return []
    
    def parse_nfse_response(self, xml_response):
        """Parsear resposta XML da NFSe"""
        try:
            root = ET.fromstring(xml_response)
            nfses = []
            
            # Namespace para NFSe
            ns = {'nfse': 'http://www.betha.com.br/e-nota-contribuinte-ws'}
            
            # Procurar por CompNfse (NFSe completa)
            for comp_nfse in root.findall('.//nfse:CompNfse', ns):
                nfse_data = self.extract_nfse_data(comp_nfse, ns)
                if nfse_data:
                    nfses.append(nfse_data)
            
            logger.info(f"Processadas {len(nfses)} NFSes")
            return nfses
            
        except Exception as e:
            logger.error(f"Erro ao parsear XML NFSe: {e}")
            return []
    
    def extract_nfse_data(self, comp_nfse, ns):
        """Extrair dados estruturados da NFSe"""
        try:
            nfse = comp_nfse.find('.//nfse:Nfse', ns)
            if nfse is None:
                return None
            
            # Dados da NFSe
            inf_nfse = nfse.find('.//nfse:InfNfse', ns)
            
            data = {
                'numero_nfse': self.get_xml_text(inf_nfse, './/nfse:Numero', ns),
                'codigo_verificacao': self.get_xml_text(inf_nfse, './/nfse:CodigoVerificacao', ns),
                'data_emissao': self.get_xml_text(inf_nfse, './/nfse:DataEmissao', ns),
                'data_competencia': self.get_xml_text(inf_nfse, './/nfse:Competencia', ns),
                
                # Prestador de serviço (EMS Project)
                'prestador_cnpj': self.get_xml_text(inf_nfse, './/nfse:PrestadorServico//nfse:Cnpj', ns),
                'prestador_razao_social': self.get_xml_text(inf_nfse, './/nfse:PrestadorServico//nfse:RazaoSocial', ns),
                
                # Tomador do serviço (Cliente da EMS)
                'tomador_cnpj': self.get_xml_text(inf_nfse, './/nfse:TomadorServico//nfse:Cnpj', ns),
                'tomador_cpf': self.get_xml_text(inf_nfse, './/nfse:TomadorServico//nfse:Cpf', ns),
                'tomador_razao_social': self.get_xml_text(inf_nfse, './/nfse:TomadorServico//nfse:RazaoSocial', ns),
                'tomador_endereco': self.get_xml_text(inf_nfse, './/nfse:TomadorServico//nfse:Endereco', ns),
                'tomador_municipio': self.get_xml_text(inf_nfse, './/nfse:TomadorServico//nfse:Municipio', ns),
                'tomador_uf': self.get_xml_text(inf_nfse, './/nfse:TomadorServico//nfse:Uf', ns),
                'tomador_cep': self.get_xml_text(inf_nfse, './/nfse:TomadorServico//nfse:Cep', ns),
                
                # Dados do serviço
                'valor_servicos': self.get_xml_float(inf_nfse, './/nfse:Servico//nfse:ValorServicos', ns),
                'valor_deducoes': self.get_xml_float(inf_nfse, './/nfse:Servico//nfse:ValorDeducoes', ns),
                'valor_pis': self.get_xml_float(inf_nfse, './/nfse:Servico//nfse:ValorPis', ns),
                'valor_cofins': self.get_xml_float(inf_nfse, './/nfse:Servico//nfse:ValorCofins', ns),
                'valor_inss': self.get_xml_float(inf_nfse, './/nfse:Servico//nfse:ValorInss', ns),
                'valor_ir': self.get_xml_float(inf_nfse, './/nfse:Servico//nfse:ValorIr', ns),
                'valor_csll': self.get_xml_float(inf_nfse, './/nfse:Servico//nfse:ValorCsll', ns),
                'valor_iss': self.get_xml_float(inf_nfse, './/nfse:Servico//nfse:ValorIss', ns),
                'valor_liquido': self.get_xml_float(inf_nfse, './/nfse:Servico//nfse:ValorLiquidoNfse', ns),
                
                'discriminacao': self.get_xml_text(inf_nfse, './/nfse:Servico//nfse:Discriminacao', ns),
                'item_lista_servico': self.get_xml_text(inf_nfse, './/nfse:Servico//nfse:ItemListaServico', ns),
                
                # Metadados de processamento
                'data_processamento': datetime.now().strftime('%Y-%m-%d'),
                'origem_consulta': 'nfse_campinas_api',
                'hash_nfse': None  # Será calculado
            }
            
            # Calcular hash da NFSe para controle de duplicatas
            hash_string = f"{data['numero_nfse']}{data['codigo_verificacao']}{data['data_emissao']}"
            data['hash_nfse'] = hashlib.sha256(hash_string.encode()).hexdigest()
            
            return data
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados da NFSe: {e}")
            return None
    
    def get_xml_text(self, parent, xpath, ns):
        """Extrair texto de elemento XML"""
        element = parent.find(xpath, ns)
        return element.text if element is not None else None
    
    def get_xml_float(self, parent, xpath, ns):
        """Extrair valor float de elemento XML"""
        text = self.get_xml_text(parent, xpath, ns)
        try:
            return float(text) if text else 0.0
        except ValueError:
            return 0.0
    
    def load_to_bigquery(self, nfse_data):
        """Carregar dados NFSe no BigQuery"""
        if not nfse_data:
            logger.warning("Nenhum dado NFSe para carregar")
            return False
        
        try:
            # Converter para DataFrame
            df = pd.DataFrame(nfse_data)
            
            # Tabela de destino
            table_id = f"{self.config['PROJECT_ID']}.{self.config['DATASET_RAW']}.nfse_campinas"
            
            # Verificar se tabela existe, senão criar
            try:
                table = self.bq_client.get_table(table_id)
            except:
                # Criar tabela com schema apropriado
                schema = [
                    bigquery.SchemaField("numero_nfse", "STRING"),
                    bigquery.SchemaField("codigo_verificacao", "STRING"),
                    bigquery.SchemaField("data_emissao", "DATE"),
                    bigquery.SchemaField("data_competencia", "DATE"),
                    bigquery.SchemaField("prestador_cnpj", "STRING"),
                    bigquery.SchemaField("prestador_razao_social", "STRING"),
                    bigquery.SchemaField("tomador_cnpj", "STRING"),
                    bigquery.SchemaField("tomador_cpf", "STRING"),
                    bigquery.SchemaField("tomador_razao_social", "STRING"),
                    bigquery.SchemaField("tomador_endereco", "STRING"),
                    bigquery.SchemaField("tomador_municipio", "STRING"),
                    bigquery.SchemaField("tomador_uf", "STRING"),
                    bigquery.SchemaField("tomador_cep", "STRING"),
                    bigquery.SchemaField("valor_servicos", "FLOAT"),
                    bigquery.SchemaField("valor_deducoes", "FLOAT"),
                    bigquery.SchemaField("valor_pis", "FLOAT"),
                    bigquery.SchemaField("valor_cofins", "FLOAT"),
                    bigquery.SchemaField("valor_inss", "FLOAT"),
                    bigquery.SchemaField("valor_ir", "FLOAT"),
                    bigquery.SchemaField("valor_csll", "FLOAT"),
                    bigquery.SchemaField("valor_iss", "FLOAT"),
                    bigquery.SchemaField("valor_liquido", "FLOAT"),
                    bigquery.SchemaField("discriminacao", "STRING"),
                    bigquery.SchemaField("item_lista_servico", "STRING"),
                    bigquery.SchemaField("data_processamento", "DATE"),
                    bigquery.SchemaField("origem_consulta", "STRING"),
                    bigquery.SchemaField("hash_nfse", "STRING")
                ]
                
                table = bigquery.Table(table_id, schema=schema)
                table = self.bq_client.create_table(table)
                logger.info(f"Tabela {table_id} criada")
            
            # Inserir dados (evitar duplicatas por hash)
            inserted_count = 0
            for _, row in df.iterrows():
                # Verificar se já existe
                query = f"""
                SELECT COUNT(*) as count 
                FROM `{table_id}` 
                WHERE hash_nfse = '{row['hash_nfse']}'
                """
                
                result = list(self.bq_client.query(query))
                if result[0].count == 0:
                    # Inserir novo registro
                    errors = self.bq_client.insert_rows_json(table_id, [row.to_dict()])
                    if not errors:
                        inserted_count += 1
                    else:
                        logger.warning(f"Erro ao inserir NFSe {row['numero_nfse']}: {errors}")
            
            logger.info(f"Inseridas {inserted_count} novas NFSes de {len(df)} processadas")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados no BigQuery: {e}")
            return False
    
    def consultar_historico(self, meses_atras=24):
        """Consultar histórico de NFSe"""
        logger.info(f"Iniciando consulta histórica ({meses_atras} meses)")
        
        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(days=meses_atras * 30)
        
        # Consultar em períodos de 1 mês para evitar timeouts
        current_date = data_inicio
        total_nfses = []
        
        while current_date < data_fim:
            periodo_fim = min(current_date + timedelta(days=30), data_fim)
            
            logger.info(f"Consultando período: {current_date.strftime('%Y-%m-%d')} a {periodo_fim.strftime('%Y-%m-%d')}")
            
            nfses_periodo = self.consultar_nfse_periodo(current_date, periodo_fim)
            total_nfses.extend(nfses_periodo)
            
            current_date = periodo_fim + timedelta(days=1)
        
        # Carregar no BigQuery
        if total_nfses:
            self.load_to_bigquery(total_nfses)
            logger.info(f"Consulta histórica concluída: {len(total_nfses)} NFSes processadas")
        else:
            logger.warning("Nenhuma NFSe encontrada no período histórico")
        
        return len(total_nfses)
    
    def consultar_incremento(self):
        """Consultar incremento desde última execução"""
        # Consultar últimas 72 horas para garantir captura completa
        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(hours=72)
        
        logger.info(f"Consultando incremento: {data_inicio.strftime('%Y-%m-%d %H:%M')} a {data_fim.strftime('%Y-%m-%d %H:%M')}")
        
        nfses = self.consultar_nfse_periodo(data_inicio, data_fim)
        
        if nfses:
            self.load_to_bigquery(nfses)
            logger.info(f"Consulta incremental concluída: {len(nfses)} NFSes processadas")
        else:
            logger.info("Nenhuma NFSe nova encontrada")
        
        return len(nfses)

def main():
    """Função principal"""
    try:
        integration = NFSeCampinasIntegration()
        
        # Verificar argumentos
        if len(sys.argv) > 1 and sys.argv[1] == 'historico':
            # Consulta histórica
            meses = int(sys.argv[2]) if len(sys.argv) > 2 else 24
            integration.consultar_historico(meses)
        else:
            # Consulta incremental (padrão para n8n)
            integration.consultar_incremento()
            
    except Exception as e:
        logger.error(f"Erro na execução: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()