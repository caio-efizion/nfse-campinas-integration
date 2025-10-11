"""Script principal de extração NFSe"""

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
