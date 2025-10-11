"""Autenticação com certificado digital"""

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
