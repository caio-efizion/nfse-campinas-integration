"""Configuração de logging estruturado"""

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
