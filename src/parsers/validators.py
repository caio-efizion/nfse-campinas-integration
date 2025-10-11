"""Validadores de dados extraídos"""

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
