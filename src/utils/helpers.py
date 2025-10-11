"""Funções auxiliares e helpers"""

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
