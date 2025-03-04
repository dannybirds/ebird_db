import configparser
import os
from pathlib import Path

def load_config(config_path:str|None = None) -> dict[str, str|None]:
    """
    Load configuration from a file or environment variables.
    
    Args:
        config_path: Path to configuration file, or None to use default locations
                     and fall back to environment variables
    
    Returns:
        dict: Configuration parameters
    """
    config: dict[str, str|None] = {
        'postgres_user': None,
        'postgres_pwd': None,
        'ebird_api_key': None,
        'db_name': 'ebird_us'
    }
    
    # Try loading from file
    if config_path is None:
        # Look in standard locations
        locations = [
            Path('config.ini'),
            Path.home() / '.ebird_db' / 'config.ini',
            Path.home() / '.config' / 'ebird_db' / 'config.ini'
        ]
        
        for loc in locations:
            if loc.exists():
                config_path = str(loc)
                break
    
    if config_path and Path(config_path).exists():
        cfg = configparser.ConfigParser()
        cfg.read(config_path)
        if 'database' in cfg:
            config['postgres_user'] = cfg.get('database', 'user', fallback=None)
            config['postgres_pwd'] = cfg.get('database', 'password', fallback=None)
            config['db_name'] = cfg.get('database', 'name', fallback='ebird')
        if 'api' in cfg:
            config['ebird_api_key'] = cfg.get('api', 'ebird_key', fallback=None)
    
    # Fall back to environment variables
    config['postgres_user'] = config['postgres_user'] or os.getenv('POSTGRES_USER')
    config['postgres_pwd'] = config['postgres_pwd'] or os.getenv('POSTGRES_PWD')
    config['ebird_api_key'] = config['ebird_api_key'] or os.getenv('EBIRD_API_KEY')
    
    # Verify we have required values
    missing = [k for k, v in config.items() if v is None and k != 'db_name']
    if missing:
        raise ValueError(f"Missing required configuration values: {', '.join(missing)}")
    
    return config