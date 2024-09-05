import yaml
import os

current_directory = os.path.dirname(__file__)
config_file_path = os.path.join(current_directory, 'config.yaml')

with open(config_file_path, 'r') as file:
    config = yaml.safe_load(file)


#DATABASE CONFIG
POSTGRES_PROD_HOST = config['postgres']['prod']['host']
POSTGRES_PROD_DATABASE = config['postgres']['prod']['database']
POSTGRES_PROD_USER = config['postgres']['prod']['user']
POSTGRES_PROD_PASSWORD = config['postgres']['prod']['password']
POSTGRES_PROD_PORT = config['postgres']['prod']['port']
POSTGRES_PROD_ACTIVE = config['postgres']['prod']['active']

POSTGRES_DESA_HOST = config['postgres']['desa']['host']
POSTGRES_DESA_DATABASE = config['postgres']['desa']['database']
POSTGRES_DESA_USER = config['postgres']['desa']['user']
POSTGRES_DESA_PASSWORD = config['postgres']['desa']['password']
POSTGRES_DESA_PORT = config['postgres']['desa']['port']
POSTGRES_DESA_ACTIVE = config['postgres']['desa']['active']

#Common constants
TIMEZONE = config['constantes']['timeZone']