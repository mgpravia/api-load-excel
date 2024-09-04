import yaml
import os

current_directory = os.path.dirname(__file__)
config_file_path = os.path.join(current_directory, 'config.yaml')

print(f"Archivo yaml: {config_file_path}")

with open(config_file_path, 'r') as file:
    config = yaml.safe_load(file)


#Database postgres config
POSTGRES_PROD_HOST = config['postgres']['host']
POSTGRES_PROD_DATABASE = config['postgres']['database']
POSTGRES_PROD_USER = config['postgres']['user']
POSTGRES_PROD_PASSWORD = config['postgres']['password']
POSTGRES_PROD_PORT = config['postgres']['port']


#Variables common
TIMEZONE = config['variables']['timeZone']