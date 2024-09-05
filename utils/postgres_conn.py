import psycopg2
import config.configuration as config
from handler.error_handler import AppException
from http import HTTPStatus
from utils.format_log import message_format

class PostgresDatabase:

    def get_connection():
        try:
            conn = None

            if config.POSTGRES_PROD_ACTIVE:

                conn = psycopg2.connect(
                    host= config.POSTGRES_PROD_HOST,
                    database=config.POSTGRES_PROD_DATABASE,
                    user=config.POSTGRES_PROD_USER,
                    password=config.POSTGRES_PROD_PASSWORD,
                    port=config.POSTGRES_PROD_PORT
                )
            elif config.POSTGRES_DESA_ACTIVE:    
                conn = psycopg2.connect(
                    host= config.POSTGRES_DESA_HOST,
                    database=config.POSTGRES_DESA_DATABASE,
                    user=config.POSTGRES_DESA_USER,
                    password=config.POSTGRES_DESA_PASSWORD,
                    port=config.POSTGRES_DESA_PORT
                )
            else:
                 raise AppException("database not configured", HTTPStatus.INTERNAL_SERVER_ERROR)   

            return conn
        
        except AppException as e:       
            raise AppException(f"{str(e)}", e.status_code)
        except Exception as e:
            message_format(f"Error establishing connection to the database: {str(e)}") 
            raise AppException("Error establishing connection to the database", HTTPStatus.INTERNAL_SERVER_ERROR)