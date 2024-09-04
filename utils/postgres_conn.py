import psycopg2
import config.configuration as config


class PostgresDatabase:

    def get_connection():
        try:
            conn = psycopg2.connect(
                host= config.POSTGRES_PROD_HOST,
                database=config.POSTGRES_PROD_DATABASE,
                user=config.POSTGRES_PROD_USER,
                password=config.POSTGRES_PROD_PASSWORD,
                port=config.POSTGRES_PROD_PORT
            )

            return conn

        except Exception as e:
            print(f"Ocurrio um error al establecer conexion con postgres: {str(e)}")    