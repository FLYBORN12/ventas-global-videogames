import psycopg2
import os
from dotenv import load_dotenv
import handlerErrors as handlerErrors


load_dotenv()

user = os.getenv('USER_DB')
db = os.getenv('NAME_DB')
password = os.getenv('PASS_DB')
host = os.getenv('HOST_DB')
port = os.getenv('PORT_DB')



def get_Connection():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=db,
            user=user,
            password=password
        )
        return conn
    except psycopg2.Error as e:
        handlerErrors.logging.critical(f"Error al conectar a PostgreSQL: {e}")
        exit()





