import pandas as pd
import numpy as np
import handlerErrors as handlerErrors
from config import get_Connection


def readFile(path_file):
    try:
        df = pd.read_excel(path_file)
        return df
    except Exception as e:
         handlerErrors.logging.error(f'Ups,algo salio mal {e}')


#SQL
def get_data_from_DB(tbl,columns):
    allowed_tables = [
        'tbl_dimension_tiempo',
        'tbl_dimension_game',
        'tbl_dimension_plataforma',
        'tbl_dimension_editorial',
        'ventas'
    ]

    allowed_columns = {
        'tbl_dimension_tiempo':['id_tiempo','anio'],
        'tbl_dimension_game':['id_game','nombre'],
        'tbl_dimension_plataforma':['id_plata','nombre'],
        'tbl_dimension_editorial':['id_edito','nombre'],
        'ventas':['id_ventas','id_game','id_edito','id_plata','id_tiempo','valor_venta']
    }

    if tbl not in allowed_tables:
        handlerErrors.logging.error(f'Tabla no permitada!')

    for col in columns:
        if col not in allowed_columns[tbl]:
            handlerErrors.logging.error(f'Columna {col} no permitida en la tabla {tbl}')

    columns_sql = ', '.join(columns)
    query = f'Select {columns_sql} from {tbl}'

    conn = get_Connection()
    if conn is None:
        return []

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(query)
                resultQuery = cur.fetchall()
                conn.commit()
            if cur.rowcount > 0:
                handlerErrors.logging.info(f'Consulta realizada con exito a tbl {tbl}')
                return resultQuery
            else:
                return handlerErrors.logging.info(f'No hay data en la tabla {tbl}!')
    except Exception as e:
        return handlerErrors.logging.error(f'Error consultando la tabla {tbl}, {e}')
    finally:
        conn.close()