# Paquetes internos
import handlerErrors as handlerErrors
from etl.config import get_Connection


def load_tbl_d_tiempo(data):
    conn = get_Connection()
    if conn is None:
        return []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany("""
                INSERT INTO tbl_dimension_tiempo (anio) VALUES (%s) ON CONFLICT (anio) DO NOTHING""",data)
                conn.commit()
        if cur.rowcount > 0:
            handlerErrors.logging.info(f'Insert realizado con exito, fueron {cur.rowcount} registros insetados en la tbl Tiempo!')
        else:
            handlerErrors.logging.info(f'Todos los registros ya existían en la base de datos. Nada nuevo insertado en la tbl Tiempo.')
    except Exception as e:
        handlerErrors.logging.error(f'Error insertando en D tiempo {e}')
    finally:
        conn.close()


def load_tbl_d_game(data):
    conn = get_Connection()
    if conn is None:
        return []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany("INSERT INTO tbl_dimension_game (nombre,id_tiempo_lanzamiento) VALUES (%s,%s) ON CONFLICT (nombre) DO NOTHING",data)
                conn.commit()
        if cur.rowcount > 0:
            handlerErrors.logging.info(f'Insert realizado con exito, fueron {cur.rowcount} registros insetados en la tbl Game!')
        else:
            handlerErrors.logging.info(f'Todos los registros ya existían en la base de datos. Nada nuevo insertado en la tbl Game.')
    except Exception as e:
        handlerErrors.logging.error(f'Error insertando en D Game {e}')
    finally:
        conn.close()


def load_tbl_d_plataform(data):
    conn = get_Connection()
    if conn is None:
        return []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany("INSERT INTO tbl_dimension_plataforma (nombre) VALUES (%s) ON CONFLICT (nombre) DO NOTHING",data)
                conn.commit()
        if cur.rowcount > 0:
            handlerErrors.logging.info(f'Insert realizado con exito, fueron {cur.rowcount} registros insetados en la tbl plataforma!')
        else:
            handlerErrors.logging.info(f'Todos los registros ya existían en la base de datos. Nada nuevo insertado en la tbl plataforma.')
    except Exception as e:
        handlerErrors.logging.error(f'Error insertando en D plataforma {e}')
    finally:
        conn.close()        


def load_tbl_d_editorial(data):
    conn = get_Connection()
    if conn is None:
        return []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany("INSERT INTO tbl_dimension_editorial (nombre) VALUES (%s) ON CONFLICT (nombre) DO NOTHING",data)
                conn.commit()
        if cur.rowcount > 0:
            handlerErrors.logging.info(f'Insert realizado con exito, fueron {cur.rowcount} registros insetados en la tbl editorial!')
        else:
            handlerErrors.logging.info(f'Todos los registros ya existían en la base de datos. Nada nuevo insertado en la tbl editorial.')
    except Exception as e:
        handlerErrors.logging.error(f'Error insertando en tbl editorial {e}')
    finally:
        conn.close()


def load_tbl_fact_venta(data):
    conn = get_Connection()
    if conn is None:
        return []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany("INSERT INTO ventas (id_game,id_edito,id_plata,id_tiempo,valor_venta) VALUES (%s,%s,%s,%s,%s)",data)
                conn.commit()
        if cur.rowcount > 0:
            handlerErrors.logging.info(f'Insert realizado con exito, fueron {cur.rowcount} registros insetados en la tbl ventas!')
        else:
            handlerErrors.logging.info(f'Todos los registros ya existían en la base de datos. Nada nuevo insertado en la tbl ventas.')
    except Exception as e:
        handlerErrors.logging.error(f'Error insertando en tbl ventas {e}')
    finally:
        conn.close()   



if __name__ == '__main__':
    handlerErrors.logging.error('This is not a main module, please execute main module')