from pathlib import Path, PurePath # manipula las rutas de windows para ejecutar el codigo
from sqlalchemy import create_engine, text # herramienta de mapeo relacional de objetos (ORM) 
# que traduce las clases de Python a tablas en bases de datos relacionales
from sqlalchemy.exc import OperationalError
from psycopg2 import errors #El contenido del módulo se genera a partir del código fuente de PostgreSQL 
# e incluye clases para cada error definido por PostgreSQL
from data.variables import db_url, tables_list
from services.get_date import fecha
from services.logs import my_log

logger=my_log(__name__)

engine = create_engine(db_url)
# metodo que establece la conexion hacia la BBDD y crea las tablas de datos.
def create_tables(data,table_name):
    
    try:
        # Logging: Reporte de eventos hacia la salida de la consola
        logger.info('leyendo db_connect...')
        logger.info('Realizando la conexion a la base de datos...')
        data.assign(fecha_actualizacion=fecha).to_sql(table_name, con=engine,if_exists="replace")
        logger.info('Conexion exitosa')
    # tipos de excepciones:
    except (OperationalError, errors.OperationalError ):
        logger.error('Error durante la conexión hacia la base de datos')
        quit(1)

def create_table():
    for table in tables_list:
        try:
            with engine.connect() as conn:
                logger.info(f'Leyendo tabla de datos {table}')
                file = open( PurePath.joinpath(Path.cwd(),"persistence\\"+table))
                query = text(file.read())
                logger.info('estableciendo comunicacion con la base de datos...')
                conn.execute(query)
                logger.info(f'Tabla {table} creada de manera exitosa')
        except (OperationalError, errors.OperationalError ):
            logger.error(f'Error durante la conexión hacia la tabla llamada: {table}')
            quit(1)

if __name__ == '__main__':
    create_table()