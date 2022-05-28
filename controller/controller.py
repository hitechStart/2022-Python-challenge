from services.get_csv import get_csv
from services.my_data import dataframes
from data.variables import list_category, list_urls
from services.db_connect import create_tables
from data.variables import list_cinemas
from services.logs import my_log

logger=my_log('controller')

def main():
    logger.info('leyendo controlador...')
    try:
        for i in map(get_csv, list_category, list_urls):
            i
    except Exception as e:
        logger.info(e)


def records(register):
    try:
        # devuelve la cantidad de registros por categorías (museo, cine y biblioteca)
        categories_records=register.groupby('categoria').size().to_frame(name = 'registros_por_categoria')

        # devuelve la cantidad de registros por fuentes o endpoints
        source_records=register.groupby(['categoria','fuente']).size().to_frame(name = 'registros_por_fuente')

        # devuelve la cantidad de registros por provincia y categorías
        province_records=register.groupby(['categoria','provincia']).size().to_frame(name='registros_por_provincia')

        # devuelve el total de datos por provincia, fuente y categorías
        total_records_cat_source=categories_records.merge(source_records, how='outer', left_index=True, right_index=True)
        total_records=total_records_cat_source.merge(province_records, how='outer',left_index=True, right_index=True)
        total_records.reset_index(inplace=True)
        total_records=total_records[['categoria','registros_por_categoria','fuente','registros_por_fuente','provincia','registros_por_provincia']]
        return total_records
    except Exception as e:
        logger.info(e)
        exit(1)


def info_cinemas(cinemas):
    try:
        cinemas[list_cinemas[3]]=cinemas[list_cinemas[3]].replace({'si': 1, 'SI': 1,'Si': 1, 'No': 0, 'no': 0, 'NO': 0, '0': 0}).fillna(0)
        cinemas=cinemas.astype({list_cinemas[3]:'int64'})
        cinemas_group=cinemas.groupby(list_cinemas[0])[[list_cinemas[1], list_cinemas[2],list_cinemas[3]]].sum()
        return cinemas_group
    except Exception as e:
        logger.error(e)
        logger.info('error en la columna numero: [espacio_incaa], no contiene un valor valido')
        exit(1)



if __name__ == '__main__':
    main()
    
    concat,register,cinemas=dataframes()
    
    create_tables(concat,"total_normalizer")
    logger.info('Tabla total_normalizer creada de manera exitosa')

    create_tables(records(register),"total_records")
    logger.info('Tabla table total_records creada de manera exitosa')
    
    create_tables(info_cinemas(cinemas),"info_cinemas")
    logger.info('Tabla table info_cinemas creada de manera exitosa')
    logger.info('Fin del programa')