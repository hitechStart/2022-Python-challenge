from pathlib import Path, PurePath
import requests
from tqdm.auto import tqdm

# modulos
from services.get_date import path_year, fecha
from services.make_dirs import make_dirs
from services.logs import my_log

logger=my_log(__name__)

def get_csv(category,urls):
    
    chunk_size=1024
    intento=0
    files_path = PurePath.joinpath(Path.cwd(),"resources\\",category,path_year)
    
    while True:
        if intento>=10:
            logger.error(f'fallo la descarga desde el sitio, intento numero: {intento}/10')
            exit(1)
            

        try:
            req= requests.get(urls, stream = True, timeout=15000, allow_redirects=True)
            total_size = int(req.headers.get('content-length'))
            break
        # except Exception as e:
        except TypeError:
            intento+=1
            logger.warning(f'fallo intento: {intento}/10 in {category}')
    try:
        logger.info('Creacion de directorios')
        make_dirs(category,path_year)
        logger.info(f'Descargando archivo: {category}')
        with open(f'{files_path}/{category}-{fecha}.csv','wb') as f:
            for chunk in tqdm(iterable=req.iter_content(chunk_size=chunk_size),total = round(total_size/chunk_size), unit = 'KB'):
                f.write(chunk)
            logger.info(f'Almacenando archivo: {category}')

        logger.info(f'Destino del archivo: {files_path}')
    except Exception as e:
        logger.error(e)
