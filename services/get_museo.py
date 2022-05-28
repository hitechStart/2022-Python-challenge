from pathlib import Path, PurePath
import requests
from tqdm.auto import tqdm

# modules
from services.get_date import path_year, fecha
from services.make_dirs import make_dirs

def get_path(category,urls):
    chunk_size=1024
    intento=0
    files_path = PurePath.joinpath(Path.cwd(),"resources\\",category,path_year)
    print(f'Descargando archivo: {category}')
    # req= requests.get(urls)
    while True:
        try:
            req= requests.get(urls, stream = True, timeout=15000, allow_redirects=True)
            total_size = int(req.headers.get('content-length'))
            break
        except TypeError:
            intento+=1
            print(f'Falló intento numero: {intento}')
    print('Creacion de directorios')
    make_dirs(category,path_year)
    with open(f'{files_path}/{category}-{fecha}.png','wb') as f:
        print(f'Almacenando archivo: {category}')
        for chunk in tqdm(iterable=req.iter_content(chunk_size=chunk_size),total = round(total_size/chunk_size), unit = 'KB'):
            f.write(chunk)

    print(f'Destino del archivo: {files_path}')

# borrar esta parte 

from variables import museos_url as cat_url

if __name__ == '__main__':
    get_path('museos', cat_url)