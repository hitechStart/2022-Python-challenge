# [Challenge Data analitics Alkemy.org](Challenge%20Data%20Analytics%20con%20Python.pdf)


<div markdown='1' align='center'>

 <a href="https://www.alkemy.org" target="_blank"><img style="background: white; width: 90vw" src="https://www.alkemy.org/static/media/alkemy-logo.b806d51f.svg" alt="" /></a>

</div>

## Requerimientos

- Lenguaje de Programación
  - Python 3.7+
- Una base de datos PostgreSQL
- git
- virtualenv (puedes utilizar la librería de tu preferencia, pero aquí seguiremos con ésta para crear entornos virtuales)
- Pip para instalar librerías

## Instalación

### Clonamos el repositorio

```shell
git clone https://SabrinaBenitez@bitbucket.org/sabrinabenitez/alkemy_challenge_python.git 
```

### Creación de entorno virtual con virtualenv

Navegamos a nuestra nueva carpeta:

```shell
cd alkemy_challenge_python
```

Creamos nuestro entorno virtual con virtualenv, si no lo tienes instalado lo haces con el siguiente comando:

```shell
pip install virtualenv
```

Ahora creamos el entorno


```shell
virtualenv venv
```

Activar el entorno virtual:

```shell
.\venv\Scripts\activate
```

Instalamos las librerías necesarias

```shell
pip install -r requirements.txt
```

creamos el archivo `.env`

```shell
mkdir .env
```

En el agregamos las urls de descarga de los datos y los parámetros de conexión a nuestra base de datos como se indica en el archivo en el archivo de extension .env: `.env`

```
BIBLIOTECAS_URL=https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv

MUSEOS_URL=https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv

CINES_URL=https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv

DB_USER=Coloque aquí su usuario postgres
DB_PASSWORD=Coloque aquí su contraseña
DB_HOST=Coloque aquí el host
DB_PORT=Coloque aquí el puerto
DB_DATABASE=Coloque aquí el nómbre de su base de datos PostgreSQL
```

El script crea las tablas en la base de datos durante su ejecución pero si lo prefiere puede crearlas usted ejecutando el siguiente comando:

```shell
python -m services.db_connect
```

## Ejecución del script

```shell
python -m controller.controller
```

El script guardará los datos finales en la base de datos, entregará información de su ejecución con mensajes en la consola y guardará registro en el archivo `app.log`

<br>
<br>
<br>

---
