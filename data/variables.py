from msilib.schema import tables # Permite leer las tablas de la BBDD
from decouple import config # define los valores predeterminados en el archivo .env

cines_url = config('CINES_URL')
museos_url = config('MUSEOS_URL')
bibliotecas_url = config('BIBLIOTECAS_URL')

DB_USER=config('DB_USER')
DB_PASSWORD=config('DB_PASSWORD')
DB_HOST=config('DB_HOST')
DB_PORT=config('DB_PORT')
DB_DATABASE=config('DB_DATABASE')
db_url=f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"


list_urls=[museos_url,cines_url,bibliotecas_url]
list_category=["museos","cines","bibliotecas" ]

columns_lower_museos=[
	'cod_localidad',
	'id_provincia',
	'id_departamento',
	'observaciones',
	'categoria',
	'subcategoria',
	'provincia',
	'localidad',
	'nombre',
	'domicilio',
	'piso',
	'codigo_postal',
	'cod_area',
	'numero_de_telefono',
	'mail',
	'web',
	'latitud',
	'longitud',
	'tipolatitudlongitud',
	'informacion_adicional',
	'fuente',
	'jurisdiccion',
	'año_inicio',
	'idsinca'
	]

columns_lower_cines=[
	'cod_localidad',
	'id_provincia',
	'id_departamento',
	'observaciones',
	'categoria',
	'provincia',
	'departamento',
	'localidad',
	'nombre',
	'domicilio',
	'piso',
	'codigo_postal',
	'cod_area',
	'numero_de_telefono',
	'mail',
	'web',
	'informacion_adicional',
	'latitud',
	'longitud',
	'tipolatitudlongitud',
	'fuente',
	'tipo_gestion',
	'pantallas',
	'butacas',
	'espacio_incaa',
	'año_actualizacion'
	]

columns_lower_bibliotecas=[
	'cod_localidad',
	'id_provincia',
	'id_departamento',
	'observaciones',
	'categoria',
	'subcategoria',
	'provincia',
	'departamento',
	'localidad',
	'nombre',
	'domicilio',
	'piso',
	'codigo_postal',
	'cod_area',
	'numero_de_telefono',
	'mail',
	'web',
	'informacion_adicional',
	'latitud',
	'longitud',
	'tipolatitudlongitud',
	'fuente',
	'tipo_gestion',
	'año_inicio',
	'año_actualizacion']

df_unique_table=[
	'cod_localidad',
	'id_provincia',
	'id_departamento',
	'categoria',
	'provincia',
	'localidad',
	'nombre',
	'domicilio',
	'codigo_postal',
	'numero_de_telefono',
	'mail',
	'web',
]

list_register=[
	'categoria',
	'fuente',
	'provincia',

]

list_cinemas=[
	'provincia',
	'pantallas',
	'butacas',
	'espacio_incaa',

]

tables_list=[
	'table_info_cinemas.sql',
	'table_total_normalizer.sql',
	'table_total_records.sql',
]