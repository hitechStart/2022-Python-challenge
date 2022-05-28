CREATE TABLE IF NOT EXISTS public.total_normalizer (
	index bigint,
	cod_localidad bigint,
	id_provincia bigint,
	id_departamento bigint,
	categoria text COLLATE pg_catalog."default",
	provincia text COLLATE pg_catalog."default",
	localidad text COLLATE pg_catalog."default",
	nombre text COLLATE pg_catalog."default",
	domicilio text COLLATE pg_catalog."default",
	codigo_postal text COLLATE pg_catalog."default",
	numero_de_telefono text COLLATE pg_catalog."default",
	mail text COLLATE pg_catalog."default",
	web text COLLATE pg_catalog."default",
	fecha_actualizacion text COLLATE pg_catalog."default"
) TABLESPACE pg_default;