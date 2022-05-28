CREATE TABLE IF NOT EXISTS public.total_records (
	index bigint,
	categoria text COLLATE pg_catalog."default",
	registros_por_categoria bigint,
	fuente text COLLATE pg_catalog."default",
	registros_por_fuente bigint,
	provincia text COLLATE pg_catalog."default",
	registros_por_provincia bigint,
	fecha_actualizacion text COLLATE pg_catalog."default"
) TABLESPACE pg_default;