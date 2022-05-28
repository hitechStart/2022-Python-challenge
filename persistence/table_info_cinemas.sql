CREATE TABLE IF NOT EXISTS public.info_cinemas (
	provincia text COLLATE pg_catalog."default",
	pantallas bigint,
	butacas bigint,
	espacio_incaa bigint,
	fecha_actualizacion text COLLATE pg_catalog."default"
) TABLESPACE pg_default;