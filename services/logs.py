import logging   # Reporte de eventos hacia la salida de la consola
def my_log(name):
	logger = logging.getLogger(name)
	logger.setLevel(logging.DEBUG)
	# Crea archivo de log que registre los mensajes de depuración
	debug_logger = logging.FileHandler('app.log')
	debug_logger.setLevel(logging.DEBUG)
	# Crea un controlador de consola con un nivel de registro más alto
	info_logger = logging.StreamHandler()
	info_logger.setLevel(logging.INFO)
	# Crea un formateador que se agrega a los controladores
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	info_logger.setFormatter(formatter)
	debug_logger.setFormatter(formatter)
	# Se agrega los controladores al manejador del controlador
	logger.addHandler(info_logger)
	logger.addHandler(debug_logger)

	return logger