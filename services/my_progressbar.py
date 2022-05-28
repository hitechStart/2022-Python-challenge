from time import sleep

longitud=40
percen=0
linea =('🚧')*(longitud+3)
print(f'la barra del kpi\n{linea}\n')
for i in range(longitud):
	cuadra='💹'*(i)
	espacio='🟥'*(longitud-(i))
	percen=(i*100)/longitud
	print(f'\r{percen}%[{cuadra}{espacio}]{i}/{longitud}', end='\r', flush=True)
	sleep(0.3)
cuadra=('💹')*longitud
print(f'\r estado: [{cuadra}] descarga completada')
print(f'\n{linea}\n')
