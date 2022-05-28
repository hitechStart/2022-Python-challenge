from datetime import date

fecha = date.today().strftime('%d-%m-%Y')

months = (
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre"
)

day = date.today().day

month = months[date.today().month - 1]

year = date.today().year

path_year = f'{year}-{month}'
