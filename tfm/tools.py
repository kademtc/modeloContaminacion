import sys
import time
from datetime import timedelta, date


# @description: Hace una pausa en la ejecución del número pasado por parámetro
# @params
#   segundos: número de segundos a esperar.
def temporizadorDisplay(seconds):
    for remaining in range(seconds, 0, -1):
        sys.stdout.write("\r")
        sys.stdout.write("{:2d} seconds left.".format(remaining))
        sys.stdout.flush()
        time.sleep(1)

    sys.stdout.write("\rReady!            \n")


# Obtención de un rango de dias
def daterange(startDay, endDay):
    for n in range(int((endDay - startDay).days)):
        yield startDay + timedelta(n)

