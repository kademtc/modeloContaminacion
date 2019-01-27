import json
import requests
import tools
import sys


class Meteorology:

    # Constructor
    def __init__(self, keyAemet, headers, querystring, startDay, endDay):
        self.keyAemet = keyAemet
        self.headers = headers
        self.querystring = querystring
        self.startDay = startDay
        self.endDay = endDay

    # @description: Hace una pausa en la ejecución del número pasado por parámetro
    # @params
    #   startDay: Día desde el que obtener datos.
    #   endDay: Día hasta el que obtener datos.
    #   estacion: Estación que mide los datos
    def getRegMad(self, estacion, startDay, endDay):
        urlEstacion = "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/" + \
                      startDay + "T00:00:00UTC/fechafin/" + endDay \
                      + "T23:59:59UTC/estacion/" + estacion + "/?api_key=" + self.keyAemet
        rutaJsonEstacion = json.loads(
            requests.request("GET", urlEstacion, headers=self.headers, params=self.querystring).text)
        if rutaJsonEstacion['estado'] == 200:
            try:
                element = requests.get(rutaJsonEstacion['datos'], headers=self.headers, params=self.querystring).json()
            except:
                print(sys.exc_info())
                return '404'
            # element = json.loads(requests.request("GET", rutaJsonEstacion['datos'], headers=self.headers, params=self.querystring).content)
        elif rutaJsonEstacion['estado'] == 400 or rutaJsonEstacion['estado'] == 429:
            return '400'
        elif rutaJsonEstacion['estado'] == 404:
            return '404'
        return element[0]

    def getStations(self, urlStations, state):
        rutaJsonEstaciones = json.loads(
            requests.request("GET", urlStations, headers=self.headers, params=self.querystring).text)
        datosEstaciones = json.loads(requests.request("GET", rutaJsonEstaciones['datos'], headers=self.headers,
                                                      params=self.querystring).text)
        # Filtro por las estaciones de Madrid
        estaciones = [estacion for estacion in datosEstaciones if estacion['provincia'] == state and state in estacion['nombre']]
        return estaciones

    def getSensorData(self, stations):
        datosList = []
        for estacion in stations:
            for single_date in tools.daterange(self.startDay, self.endDay):
                elemento = self.getRegMad(estacion['indicativo'], single_date.strftime("%Y-%m-%d"),
                                          single_date.strftime("%Y-%m-%d"))
                print(elemento)
                temporizador = 60
                while elemento == '400':
                    tools.temporizadorDisplay(temporizador)
                    temporizador += 10
                    elemento = self.getRegMad(estacion['indicativo'], single_date.strftime("%Y-%m-%d"),
                                              single_date.strftime("%Y-%m-%d"))
                    print(elemento)
                if elemento != "404":
                    datosList.append(elemento)
        return datosList
