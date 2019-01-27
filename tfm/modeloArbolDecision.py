# 1. Se importan librerias
import json
from datetime import timedelta, date
import pyspark as py
from pyspark.sql.functions import lit, concat
from pyspark.sql import SQLContext
from file import WorkingFile
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import sensoresAmbientales

# 2. Generacion de variables
startDay = date(2013, 1, 1)
endDay = date.today() - timedelta(1)
state = 'MADRID'

# 2.1 Cargo el fichero de constantes
with open('/home/javier/Proyectos/ingestas/constantes/constantes.json', 'r') as f:
    config = json.load(f)
keyAemet = config['aemet']['keyAemet']
urlEstaciones = "https://opendata.aemet.es/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones/"
querystring = {"api_key": keyAemet}
headers = {
    'cache-control': "no-cache"
}
sc = py.SparkContext()
sqlContext = SQLContext(sc)

""" Obtención de datos de sensores ambientales """

''' Se define una función para trasponer las columnas y tener un registro por hora '''
def transponer(df, var1, var2, hora, varf):
    return df.select(['cod_estacion', 'cod_parametros', 'fec_anno', 'fec_mes', 'fec_dia', var1, var2]) \
        .withColumnRenamed(var1, varf).withColumn('hora', lit(hora)) \
        .withColumn('fecha', concat(df['fec_anno'], lit("-"), df['fec_mes'], lit("-"), df['fec_dia']))\
        .withColumnRenamed('flg_' + varf, 'flg_ho1') \
        .drop('fec_anno').drop('fec_mes').drop('fec_dia')


particiones = [['01', 'num_hor1', 'flg_hor1', '01:00'],
               ['02', 'num_hor2', 'flg_hor2', '02:00'],
               ['03', 'num_hor3', 'flg_hor3', '03:00'],
               ['04', 'num_hor4', 'flg_hor4', '04:00'],
               ['05', 'num_hor5', 'flg_hor5', '05:00'],
               ['06', 'num_hor6', 'flg_hor6', '06:00'],
               ['07', 'num_hor7', 'flg_hor7', '07:00'],
               ['08', 'num_hor8', 'flg_hor8', '08:00'],
               ['09', 'num_hor9', 'flg_hor9', '09:00'],
               ['10', 'num_hor10', 'flg_hor10', '10:00'],
               ['11', 'num_hor11', 'flg_hor11', '11:00'],
               ['12', 'num_hor12', 'flg_hor12', '12:00'],
               ['13', 'num_hor13', 'flg_hor13', '13:00'],
               ['14', 'num_hor14', 'flg_hor14', '14:00'],
               ['15', 'num_hor15', 'flg_hor15', '15:00'],
               ['16', 'num_hor16', 'flg_hor16', '16:00'],
               ['17', 'num_hor17', 'flg_hor17', '17:00'],
               ['18', 'num_hor18', 'flg_hor18', '18:00'],
               ['19', 'num_hor19', 'flg_hor19', '19:00'],
               ['20', 'num_hor20', 'flg_hor20', '20:00'],
               ['21', 'num_hor21', 'flg_hor21', '21:00'],
               ['22', 'num_hor22', 'flg_hor22', '22:00'],
               ['23', 'num_hor23', 'flg_hor23', '23:00'],
               ['24', 'num_hor24', 'flg_hor24', '24:00']]


ambientSensors = config['sensoresAmbientales']
sensoresContaminacion = sensoresAmbientales.Sensors(sc)
sqlContext = SQLContext(sc)
pollutionSensorsDataTotal = sensoresContaminacion.getDF()
""" Se extraen los datos de O3 """
for file in ambientSensors:
    if int(file['fecha'][-4:]) > 2012:  # Leo datos únicamente desde 2012
        pollutionSensorsData = sensoresContaminacion.getSensorsData(int(file['fecha'][-4:]), file['ruta'],
                                                                    file['datos']['nombre'], file['datos']['separador'],
                                                                    '14')
        pollutionSensorsDataTotal = pollutionSensorsDataTotal.union(pollutionSensorsData)
    print(file['fecha'])


""" Se persisten los datos de O3 """
for particion in particiones:
    po = transponer(pollutionSensorsDataTotal, particion[1], particion[2], particion[3], 'O3')
    wFHDFS = WorkingFile
    wFHDFS.writeFileHDFS('', 'hdfs://jmsi:9000/datos/sensores/O3', particion[0], 'overwrite', po)


sensoresContaminacion = sensoresAmbientales.Sensors(sc)
sqlContext = SQLContext(sc)
pollutionSensorsDataTotal = sensoresContaminacion.getDF()
""" Se extraen los datos de NO2 """
for file in ambientSensors:
    if int(file['fecha'][-4:]) > 2012:  # Leo datos únicamente desde 2012
        pollutionSensorsData = sensoresContaminacion.getSensorsData(int(file['fecha'][-4:]), file['ruta'],
                                                                    file['datos']['nombre'], file['datos']['separador'],
                                                                    '08')
        pollutionSensorsDataTotal = pollutionSensorsDataTotal.union(pollutionSensorsData)
    print(file['fecha'])


""" Se persisten los datos de NO2 """
for particion in particiones:
    po = transponer(pollutionSensorsDataTotal, particion[1], particion[2], particion[3], 'NO2')
    wFHDFS = WorkingFile
    wFHDFS.writeFileHDFS('', 'hdfs://jmsi:9000/datos/sensores/NO2', particion[0], 'overwrite', po)


sensoresContaminacion = sensoresAmbientales.Sensors(sc)
sqlContext = SQLContext(sc)
pollutionSensorsDataTotal = sensoresContaminacion.getDF()
""" Se extraen los datos de PM2.5 """
for file in ambientSensors:
    if int(file['fecha'][-4:]) > 2012:  # Leo datos únicamente desde 2012
        pollutionSensorsData = sensoresContaminacion.getSensorsData(int(file['fecha'][-4:]), file['ruta'],
                                                                    file['datos']['nombre'], file['datos']['separador'],
                                                                    '09')
        pollutionSensorsDataTotal = pollutionSensorsDataTotal.union(pollutionSensorsData)
    print(file['fecha'])


""" Se persisten los datos de PM2.5 """
for particion in particiones:
    po = transponer(pollutionSensorsDataTotal, particion[1], particion[2], particion[3], 'pm25')
    wFHDFS = WorkingFile
    wFHDFS.writeFileHDFS('', 'hdfs://jmsi:9000/datos/sensores/pm25', particion[0], 'overwrite', po)


sensoresContaminacion = sensoresAmbientales.Sensors(sc)
sqlContext = SQLContext(sc)
pollutionSensorsDataTotal = sensoresContaminacion.getDF()
""" Se extraen los datos de NOx """
for file in ambientSensors:
    if int(file['fecha'][-4:]) > 2012:  # Leo datos únicamente desde 2012
        pollutionSensorsData = sensoresContaminacion.getSensorsData(int(file['fecha'][-4:]), file['ruta'],
                                                                    file['datos']['nombre'], file['datos']['separador'],
                                                                    '12')
        pollutionSensorsDataTotal = pollutionSensorsDataTotal.union(pollutionSensorsData)
    print(file['fecha'])


""" Se persisten los datos de NOx """
for particion in particiones:
    po = transponer(pollutionSensorsDataTotal, particion[1], particion[2], particion[3], 'nox')
    wFHDFS = WorkingFile
    wFHDFS.writeFileHDFS('', 'hdfs://jmsi:9000/datos/sensores/nox', particion[0], 'overwrite', po)




schema = StructType([
            StructField('cod_estacion', StringType()),
            StructField('cod_parametros', StringType()),
            StructField('NOX', StringType()),
            StructField('flg_NOX', StringType()),
            StructField('hora', StringType()),
            StructField('fecha', StringType())
    ])
df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
for particion in particiones:
    dftemp = sqlContext.read.parquet("hdfs://jmsi:9000/datos/sensores/nox"+particion[0]+"/")
    df = df.union(dftemp)
    print("Cargados datos NOX: "+particion[0])
print("Datos Sensores NOX")
df.show()

