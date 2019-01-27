# 1. Se importan librerias
import json
from datetime import timedelta, date
import pyspark as py
from pyspark.sql.functions import date_format, round, concat, lit, when
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

import medicionesTrafico
import meteorology
import sensoresAmbientales
from file import WorkingFile

# 2. Generacion de variables
startDay = date(2013, 1, 1)
endDay = date.today() - timedelta(1)
state = 'MADRID'
rutaProyecto='/home/javier/Proyectos/modeloContaminacion/'

#   2.1 Carga de fichero de constantes
with open(rutaProyecto + '/constantes/constantes.json', 'r') as f:
    config = json.load(f)
keyAemet = config['aemet']['keyAemet']
rutaHDFS = config['rutas']['rutaHDFS']
urlEstaciones = config['aemet']['urlEstaciones']
querystring = {"api_key": keyAemet}
headers = {
    'cache-control': "no-cache"
}
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
anyos = [2013, 2014, 2015, 2016, 2017, 2018]
ambientSensors = config['sensoresAmbientales']
sc = py.SparkContext()
sqlContext = SQLContext(sc)
wFHDFS = WorkingFile

# 3. Definición funciones
#   3.1 Función transponer: Se define una función para trasponer las columnas y tener un registro por hora.
def transponer(df, var1, var2, hora, varf):
    return df.select(['cod_estacion', 'cod_parametros', 'fec_anno', 'fec_mes', 'fec_dia', var1, var2]) \
        .withColumnRenamed(var1, varf).withColumn('hora', lit(hora)) \
        .withColumn('fecha', concat(df['fec_anno'], lit("-"), df['fec_mes'], lit("-"), df['fec_dia'])) \
        .withColumnRenamed('flg_' + varf, 'flg_ho1') \
        .drop('fec_anno').drop('fec_mes').drop('fec_dia')

#   3.2 Función acumDatosSensores: Unifica los ficheros de datos de sensores
def acumDatosSensores(contaminante, flag_contaminante):
    schema = StructType([
        StructField('cod_estacion', StringType()),
        StructField('cod_parametros', StringType()),
        StructField(contaminante, StringType()),
        StructField(flag_contaminante, StringType()),
        StructField('hora', StringType()),
        StructField('fecha', StringType())
    ])
    df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
    for particion in particiones:
        dftemp = sqlContext.read.parquet(rutaHDFS + "/datos/sensores/" + contaminante + particion[0] + "/")
        df = df.union(dftemp)
        print("Cargados datos : " + contaminante + particion[0])
    print("Datos Sensores " + contaminante)
    df.show()
    return df

# 4. Obtención de mediciones de tráfico
mT = medicionesTrafico.MedicionesTrafico(sc)
trafficData = config['datosTrafico']
for file in trafficData:
    mT.readFile(file['ruta'], file['datos']['nombre'], file['datos']['cabecera'], file['datos']['separador'],
                rutaHDFS, file['fecha'])
df = sqlContext.read.format('com.databricks.spark.csv') \
    .options(header='true',
             inferschema='true',
             delimiter=';') \
    .load(rutaProyecto + '/datos/pmed_ubicacion_10_2018.csv')
df.show()
wFHDFS.writeFileHDFS('', rutaHDFS + '/metadata/vehiculos/', 'ubicacionSensores', 'overwrite', df)

# 5. Obtención de datos meteorológicos
for anyo in anyos:
    if anyo == 2018:
        mes_fin = 10
    else:
        mes_fin = 12
    diaInicio = date(anyo, 12, 31)
    diaFin = date(anyo, 1, 1)  # date.today()-timedelta(1)
    meteorologia = meteorology.Meteorology(keyAemet, headers, querystring, diaInicio, diaFin)

    # Obtengo las estaciones para el estudio
    estaciones = meteorologia.getStations(urlEstaciones, state)
    # estaciones = [{'latitud': '402706N', 'provincia': 'MADRID', 'altitud': '664', 'indicativo': '3194U',
    # 'nombre': 'MADRID, CIUDAD UNIVERSITARIA', 'indsinop': '08220', 'longitud': '034327W'}, {'latitud': '402232N',
    # 'provincia': 'MADRID', 'altitud': '690', 'indicativo': '3196', 'nombre': 'MADRID, CUATRO VIENTOS', 'indsinop':
    # '08223', 'longitud': '034710W'}, {'latitud': '402443N', 'provincia': 'MADRID', 'altitud': '667', 'indicativo':
    # '3195', 'nombre': 'MADRID, RETIRO', 'indsinop': '08222', 'longitud': '034041W'}]
    for estacion in estaciones:
        datosSensoresMeteorologicos = meteorologia.getSensorData([estacion])
        file.WorkingFile.printFile('', '/home/javier', 'datosMeterologicos.json', 'a+', datosSensoresMeteorologicos)
df = sqlContext.read.json(rutaProyecto + '/datos/datosMeterologicos.json')
df.show()
wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/meteorologia/', 'datosMeteorologicos', 'overwrite', df)

# 6. Obtención de datos de sensores ambientales
sensoresContaminacion = sensoresAmbientales.Sensors(sc)
pollutionSensorsDataTotal = sensoresContaminacion.getDF()
#   6.1 Se extraen los datos de O3
for file in ambientSensors:
    if int(file['fecha'][-4:]) > 2012:  # Leo datos únicamente desde 2012
        pollutionSensorsData = sensoresContaminacion.getSensorsData(int(file['fecha'][-4:]), file['ruta'],
                                                                    file['datos']['nombre'], file['datos']['separador'],
                                                                    '14')
        pollutionSensorsDataTotal = pollutionSensorsDataTotal.union(pollutionSensorsData)
    print(file['fecha'])

#   6.2 Se persisten los datos de O3
for particion in particiones:
    po = transponer(pollutionSensorsDataTotal, particion[1], particion[2], particion[3], 'O3')
    wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/sensores/O3', particion[0], 'overwrite', po)

pollutionSensorsDataTotal = sensoresContaminacion.getDF()
#   6.3 Se extraen los datos de NO2
for file in ambientSensors:
    if int(file['fecha'][-4:]) > 2012:  # Leo datos únicamente desde 2012
        pollutionSensorsData = sensoresContaminacion.getSensorsData(int(file['fecha'][-4:]), file['ruta'],
                                                                    file['datos']['nombre'], file['datos']['separador'],
                                                                    '08')
        pollutionSensorsDataTotal = pollutionSensorsDataTotal.union(pollutionSensorsData)
    print(file['fecha'])

#   6.4 Se persisten los datos de NO2 """
for particion in particiones:
    po = transponer(pollutionSensorsDataTotal, particion[1], particion[2], particion[3], 'NO2')
    wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/sensores/NO2', particion[0], 'overwrite', po)

pollutionSensorsDataTotal = sensoresContaminacion.getDF()

#   6.5 Se extraen los datos de PM2.5 """
for file in ambientSensors:
    if int(file['fecha'][-4:]) > 2012:  # Leo datos únicamente desde 2012
        pollutionSensorsData = sensoresContaminacion.getSensorsData(int(file['fecha'][-4:]), file['ruta'],
                                                                    file['datos']['nombre'], file['datos']['separador'],
                                                                    '09')
        pollutionSensorsDataTotal = pollutionSensorsDataTotal.union(pollutionSensorsData)
    print(file['fecha'])

#   6.6 Se persisten los datos de PM2.5 """
for particion in particiones:
    po = transponer(pollutionSensorsDataTotal, particion[1], particion[2], particion[3], 'pm25')
    wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/sensores/pm25', particion[0], 'overwrite', po)

pollutionSensorsDataTotal = sensoresContaminacion.getDF()

#   6.7 Se extraen los datos de NOx """
for file in ambientSensors:
    if int(file['fecha'][-4:]) > 2012:  # Leo datos únicamente desde 2012
        pollutionSensorsData = sensoresContaminacion.getSensorsData(int(file['fecha'][-4:]), file['ruta'],
                                                                    file['datos']['nombre'], file['datos']['separador'],
                                                                    '12')
        pollutionSensorsDataTotal = pollutionSensorsDataTotal.union(pollutionSensorsData)
    print(file['fecha'])

#   6.8 Se persisten los datos de NOx """
for particion in particiones:
    po = transponer(pollutionSensorsDataTotal, particion[1], particion[2], particion[3], 'nox')
    wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/sensores/nox', particion[0], 'overwrite', po)

# 7. Lectura fichero metadata distrito sensores ambientales
dfMSA = sqlContext.read.format('com.databricks.spark.csv') \
    .options(header='true',
             inferschema='true',
             delimiter=';') \
    .load(rutaProyecto + '/datos/ubicacionDistritoMeteo.csv')
wFHDFS.writeFileHDFS('', rutaHDFS + '/metadata/meteo/', 'ubicacionDistritoMeteo', 'overwrite', dfMSA)

# 8. Se acumulan los datos de tráfico
trafficData = config['datosTrafico']
schema = StructType([
    StructField('identif', StringType()),
    StructField('intensidad', StringType()),
    StructField('fecha', StringType()),
    StructField('hora', StringType())
])
df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
for element in trafficData:
    dftemp = sqlContext.read.parquet(rutaHDFS + "/datos/vehiculos/" + element['fecha'] + "/")
    df = df.union(dftemp)
    print("Cargados datos trafico: " + element['fecha'])
print("Datos trafico")
df.show()
wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/trafico/', 'totales', 'overwrite', df)

# 9. Se acumulan los datos de sensores ambientales
dfNO2 = acumDatosSensores('NO2', 'flg_NO2')
wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/sensores/totales/', 'NO2', 'overwrite', dfNO2)
dfO3 = acumDatosSensores('O3', 'flg_O3')
wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/sensores/totales/', 'O3', 'overwrite', dfO3)
dfpm25 = acumDatosSensores('pm25', 'flg_pm25')
wFHDFS.writeFileHDFS('', rutaHDFS + '/sensores/totales/', 'PM25', 'overwrite', dfpm25)
dfnox = acumDatosSensores('nox', 'flg_nox')
wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/sensores/totales/', 'NOX', 'overwrite', dfnox)
# dfNO2.count()
# dfO3.count()
# dfpm25.count()
# dfnox.count()

# 10. Lectura datos de tráfico agregados
df = sqlContext.read.parquet(rutaHDFS + "/datos/trafico/totales/").where('intensidad != "NaN"')
print("Datos de tráfico")
df.show()

# 11. Lectura ubicación sensores de trafico
dfMeta = sqlContext.read.parquet(rutaHDFS + "/metadata/vehiculos/ubicacionSensores/")
print("Ubicacion sensores trafico")
dfMeta.show()

# 12. Unión datos sensores tráfico con ubicación estaciones tráfico
df.registerTempTable('df')
dfMeta.registerTempTable('dfMeta')
dfDataTrafico = sqlContext.sql('select df.identif, df.fecha, df.intensidad, df.hora, dfMeta.distrito from df left join '
                               '(select distinct distrito, cod_cent from dfMeta) as dfMeta on '
                               'df.identif=dfMeta.cod_cent  where distrito is not null')
dfDataTrafico2 = sqlContext.sql('select df.* from df left join (select distinct distrito, cod_cent from dfMeta) as '
                                'dfMeta on df.identif=dfMeta.cod_cent where distrito is null')
dfDataTrafico2.registerTempTable('dfDataTrafico2')
dfDataTrafico3 = sqlContext.sql('select dfDataTrafico2.identif, dfDataTrafico2.fecha, dfDataTrafico2.intensidad, '
                                'dfDataTrafico2.hora, dfMeta.distrito from dfDataTrafico2 left join (select distinct '
                                'distrito, id from dfMeta) as dfMeta on dfDataTrafico2.identif=dfMeta.id where '
                                'distrito is not null')
dfDataTrafico = dfDataTrafico.union(dfDataTrafico3)

# Se agregan los datos de tráfico por distrito (de momento no agrego)
# dfDataTrafico = dfDataTrafico.groupBy('fecha', 'distrito').agg({'intensidad': 'avg'}) \
#     .withColumnRenamed('avg(intensidad)', 'intensidad')
# dfDataTrafico = dfDataTrafico.withColumn('intensidad', dfDataTrafico['intensidad'].cast(IntegerType()))

print("Datos agregados trafico + ubicación sensores trafico")
dfDataTrafico.show()

# 13. Lectura ubicación sensores de medioambiente
dfMSA = sqlContext.read.parquet("hdfs://jmsi:9000/metadata/sensores/distritosSensoresAmbientales/")
print("Ubicacion sensores ambientales")
dfMSA.show()

# 14. Lectura datos sensores ambientales
dfSDataNO2 = sqlContext.read.parquet("hdfs://jmsi:9000/datos/sensores/totales/NO2/")
dfSDataO3 = sqlContext.read.parquet("hdfs://jmsi:9000/datos/sensores/totales/O3/")
dfSDataPM25 = sqlContext.read.parquet("hdfs://jmsi:9000/datos/sensores/totales/PM25/")
dfSDataNOx = sqlContext.read.parquet("hdfs://jmsi:9000/datos/sensores/totales/NOX/")

# 15. Unión de los datos en una sola tabla
dfSData = dfSDataNO2.join(dfSDataO3, (dfSDataNO2['fecha'] == dfSDataO3['fecha']) &
                          (dfSDataNO2['hora'] == dfSDataO3['hora']) & (
                                      dfSDataNO2['cod_estacion'] == dfSDataO3['cod_estacion']), how='full') \
    .select(
    when(dfSDataNO2['cod_estacion'].isNull(), dfSDataO3['cod_estacion']).otherwise(dfSDataNO2['cod_estacion']).alias(
        'cod_estacion'),
    when(dfSDataNO2['fecha'].isNull(), dfSDataO3['fecha']).otherwise(dfSDataNO2['fecha']).alias('fecha'),
    when(dfSDataNO2['hora'].isNull(), dfSDataO3['hora']).otherwise(dfSDataNO2['hora']).alias('hora'),
    dfSDataNO2['NO2'],
    dfSDataNO2['flg_NO2'],
    dfSDataO3['O3'],
    dfSDataO3['flg_O3'])

dfSData = dfSData.join(dfSDataPM25, (dfSData['fecha'] == dfSDataPM25['fecha']) &
                       (dfSData['hora'] == dfSDataPM25['hora']) & (
                                   dfSData['cod_estacion'] == dfSDataPM25['cod_estacion']), how='full') \
    .select(
    when(dfSData['cod_estacion'].isNull(), dfSDataPM25['cod_estacion']).otherwise(dfSData['cod_estacion']).alias(
        'cod_estacion'),
    when(dfSData['fecha'].isNull(), dfSDataPM25['fecha']).otherwise(dfSData['fecha']).alias('fecha'),
    when(dfSData['hora'].isNull(), dfSDataPM25['hora']).otherwise(dfSData['hora']).alias('hora'),
    dfSData['NO2'],
    dfSData['O3'],
    dfSData['flg_O3'],
    dfSData['flg_NO2'],
    dfSDataPM25['pm25'],
    dfSDataPM25['flg_pm25'])

dfSData = dfSData.join(dfSDataNOx, (dfSData['fecha'] == dfSDataNOx['fecha']) &
                       (dfSData['hora'] == dfSDataNOx['hora']) & (
                                   dfSData['cod_estacion'] == dfSDataNOx['cod_estacion']), how='full') \
    .select(when(dfSData['cod_estacion'].isNull(), dfSDataNOx['cod_estacion']).otherwise(dfSData['cod_estacion']).alias(
    'cod_estacion'),
            when(dfSData['fecha'].isNull(), dfSDataNOx['fecha']).otherwise(dfSData['fecha']).alias('fecha'),
            when(dfSData['hora'].isNull(), dfSDataNOx['hora']).otherwise(dfSData['hora']).alias('hora'),
            dfSData['NO2'],
            dfSData['flg_NO2'],
            dfSData['O3'],
            dfSData['flg_O3'],
            dfSData['pm25'],
            dfSData['flg_pm25'],
            dfSDataNOx['nox'],
            dfSDataNOx['flg_nox'])

# 16. Corrección para distinto formato en el año
dfSData.registerTempTable('dfSData')
dfSData = sqlContext.sql('select cod_estacion, case when length(fecha) == 8 then concat("20", fecha) else fecha end '
                         'as fecha, hora, no2, o3, pm25, nox from dfSData')
print("Datos agregados sensores ambientales")
dfSData.orderBy(['fecha', 'hora', 'cod_estacion']).show()

dfMSA.registerTempTable('dfMSA')
dfSData.registerTempTable('dfSData')
dfF = sqlContext.sql('SELECT dfSData.cod_estacion, dfSData.fecha, dfSData.hora,dfSData.cod_estacion, dfSData.no2, '
                     'dfSData.o3, dfSData.pm25, dfSData.nox, '
                     'dfMSA.distrito from dfSData left join dfMSA on dfSData.cod_estacion=substr('
                     'dfMSA.identif, 6, 3)')
print("Datos agregados sensores ambientales + ubicación sensores ambientales")
dfF.show()

dfF.registerTempTable('dfF')
dfDataTrafico.registerTempTable('dfDataTrafico')
print(dfDataTrafico.count())
dfDataTA = sqlContext.sql('select dfF.cod_estacion, dfF.no2, dfF.o3, dfF.pm25, dfF.nox, dfDataTrafico.* from '
                          'dfDataTrafico '
                          'inner join dfF on dfDataTrafico.fecha = dfF.fecha and dfDataTrafico.hora = dfF.hora '
                          'and dfDataTrafico.distrito = dfF.distrito')
print("Datos agregados trafico + ubicación sensores trafico + "
      "Datos agregados ambientales + Ubicacion sensores ambientales")
dfDataTA.orderBy(['fecha', 'hora', 'distrito']).show()

dfDataTA.registerTempTable('dfDataTA')
dfDataTA2 = sqlContext.sql('select max(no2) as max_no2, avg(no2) as avg_no2, avg(intensidad) as intensidad, fecha, '
                           'max(o3) as max_o3, avg(o3) as avg_o3, max(pm25) as max_pm25, avg(pm25) as avg_pm25, '
                           'max(nox) as max_nox, avg(nox) as avg_nox, hora, distrito from dfDataTA group by fecha, '
                           'hora, distrito order by fecha, hora, distrito ')
dfDataTA2.where('fecha = "2013-01-01" and distrito = 1 and hora = "01:00"').show()

# 17. Conversión de formatos
dfDataTA2 = dfDataTA2.withColumn('avg_no2', round(dfDataTA2['avg_no2'], 0).cast(IntegerType())) \
    .withColumn('intensidad', round(dfDataTA2['intensidad'], 0).cast(IntegerType())) \
    .withColumn('avg_o3', round(dfDataTA2['avg_o3'], 2).cast(DoubleType())) \
    .withColumn('avg_pm25', round(dfDataTA2['avg_pm25'], 2).cast(DoubleType())) \
    .withColumn('avg_nox', round(dfDataTA2['avg_nox'], 2).cast(DoubleType()))

wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/temp/', 'traficoYAmbiente/', 'overwrite', dfDataTA2)
dfDataTA = sqlContext.read.parquet(rutaHDFS + "/datos/temp/traficoYAmbiente/")

# 18. Lectura datos meteorología
dfMData = sqlContext.read.parquet(rutaHDFS + "/datos/meteorologia/datosMeteorologicos/")
# print("Datos meteorologia")
# dfMData.show()

# 19. Lectura metadatos ubicacion sensores meteorología
dfMUbi = sqlContext.read.parquet(rutaHDFS + "/metadata/meteo/ubicacionDistritoMeteo")
# print("Metadatos ubicacion estaciones meteorologia")
# dfMUbi.show()

# 20. Unión datos estaciones meteorológicas con ubicación estaciones
dfMData.registerTempTable('dfMData')
dfMUbi.registerTempTable('dfMUbi')
dfMDatUbi = sqlContext.sql('select dfMData.fecha, dfMData.tmax, dfMData.tmed, dfMData.tmin, dfMData.velmedia, '
                           'dfMData.prec, dfMData.altitud, dfMUbi.distrito from dfMData left join dfMUbi on '
                           'dfMData.indicativo=dfMUbi.identificador')
# print("Datos meteorologicos + ubicación estaciones meteorologicas")
# dfMDatUbi.orderBy(['fecha', 'distrito']).show()

dfMDatUbi.registerTempTable('dfMDatUbi')
dfDataTA.registerTempTable('dfDataTA')
dfFin = sqlContext.sql('select dfDataTA.fecha, dfDataTA.distrito, dfDataTA.intensidad, dfDataTA.hora, '
                       'dfDataTA.avg_no2, dfDataTA.max_no2, dfMDatUbi.tmax, dfMDatUbi.tmed, dfMDatUbi.tmin, '
                       'dfDataTA.avg_o3, dfDataTA.max_o3, dfDataTA.avg_pm25, dfDataTA.max_pm25, '
                       'dfDataTA.avg_nox, dfDataTA.max_nox,'
                       'dfMDatUbi.velmedia, dfMDatUbi.altitud, '
                       'dfMDatUbi.prec from dfDataTA left join dfMDatUbi on dfDataTA.fecha = dfMDatUbi.fecha and '
                       'dfDataTA.distrito = dfMDatUbi.distrito')
# print("Datos total")
# dfFin.show()

dfFin = dfFin.select('fecha', 'hora', date_format('fecha', 'u').alias('dia_semana'), 'distrito', 'intensidad',
                     'max_no2', 'avg_no2', 'max_o3', 'avg_o3', 'max_pm25', 'avg_pm25', 'max_nox', 'avg_nox',
                     'tmax', 'tmed', 'tmin', 'velmedia', 'prec', 'altitud')
dfFin.show()

# 21. Persistencia de los datos en HDFS para generar modelos
wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/modelos/', 'datosTratados', 'overwrite', dfFin)
