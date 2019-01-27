###################################################################################
## ANALISIS DE LOS DATOS
###################################################################################

# 1. Importado de librerías
import pyspark as py
from pyspark.sql.functions import regexp_replace, when
from pyspark.sql import SQLContext
from pyspark.sql.types import DoubleType, IntegerType
import matplotlib.pyplot as plt
from sklearn import linear_model
import math

# 2. Inicializar spark context
sc = py.SparkContext()
sqlContext = SQLContext(sc)

# 3. Lectura de los datos
rutaHDFS = 'hdfs://localhost:9000/'
df = sqlContext.read.parquet(rutaHDFS + "/datos/modelos/datosTratados/")

# 4. Arreglo de variables
df = df \
    .withColumn('avg_no2', df['avg_no2'].cast(IntegerType())).where('avg_no2 is not null') \
    .withColumn('max_no2', df['max_no2'].cast(IntegerType())).where('max_no2 is not null') \
    .withColumn('tmed', regexp_replace('tmed', ',', '.').cast(DoubleType())).where('tmed is not null') \
    .withColumn('prec', regexp_replace('prec', ',', '.').cast(DoubleType())).where('prec is not null') \
    .withColumn('dia_semana', df['dia_semana'].cast(IntegerType())) \
    .withColumn('tmax', regexp_replace('tmax', ',', '.').cast(DoubleType())).where('tmax is not null') \
    .withColumn('tmin', regexp_replace('tmin', ',', '.').cast(DoubleType())).where('tmin is not null') \
    .withColumn('velmedia', regexp_replace('velmedia', ',', '.').cast(DoubleType())).where('velmedia is not null')


# 5. Filtro de los registros not null para obtener rectas de regresión para evitar demasiados valores null en
# los datos del modelo final manteniendo porcentaje de correlación.
def imprimirDatosRecta(regr):
    # Imprimir Recta
    m = regr.coef_[0]
    b = regr.intercept_
    print(' y = {0} * x + {1}'.format(m, b))


def calcularRecta(df, var1, var2):
    dfp = df.select(var1, var2).toPandas()
    x = dfp[var2].as_matrix()
    y = dfp[var1].as_matrix()
    x = x.reshape(-1, 1)
    y = y.reshape(-1, 1)

    regr = linear_model.LinearRegression()
    regr.fit(x, y)
    return regr


dfcalc = df.withColumn('avg_o3', df['avg_o3'].cast(IntegerType())).where('avg_o3 is not null')
regr = calcularRecta(dfcalc, 'avg_no2', 'avg_o3')
imprimirDatosRecta(regr)
dfcalc = df.withColumn('max_o3', df['max_o3'].cast(IntegerType())).where('max_o3 is not null')
regr = calcularRecta(dfcalc, 'max_no2', 'max_o3')
imprimirDatosRecta(regr)
dfcalc = df.withColumn('avg_pm25', df['avg_pm25'].cast(IntegerType())).where('avg_pm25 is not null')
regr = calcularRecta(dfcalc, 'avg_no2', 'avg_pm25')
imprimirDatosRecta(regr)
dfcalc = df.withColumn('max_pm25', df['max_pm25'].cast(IntegerType())).where('max_pm25 is not null')
regr = calcularRecta(dfcalc, 'max_no2', 'max_pm25')
imprimirDatosRecta(regr)
dfcalc = df.withColumn('avg_nox', df['avg_nox'].cast(IntegerType())).where('avg_nox is not null')
regr = calcularRecta(dfcalc, 'avg_no2', 'avg_nox')
imprimirDatosRecta(regr)
dfcalc = df.withColumn('max_nox', df['max_nox'].cast(IntegerType())).where('max_nox is not null')
regr = calcularRecta(dfcalc, 'max_no2', 'max_nox')
imprimirDatosRecta(regr)

df = df.withColumn('avg_o3', when(df['avg_o3'].isNull(), ((df['avg_no2'] - 61.27692401) / -0.51746665)).otherwise(
    df['avg_o3']).cast(IntegerType())).where('avg_o3 >= 0') \
    .withColumn('max_o3',
                when(df['max_o3'].isNull(), ((df['max_no2'] - 65.28363081) / -0.5163941)).otherwise(df['max_o3']).cast(
                    IntegerType())).where('max_o3 >= 0') \
    .withColumn('avg_pm25', when(df['avg_pm25'].isNull(),
                                 when(df['avg_no2'] <= 100, ((df['avg_no2'] - 16.62141325) / 1.93788304)).otherwise(
                                     0)).otherwise(df['avg_pm25']).cast(IntegerType())).where('avg_pm25 >= 0') \
    .withColumn('max_pm25', when(df['max_pm25'].isNull(),
                                 when(df['max_no2'] <= 125, ((df['max_no2'] - 18.19584719) / 2.08983974)).otherwise(
                                     0)).otherwise(df['max_pm25']).cast(IntegerType())).where(
    'max_pm25 >= 0 and max_pm25 <= 110') \
    .withColumn('avg_nox',
                when(df['avg_nox'].isNull(), ((df['max_no2'] - 19.50490427) / 0.2777936)).otherwise(df['avg_nox']).cast(
                    IntegerType())).where('avg_nox >= 0') \
    .withColumn('max_nox', when(df['max_nox'].isNull(), ((df['max_no2'] - 21.26937607) / 0.26389714)).otherwise(
    df['max_nox']).cast(IntegerType())).where('max_nox >= 0')

df.show()

# 6. Categorizar variables
df.registerTempTable('df')
dfintensidad = sqlContext.sql('select df.*, case \
                                when df.intensidad < 2000 then 0 \
                                WHEN df.intensidad >= 2000 and df.intensidad < 4000 then 1 \
                                WHEN df.intensidad >= 4000 and df.intensidad < 6000 then 2 \
                                WHEN df.intensidad >= 6000 and df.intensidad < 8000 then 3 \
                                WHEN df.intensidad >= 8000 and df.intensidad < 10000 then 4 \
                                WHEN df.intensidad >= 10000 then 5 \
                                end as intensidad_cat \
                               from df')
dfintensidad.registerTempTable('dfintensidad')
dfintensidad = sqlContext.sql('select *, \
                         CASE \
                            when df.max_no2 <  25                       then 0 \
                            when df.max_no2 >= 25  and df.max_no2 < 50  then 1 \
                            when df.max_no2 >= 50  and df.max_no2 < 75  then 2 \
                            when df.max_no2 >= 75  and df.max_no2 < 100 then 3 \
                            when df.max_no2 >= 100 and df.max_no2 < 125 then 4 \
                            when df.max_no2 >= 125 and df.max_no2 < 150 then 5 \
                            when df.max_no2 >= 150 and df.max_no2 < 175 then 6 \
                            when df.max_no2 >= 175 and df.max_no2 < 200 then 7 \
                            when df.max_no2 >= 200 and df.max_no2 < 225 then 8 \
                            when df.max_no2 >= 225 and df.max_no2 < 250 then 9 \
                            when df.max_no2 >= 250 and df.max_no2 < 275 then 10 \
                            when df.max_no2 >= 275 and df.max_no2 < 300 then 11 \
                            when df.max_no2 >= 300 and df.max_no2 < 325 then 12 \
                            when df.max_no2 >= 325 and df.max_no2 < 350 then 13 \
                            when df.max_no2 >= 350 and df.max_no2 < 375 then 14 \
                            when df.max_no2 >= 375 and df.max_no2 < 400 then 15 \
                            when df.max_no2 >= 400  then 16 \
                            end as max_no2_cat \
                         from dfintensidad as df')
dfintensidad.show()


###################################################################################
## Regresión lineal
###################################################################################

def pintarGrafico(label_y, label_x, x, y):
    plt.ylabel(label_y)
    plt.xlabel(label_x)
    plt.scatter(x, y, color='black')
    plt.plot(x, regr.predict(x), color='blue', linewidth=3)
    plt.show()


###################################################################################
## Intensidad x Maximo NO2
###################################################################################
dfp = df.select('max_no2', 'intensidad').where('tmed is not null').toPandas()
x = dfp['max_no2'].as_matrix()
y = dfp['intensidad'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Intensidad trafico'
label_x = 'Máximo NO2'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## max_no2 x Temperatura media
###################################################################################
dfp = df.select('max_no2', 'tmed').where('tmed is not null').toPandas()
x = dfp['max_no2'].as_matrix()
y = dfp['tmed'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Temperatura media'
label_x = 'Máximo NO2'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## max_no2 x Precipitaciones media
###################################################################################
dfp = df.select('max_no2', 'prec').where('prec is not null').toPandas()
x = dfp['prec'].as_matrix()
y = dfp['max_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Máximo NO2'
label_x = 'Precipitaciones'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## max_no2 x vel media
###################################################################################
dfp = df.select('max_no2', 'velmedia').where('velmedia is not null').toPandas()
x = dfp['velmedia'].as_matrix()
y = dfp['max_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Máximo NO2'
label_x = 'Velocidad media aire'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## max_no2 x dia semana
###################################################################################
dfp = df.select('max_no2', 'dia_semana').toPandas()
x = dfp['dia_semana'].as_matrix()
y = dfp['max_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Máximo NO2'
label_x = 'Día de la semana'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## Intensidad x avg_no2
###################################################################################
dfp = df.select('avg_no2', 'intensidad').toPandas()
x = dfp['avg_no2'].as_matrix()
y = dfp['intensidad'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Intensidad trafico'
label_x = 'Media NO2'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## avg_no2 x Temperatura media
###################################################################################
dfp = df.select('avg_no2', 'tmed').where('tmed is not null').toPandas()
x = dfp['avg_no2'].as_matrix()
y = dfp['tmed'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Temperatura media'
label_x = 'Media NO2'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## avg_no2 x percipitaciones
###################################################################################
dfp = df.select('avg_no2', 'prec').where('prec is not null').toPandas()
x = dfp['prec'].as_matrix()
y = dfp['avg_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Media NO2'
label_x = 'Precipitaciones'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## avg_no2 x vel media
###################################################################################
dfp = df.select('avg_no2', 'velmedia').where('velmedia is not null').toPandas()
x = dfp['velmedia'].as_matrix()
y = dfp['avg_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Media NO2'
label_x = 'Velocidad media aire'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## avg_no2 x dia semana
###################################################################################
dfp = df.select('avg_no2', 'dia_semana').toPandas()
x = dfp['dia_semana'].as_matrix()
y = dfp['avg_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Media NO2'
label_x = 'Día de la semana'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## avg_no2 x avg_o3
###################################################################################
dfp = df.select('avg_no2', 'avg_o3').toPandas()
x = dfp['avg_o3'].as_matrix()
y = dfp['avg_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Media NO2'
label_x = 'Media O3'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## max_no2 x max_o3
###################################################################################
dfp = df.select('max_no2', 'max_o3').toPandas()
x = dfp['max_o3'].as_matrix()
y = dfp['max_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Máximo NO2'
label_x = 'Máximo O3'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## avg_no2 x avg_pm25
###################################################################################
dfp = df.select('avg_no2', 'avg_pm25').toPandas()
x = dfp['avg_pm25'].as_matrix()
y = dfp['avg_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Media NO2'
label_x = 'Media PM2,5'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## max_no2 x max_pm25
###################################################################################
dfp = df.select('max_no2', 'max_pm25').toPandas()
x = dfp['max_pm25'].as_matrix()
y = dfp['max_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Máximo NO2'
label_x = 'Máximo PM2,5'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## avg_no2 x avg_nox
###################################################################################
dfp = df.select('avg_no2', 'avg_nox').toPandas()
x = dfp['avg_nox'].as_matrix()
y = dfp['avg_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Media NO2'
label_x = 'Media NOx'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## max_no2 x max_nox
###################################################################################
dfp = df.select('max_no2', 'max_nox').toPandas()
x = dfp['max_nox'].as_matrix()
y = dfp['max_no2'].as_matrix()
x = x.reshape(-1, 1)
y = y.reshape(-1, 1)

regr = linear_model.LinearRegression()
regr.fit(x, y)

print('Coeff of determination:', regr.score(x, y))
print('correlation is:', math.sqrt(regr.score(x, y)))

label_y = 'Máximo NO2'
label_x = 'Máximo NOx'
pintarGrafico(label_y, label_x, x, y)

###################################################################################
## FIN Regresión lineal
###################################################################################

# Exportado csv para generar modelos en Weka
dfexportar = dfintensidad.select('hora', 'dia_semana', 'distrito', 'intensidad', 'max_o3', 'avg_o3', 'max_pm25',
                                 'avg_pm25', 'max_nox', 'avg_nox', 'tmax', 'tmed', 'tmin', 'velmedia', 'prec',
                                 'altitud', 'intensidad_cat', 'max_no2_cat')

dfexportar.coalesce(1).write.option("header", "true").option("NON_NUMERIC", True) \
    .csv('/home/javier/Proyectos/ingestas/datos/datosmodelo.csv')
