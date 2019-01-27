from pyspark.sql import SQLContext
from tfm.file import WorkingFile
from pyspark.sql.functions import lit, concat


class MedicionesTrafico:

    def __init__(self, sc):
        self.sc = sc

    def readFile(self, path, fileName, header, delimiter, fecha, rutaHDFS, inferschema='true'):
        sqlContext = SQLContext(self.sc)
        # Read the file
        df = sqlContext.read.format('com.databricks.spark.csv') \
            .options(header=header,
                     inferschema=inferschema,
                     delimiter=delimiter) \
            .load(path + fileName)

        dfAggregateData = df.withColumn('fecha_aux', df['fecha'].substr(1, 13)) \
            .filter(df['error'] == 'N') \
            .groupBy(df.columns[0], 'fecha_aux') \
            .agg({'intensidad': 'sum'}) \
            .withColumnRenamed('sum(intensidad)', 'intensidad')

        dfAggregateData = dfAggregateData.withColumn('fecha', dfAggregateData['fecha_aux'].substr(1, 10)) \
            .withColumn('hora', concat(dfAggregateData["fecha_aux"].substr(12, 2), lit(":00"))) \
            .drop('fecha_aux')

        dfAggregateData.show()

        wFHDFS = WorkingFile
        wFHDFS.writeFileHDFS('', rutaHDFS + '/datos/vehiculos/', fecha, 'overwrite', dfAggregateData)
