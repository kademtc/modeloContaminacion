from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import greatest, lit, when, col


class Sensors:

    def __init__(self, sc):
        self.sc = sc

        self.schema = StructType([
            StructField('cod_provincia', StringType()),
            StructField('cod_municipio', StringType()),
            StructField('cod_estacion', StringType()),
            StructField('cod_parametros', StringType()),
            StructField('cod_tecana', StringType()),
            StructField('cod_peranal', StringType()),
            StructField('fec_anno', StringType()),
            StructField('fec_mes', StringType()),
            StructField('fec_dia', StringType()),
            StructField('num_hor1', StringType()),
            StructField('flg_hor1', StringType()),
            StructField('num_hor2', StringType()),
            StructField('flg_hor2', StringType()),
            StructField('num_hor3', StringType()),
            StructField('flg_hor3', StringType()),
            StructField('num_hor4', StringType()),
            StructField('flg_hor4', StringType()),
            StructField('num_hor5', StringType()),
            StructField('flg_hor5', StringType()),
            StructField('num_hor6', StringType()),
            StructField('flg_hor6', StringType()),
            StructField('num_hor7', StringType()),
            StructField('flg_hor7', StringType()),
            StructField('num_hor8', StringType()),
            StructField('flg_hor8', StringType()),
            StructField('num_hor9', StringType()),
            StructField('flg_hor9', StringType()),
            StructField('num_hor10', StringType()),
            StructField('flg_hor10', StringType()),
            StructField('num_hor11', StringType()),
            StructField('flg_hor11', StringType()),
            StructField('num_hor12', StringType()),
            StructField('flg_hor12', StringType()),
            StructField('num_hor13', StringType()),
            StructField('flg_hor13', StringType()),
            StructField('num_hor14', StringType()),
            StructField('flg_hor14', StringType()),
            StructField('num_hor15', StringType()),
            StructField('flg_hor15', StringType()),
            StructField('num_hor16', StringType()),
            StructField('flg_hor16', StringType()),
            StructField('num_hor17', StringType()),
            StructField('flg_hor17', StringType()),
            StructField('num_hor18', StringType()),
            StructField('flg_hor18', StringType()),
            StructField('num_hor19', StringType()),
            StructField('flg_hor19', StringType()),
            StructField('num_hor20', StringType()),
            StructField('flg_hor20', StringType()),
            StructField('num_hor21', StringType()),
            StructField('flg_hor21', StringType()),
            StructField('num_hor22', StringType()),
            StructField('flg_hor22', StringType()),
            StructField('num_hor23', StringType()),
            StructField('flg_hor23', StringType()),
            StructField('num_hor24', StringType()),
            StructField('flg_hor24', StringType()),
            StructField('max_cont', StringType()),
            StructField('avg_cont', StringType())
        ])


    def getDF(self):
        sqlContext = SQLContext(self.sc)
        return sqlContext.createDataFrame(self.sc.emptyRDD(), self.schema)

    def getSensorsData(self, anno, path, file, separator, contaminante):
        sqlContext = SQLContext(self.sc)
        if separator == ',':
            df = sqlContext.read.csv(path + file, sep=',', schema=self.schema)
        else:
            df = sqlContext.read.text(path + file)
            df = df.select(
                df.value.substr(1, 2).alias('cod_provincia'),
                df.value.substr(3, 3).alias('cod_municipio'),
                df.value.substr(6, 3).alias('cod_estacion'),
                df.value.substr(9, 2).alias('cod_parametros'),
                df.value.substr(11, 2).alias('cod_tecana'),
                df.value.substr(13, 2).alias('cod_peranal'),
                df.value.substr(15, 2).alias('fec_anno'),
                df.value.substr(17, 2).alias('fec_mes'),
                df.value.substr(19, 2).alias('fec_dia'),
                df.value.substr(21, 5).alias('num_hor1'),
                df.value.substr(26, 1).alias('flg_hor1'),
                df.value.substr(27, 5).alias('num_hor2'),
                df.value.substr(32, 1).alias('flg_hor2'),
                df.value.substr(33, 5).alias('num_hor3'),
                df.value.substr(38, 1).alias('flg_hor3'),
                df.value.substr(39, 5).alias('num_hor4'),
                df.value.substr(44, 1).alias('flg_hor4'),
                df.value.substr(45, 5).alias('num_hor5'),
                df.value.substr(50, 1).alias('flg_hor5'),
                df.value.substr(51, 5).alias('num_hor6'),
                df.value.substr(56, 1).alias('flg_hor6'),
                df.value.substr(57, 5).alias('num_hor7'),
                df.value.substr(62, 1).alias('flg_hor7'),
                df.value.substr(63, 5).alias('num_hor8'),
                df.value.substr(68, 1).alias('flg_hor8'),
                df.value.substr(69, 5).alias('num_hor9'),
                df.value.substr(74, 1).alias('flg_hor9'),
                df.value.substr(75, 5).alias('num_hor10'),
                df.value.substr(80, 1).alias('flg_hor10'),
                df.value.substr(81, 5).alias('num_hor11'),
                df.value.substr(86, 1).alias('flg_hor11'),
                df.value.substr(87, 5).alias('num_hor12'),
                df.value.substr(92, 1).alias('flg_hor12'),
                df.value.substr(93, 5).alias('num_hor13'),
                df.value.substr(98, 1).alias('flg_hor13'),
                df.value.substr(99, 5).alias('num_hor14'),
                df.value.substr(104, 1).alias('flg_hor14'),
                df.value.substr(105, 5).alias('num_hor15'),
                df.value.substr(110, 1).alias('flg_hor15'),
                df.value.substr(111, 5).alias('num_hor16'),
                df.value.substr(116, 1).alias('flg_hor16'),
                df.value.substr(117, 5).alias('num_hor17'),
                df.value.substr(122, 1).alias('flg_hor17'),
                df.value.substr(123, 5).alias('num_hor18'),
                df.value.substr(128, 1).alias('flg_hor18'),
                df.value.substr(129, 5).alias('num_hor19'),
                df.value.substr(134, 1).alias('flg_hor19'),
                df.value.substr(135, 5).alias('num_hor20'),
                df.value.substr(140, 1).alias('flg_hor20'),
                df.value.substr(141, 5).alias('num_hor21'),
                df.value.substr(146, 1).alias('flg_hor21'),
                df.value.substr(141, 5).alias('num_hor22'),
                df.value.substr(152, 1).alias('flg_hor22'),
                df.value.substr(153, 5).alias('num_hor23'),
                df.value.substr(158, 1).alias('flg_hor23'),
                df.value.substr(159, 5).alias('num_hor24'),
                df.value.substr(164, 1).alias('flg_hor24')
            )

        marksColumns = [col("num_hor1"), col("num_hor2"), col("num_hor3"), col("num_hor4"), col("num_hor5"),
                    col("num_hor6"),
                    col("num_hor7"), col("num_hor8"), col("num_hor9"), col("num_hor10"), col("num_hor11"),
                    col("num_hor12"),
                    col("num_hor13"), col("num_hor14"), col("num_hor15"), col("num_hor16"), col("num_hor17"),
                    col("num_hor18"),
                    col("num_hor19"), col("num_hor20"), col("num_hor21"), col("num_hor22"), col("num_hor23"),
                    col("num_hor24")]
        averageFunc = sum(x for x in marksColumns) / len(marksColumns)

        # Se filtra para quedarnos con la magnitud NO2 y se identifican los valores no válidos
        # Se calcula el valor máximo y la media de los valores
        df = df.where('cod_parametros == ' + contaminante)\
            .withColumn('num_hor1', when(df["flg_hor1"] != "V", lit("00000")).otherwise(df["num_hor1"]))\
            .withColumn('num_hor2', when(df["flg_hor2"] != "V", lit("00000")).otherwise(df["num_hor2"]))\
            .withColumn('num_hor3', when(df["flg_hor3"] != "V", lit("00000")).otherwise(df["num_hor3"]))\
            .withColumn('num_hor4', when(df["flg_hor4"] != "V", lit("00000")).otherwise(df["num_hor4"]))\
            .withColumn('num_hor5', when(df["flg_hor5"] != "V", lit("00000")).otherwise(df["num_hor5"]))\
            .withColumn('num_hor6', when(df["flg_hor6"] != "V", lit("00000")).otherwise(df["num_hor6"]))\
            .withColumn('num_hor7', when(df["flg_hor7"] != "V", lit("00000")).otherwise(df["num_hor7"]))\
            .withColumn('num_hor8', when(df["flg_hor8"] != "V", lit("00000")).otherwise(df["num_hor8"]))\
            .withColumn('num_hor9', when(df["flg_hor9"] != "V", lit("00000")).otherwise(df["num_hor9"]))\
            .withColumn('num_hor10', when(df["flg_hor10"] != "V", lit("00000")).otherwise(df["num_hor10"]))\
            .withColumn('num_hor11', when(df["flg_hor11"] != "V", lit("00000")).otherwise(df["num_hor11"]))\
            .withColumn('num_hor12', when(df["flg_hor12"] != "V", lit("00000")).otherwise(df["num_hor12"]))\
            .withColumn('num_hor13', when(df["flg_hor13"] != "V", lit("00000")).otherwise(df["num_hor13"]))\
            .withColumn('num_hor14', when(df["flg_hor14"] != "V", lit("00000")).otherwise(df["num_hor14"]))\
            .withColumn('num_hor15', when(df["flg_hor15"] != "V", lit("00000")).otherwise(df["num_hor15"]))\
            .withColumn('num_hor16', when(df["flg_hor16"] != "V", lit("00000")).otherwise(df["num_hor16"]))\
            .withColumn('num_hor17', when(df["flg_hor17"] != "V", lit("00000")).otherwise(df["num_hor17"]))\
            .withColumn('num_hor18', when(df["flg_hor18"] != "V", lit("00000")).otherwise(df["num_hor18"]))\
            .withColumn('num_hor19', when(df["flg_hor19"] != "V", lit("00000")).otherwise(df["num_hor19"]))\
            .withColumn('num_hor20', when(df["flg_hor20"] != "V", lit("00000")).otherwise(df["num_hor20"]))\
            .withColumn('num_hor21', when(df["flg_hor21"] != "V", lit("00000")).otherwise(df["num_hor21"]))\
            .withColumn('num_hor22', when(df["flg_hor22"] != "V", lit("00000")).otherwise(df["num_hor22"]))\
            .withColumn('num_hor23', when(df["flg_hor23"] != "V", lit("00000")).otherwise(df["num_hor23"]))\
            .withColumn('num_hor24', when(df["flg_hor24"] != "V", lit("00000")).otherwise(df["num_hor24"]))\
            .withColumn('max_cont',  greatest("num_hor1", "num_hor2", "num_hor3", "num_hor4", "num_hor5", "num_hor6",
                                             "num_hor7", "num_hor8", "num_hor9", "num_hor10", "num_hor11", "num_hor12",
                                             "num_hor13", "num_hor14", "num_hor15", "num_hor16", "num_hor17",
                                             "num_hor18", "num_hor19", "num_hor20", "num_hor21", "num_hor22",
                                             "num_hor23", "num_hor24"))\
            .withColumn('avg_cont', averageFunc)

        return df
