# -*- coding: utf-8 -*-
# necessario o cabecalho do utf8 para funcionar as deficoes das funcoes do flatten

# para execucao: spark-submit local_arquivo/ingestao_json_flatten.py

# 1. Enviar arquivos JSON para HDFS
# hadoop fs -put <arquivo> /<diretorio_destino_hdfs>

# 2. import blibliotecas 
from pyspark import SparkContext, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, explode, lit, count
import pyspark.sql.functions as F
import pyspark.sql.utils
from pyspark.sql.types import DoubleType



# 4. Contextos spark e sql



sc = SparkContext.getOrCreate()

sqlContext = SQLContext(sc)

# 5. data frame carregado do json - conseguindo enxergar o schema
# campos ainda aninhados pré flatten
df = sqlContext.read.json(sc.wholeTextFiles("hdfs://localhost/tmp/carteiraRecomendada.json").values())
df.printSchema()

# 6. aplicando flatten no data frame
# todos os campos sendo visualizados no nível root
dfbronze = df
dfbronze.printSchema()
#dfFlatten.show()

#dfResult = dfFlatten.write.format("orc")

# 7. salvando os dados no formato parquet no hdfs
# podemos alterar para orc caso necessario
dfbronze.write.json("hdfs://localhost/tmp/ingestao_teste_2")







