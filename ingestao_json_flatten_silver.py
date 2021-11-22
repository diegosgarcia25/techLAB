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


# 3. Funcao para fazer o Flatten do JSON
def flatten_structs(nested_df):

    stack = [((), nested_df)]

    columns = []

    while len(stack) > 0:

        parents, df = stack.pop()

        array_cols = [

            c[0]

            for c in df.dtypes

            if c[1][:5] == "array"

        ]

        flat_cols = [

            F.col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))

            for c in df.dtypes

            if c[1][:6] != "struct"

        ]

        nested_cols = [

            c[0]

            for c in df.dtypes

            if c[1][:6] == "struct"

        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:

            projected_df = df.select(nested_col + ".*")

            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)



# 3. Continuacao definicao para fazer o Flatten do JSON - caso especifico de array

def flatten_array_struct_df(df):

    array_cols = [

            c[0]

            for c in df.dtypes

            if c[1][:5] == "array"

        ]

    while len(array_cols) > 0:

        for array_col in array_cols:

            cols_to_select = [x for x in df.columns if x != array_col ]

            df = df.withColumn(array_col, F.explode(F.col(array_col)))

        df = flatten_structs(df)

        array_cols = [

            c[0]

            for c in df.dtypes

            if c[1][:5] == "array"

        ]

    return df


# 4. Contextos spark e sql



sc = SparkContext.getOrCreate()

sqlContext = SQLContext(sc)

# 5. data frame carregado do json - conseguindo enxergar o schema
# campos ainda aninhados pré flatten
df = sqlContext.read.json(sc.wholeTextFiles("hdfs://localhost/tmp/ingestao_teste/*").values())
df.printSchema()

#df = sqlContext.read.json("hdfs://localhost/tmp/teste/*")


# 6. aplicando flatten no data frame
# todos os campos sendo visualizados no nível root
dfFlatten = flatten_array_struct_df(flatten_structs(df))
dfFlatten.printSchema()
dfFlatten.show()

#dfResult = dfFlatten.write.format("orc")

# 7. salvando os dados no formato parquet no hdfs
# podemos alterar para orc caso necessario
dfFlatten.write.mode("append").parquet("hdfs://localhost/tmp/ingestao_silver")







