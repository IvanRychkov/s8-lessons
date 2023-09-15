from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName('spark reads from postgres')
    .config('spark.jars.packages', 'org.postgresql:postgresql:42.4.0')
    .getOrCreate()
)

df = (
    spark
    .read.jdbc(
        url='jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
        table='public.marketing_companies',
        properties=dict(
            driver='org.postgresql.Driver',
            user='student',
            password='de-student',
        )
    )
)
df.printSchema()
df.count()
