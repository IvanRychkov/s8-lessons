from pyspark.sql import SparkSession, DataFrame

spark_jars_packages = ','.join(
    [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0',
    ],
)

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required '
                              'username=\"de-student\" password=\"ltcneltyn\";',
}


def spark_init() -> SparkSession:
    return SparkSession.builder \
        .master('local') \
        .appName('spark streaming test') \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()


def load_topic(spark: SparkSession, topic: str) -> DataFrame:
    return spark.read.format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .options(**kafka_security_options) \
        .option('subscribe', topic) \
        .load()
