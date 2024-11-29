from pyspark.sql import SparkSession


def create_spark_session(user, password, appname):
    """Create Spark session with necessary configurations for Hudi and lakeFS"""
    return SparkSession.builder \
        .appName(appname) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
        .config("spark.jars.packages",
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
                "io.lakefs:hadoop-lakefs-assembly:0.2.4") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.lakefs.impl", "io.lakefs.LakeFSFileSystem") \
        .config("spark.hadoop.fs.lakefs.access.key", user) \
        .config("spark.hadoop.fs.lakefs.secret.key", password) \
        .config("spark.hadoop.fs.lakefs.endpoint", "http://lakefs:8000/api/v1") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://lakefs:8000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.access.key", user) \
        .config("spark.hadoop.fs.s3a.secret.key", password) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
