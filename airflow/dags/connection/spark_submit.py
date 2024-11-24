def spark_submit(file_path):
    return f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/{file_path}"
