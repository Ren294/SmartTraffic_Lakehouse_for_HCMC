"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""


def spark_submit(file_path):
    return f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/{file_path}"
