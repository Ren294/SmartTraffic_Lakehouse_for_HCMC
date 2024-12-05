"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""


def get_metastore_properties():
    return {
        "warehouse_dir": "s3a://gold/main/warehouse",
        "ConnectionDriverName": "org.postgresql.Driver",
        "ConnectionURL": "jdbc:postgresql://metastore_db:5432/metastore",
        "ConnectionUserName": "hive",
        "ConnectionPassword": "hive",
        "metastore_version": "3.1.3"
    }
