"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""


def get_postgres_properties():
    return {
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://postgres_traffic:5432/traffic",
        "user": "postgres",
        "password": "postgres"
    }
