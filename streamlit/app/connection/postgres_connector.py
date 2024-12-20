"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""

import psycopg2


def get_postgres_connection():
    return psycopg2.connect(
        dbname="traffic",
        user="postgres",
        password="postgres",
        host="postgres_traffic",
        port="5432"
    )
