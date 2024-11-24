def get_postgres_properties():
    return {
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://postgres_traffic:5432/traffic",
        "user": "postgres",
        "password": "postgres"
    }
