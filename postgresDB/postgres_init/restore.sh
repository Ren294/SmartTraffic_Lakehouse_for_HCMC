#!/bin/bash

until pg_isready -U postgres -d traffic
do
  echo "Waiting for postgres..."
  sleep 2
done

PARKING_EXISTS=$(psql -U postgres -d traffic -tAc "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'parking'")
GASSTATION_EXISTS=$(psql -U postgres -d traffic -tAc "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'gasstation'")

if [ "$PARKING_EXISTS" -eq "0" ]; then
    echo "Creating parking schema..."
    psql -U postgres -d traffic -c "CREATE SCHEMA parking;"
fi

if [ "$GASSTATION_EXISTS" -eq "0" ]; then
    echo "Creating gasstation schema..."
    psql -U postgres -d traffic -c "CREATE SCHEMA gasstation;"
fi

if [ -f "/home/parking_backup.dump" ]; then
    echo "Restoring parking schema..."
    pg_restore -U postgres -d traffic --clean --if-exists --no-owner --no-privileges /home/parking_backup.dump
fi

if [ -f "/home/gasstation_backup.dump" ]; then
    echo "Restoring gasstation schema..."
    pg_restore -U postgres -d traffic --clean --if-exists --no-owner --no-privileges /home/gasstation_backup.dump
fi

echo "Restore process completed"