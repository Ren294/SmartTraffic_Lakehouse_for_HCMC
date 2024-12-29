########################################################################################
# Project: SmartTraffic_Lakehouse_for_HCMC
# Author: Nguyen Trung Nghia (ren294)
# Contact: trungnghia294@gmail.com
# GitHub: Ren294
########################################################################################
curl -X POST --location "http://localhost:8083/connectors" \
-H "Content-Type: application/json" \
-H "Accept: application/json" \
-d @connector-db/postgres_con.json