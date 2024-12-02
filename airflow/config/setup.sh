################################################################################################
#   Project: SmartTraffic_Lakehouse_for_HCMC
#   Author: Nguyen Trung Nghia (ren294)
#   Contact: trungnghia294@gmail.com
#   GitHub: Ren294
################################################################################################
airflow connections add spark_server \
    --conn-type ssh \
    --conn-host spark-master \
    --conn-login root \
    --conn-password ren294 \
    --conn-port 22