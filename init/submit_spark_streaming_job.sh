#!/bin/bash
########################################################################################
# Project: SmartTraffic_Lakehouse_for_HCMC
# Author: Nguyen Trung Nghia (ren294)
# Contact: trungnghia294@gmail.com
# GitHub: Ren294
########################################################################################
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

SPARK_MASTER="spark://spark-master:7077"
SPARK_APPS_DIR="/opt/spark-apps/streaming"

STREAMING_JOBS=(
    "AccidentDataToSilverLayer.py"
    "ParkinglotToSilverProcessor.py"
    "StoragetankToSilverProcessor.py"
    "TrafficDataToSilverLayer.py"
    "WeatherDataToSilverLayer.py"
)

echo -e "${YELLOW}Starting Spark streaming jobs submission...${NC}"

for job in "${STREAMING_JOBS[@]}"; do
    echo -e "${YELLOW}Submitting job: $job${NC}"
    
    docker exec spark-master nohup /opt/spark/bin/spark-submit \
        --master ${SPARK_MASTER} \
        ${SPARK_APPS_DIR}/${job} \
        > ./logs/spark/${job}.log 2>&1 &
        
    sleep 2
    
    if docker exec spark-master /opt/spark/bin/spark-submit --status | grep -q "${job}"; then
        echo -e "${GREEN}✓ Successfully submitted $job${NC}"
    else
        echo -e "${RED}✗ Failed to submit $job${NC}"
        exit 1
    fi
done

echo -e "${GREEN}All Spark streaming jobs submitted successfully!${NC}"

echo -e "\n${YELLOW}Verifying running Spark applications:${NC}"
docker exec spark-master /opt/spark/bin/spark-submit --status

echo -e "\n${YELLOW}Logs are being written to ./logs/spark/ directory${NC}"