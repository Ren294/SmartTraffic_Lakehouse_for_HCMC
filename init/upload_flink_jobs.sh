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

FLINK_CONTAINER="flink-jobmanager"
FLINK_JOBMANAGER="jobmanager"
FLINK_JOBMANAGER_PORT="8081"

JAR_DIR="/opt/flink/usr/lib"

UPLOADED_COUNT=0
FAILED_COUNT=0

echo -e "${YELLOW}Starting Flink job upload process...${NC}"

if ! docker ps | grep -q $FLINK_CONTAINER; then
    echo -e "${RED}Error: Flink JobManager container ($FLINK_CONTAINER) is not running${NC}"
    exit 1
fi

wait_for_jobmanager() {
    echo -e "${YELLOW}Waiting for JobManager to be ready...${NC}"
    while ! docker exec $FLINK_CONTAINER curl -s "http://${FLINK_JOBMANAGER}:${FLINK_JOBMANAGER_PORT}/overview" > /dev/null; do
        echo -e "${YELLOW}JobManager not ready yet, waiting...${NC}"
        sleep 5
    done
    echo -e "${GREEN}JobManager is ready!${NC}"
}

wait_for_jobmanager

echo -e "\n${YELLOW}Scanning for JAR files in $JAR_DIR${NC}"
JAR_FILES=$(docker exec $FLINK_CONTAINER find $JAR_DIR -type f -name "*.jar")

if [ -z "$JAR_FILES" ]; then
    echo -e "${RED}No JAR files found in $JAR_DIR${NC}"
    exit 1
fi

for jar in $JAR_FILES; do
    filename=$(basename "$jar")
    echo -e "\n${YELLOW}Uploading: $filename${NC}"
    
    if docker exec $FLINK_CONTAINER /opt/flink/bin/flink run --detached "$jar"; then
        echo -e "${GREEN}✓ Successfully uploaded: $filename${NC}"
        UPLOADED_COUNT=$((UPLOADED_COUNT + 1))
    else
        echo -e "${RED}✗ Failed to upload: $filename${NC}"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
done

echo -e "\n${GREEN}Upload process completed!${NC}"
echo -e "Successfully uploaded jobs: ${GREEN}$UPLOADED_COUNT${NC}"
if [ $FAILED_COUNT -gt 0 ]; then
    echo -e "Failed uploads: ${RED}$FAILED_COUNT${NC}"
fi

echo -e "\n${YELLOW}Current running jobs:${NC}"
docker exec $FLINK_CONTAINER /opt/flink/bin/flink list