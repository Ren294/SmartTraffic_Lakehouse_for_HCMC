#!/bin/bash
########################################################################################
# Project: SmartTraffic_Lakehouse_for_HCMC
# Author: Nguyen Trung Nghia (ren294)
# Contact: trungnghia294@gmail.com
# GitHub: Ren294
########################################################################################
username='AKIAJC5AQUW4OXQYCRAQ'
password='iYf4H8GSjY+HMfN70AMquj0+PRpYcgUl0uN01G7Z'

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $1 completed successfully${NC}"
    else
        echo -e "${RED}✗ $1 failed${NC}"
        exit 1
    fi
}

echo -e "${YELLOW}Starting initialization process...${NC}"

echo -e "\n${YELLOW}Step 1: Dowloading data for project...${NC}"
chmod +x ./init/init/download_data.sh
./init/update_lakefs_credentials.sh "${username}" "${password}"
check_status "Data downloaded"

echo -e "\n${YELLOW}Step 1: Updating LakeFS credentials...${NC}"
chmod +x ./init/update_lakefs_credentials.sh
./init/update_lakefs_credentials.sh "${username}" "${password}"
check_status "LakeFS credentials update"

echo -e "\n${YELLOW}Step 2: Initializing storage...${NC}"
chmod +x ./init/init_storage.sh
./init/init_storage.sh
check_status "Storage initialization"

echo -e "\n${YELLOW}Step 3: Setting up Airflow connector...${NC}"
chmod +x ./init/set_up_airflow_connector.sh
./init/set_up_airflow_connector.sh
check_status "Airflow connector setup"

echo -e "\n${YELLOW}Step 4: Creating Debezium connector...${NC}"
chmod +x ./init/create_connector_debezium.sh
./init/create_connector_debezium.sh
check_status "Debezium connector creation"

echo -e "\n${YELLOW}Step 5: Uploading Flink jobs...${NC}"
chmod +x ./init/upload_flink_jobs.sh
./init/upload_flink_jobs.sh
check_status "Flink jobs upload"

echo -e "\n${GREEN}Initialization process completed successfully!${NC}"
echo -e "\n${YELLOW}Performing final verification:${NC}"
echo -e "${YELLOW}1. Checking Minio buckets:${NC}"
docker exec storage mc ls renStorage/

echo -e "\n${YELLOW}2. Checking LakeFS repositories:${NC}"
docker exec lakefs lakectl repo list

echo -e "\n${YELLOW}3. Checking Flink jobs:${NC}"
docker exec flink-jobmanager /opt/flink/bin/flink list

echo -e "\n${GREEN}All initialization steps completed! The system is ready to use.${NC}"