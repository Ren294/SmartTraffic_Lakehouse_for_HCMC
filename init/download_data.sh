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

mkdir -p ./postgresDB/backup
mkdir -p ./nifi/data

handle_postgres_dump() {
    local zip_name=$1
    local dataset_name=$2
    
    echo -e "${YELLOW}Downloading ${dataset_name}...${NC}"
    curl -L -o "${zip_name}.zip" \
        "https://www.kaggle.com/api/v1/datasets/download/ren294/${dataset_name}"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Successfully downloaded ${dataset_name}${NC}"
        
        echo -e "${YELLOW}Extracting ${dataset_name}...${NC}"
        unzip -j "${zip_name}.zip" "*.dump" -d ./postgresDB/backup/
        
        echo -e "${YELLOW}Cleaning up temporary files...${NC}"
        rm "${zip_name}.zip"
        
        echo -e "${GREEN}Successfully processed ${dataset_name}${NC}"
    else
        echo -e "${RED}Failed to download ${dataset_name}${NC}"
        return 1
    fi
}

handle_nifi_data() {
    local zip_name=$1
    local dataset_name=$2
    
    echo -e "${YELLOW}Downloading ${dataset_name}...${NC}"
    curl -L -o "${zip_name}.zip" \
        "https://www.kaggle.com/api/v1/datasets/download/ren294/${dataset_name}"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Successfully downloaded ${dataset_name}${NC}"
        
        echo -e "${YELLOW}Extracting ${dataset_name}...${NC}"
        unzip -j "${zip_name}.zip" -d ./nifi/data/
        
        echo -e "${YELLOW}Cleaning up temporary files...${NC}"
        rm "${zip_name}.zip"
        
        echo -e "${GREEN}Successfully processed ${dataset_name}${NC}"
    else
        echo -e "${RED}Failed to download ${dataset_name}${NC}"
        return 1
    fi
}

echo -e "${YELLOW}Starting data download and organization process...${NC}"

echo -e "\n${YELLOW}Processing PostgreSQL database dumps...${NC}"
handle_postgres_dump "parkingdb" "parkingdb-hcmcity-postgres"
handle_postgres_dump "gasstationdb" "gasstationdb-hcmcity-postgres"

echo -e "\n${YELLOW}Processing NiFi data...${NC}"
handle_nifi_data "weather-api" "weather-api-hcmcity"
handle_nifi_data "traffic-accidents" "traffic-accidents-hcmcity"
handle_nifi_data "iot-car" "iot-car-hcmcity"

echo -e "\n${GREEN}All downloads and extractions completed!${NC}"

echo -e "\n${YELLOW}Verifying downloads:${NC}"
echo -e "${YELLOW}PostgreSQL dumps:${NC}"
ls -l ./postgresDB/backup/
echo -e "\n${YELLOW}NiFi data:${NC}"
ls -l ./nifi/data/