#!/bin/bash
########################################################################################
# Project: SmartTraffic_Lakehouse_for_HCMC
# Author: Nguyen Trung Nghia (ren294)
# Contact: trungnghia294@gmail.com
# GitHub: Ren294
########################################################################################
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

LAYERS=("bronze" "silver" "gold")

echo -e "${YELLOW}Cleaning up existing layers...${NC}"

for layer in "${LAYERS[@]}"
do
    echo -e "${YELLOW}Removing $layer repository from LakeFS...${NC}"
    docker exec lakefs lakectl repo delete lakefs://$layer --force
    if [ $? -eq 0 ]; then
        echo -e "${RED}✓ Removed $layer repository from LakeFS${NC}"
    fi
done

for layer in "${LAYERS[@]}"
do
    echo -e "${YELLOW}Removing $layer bucket from Minio...${NC}"
    docker exec storage mc rb renStorage/$layer --force
    if [ $? -eq 0 ]; then
        echo -e "${RED}✓ Removed $layer bucket from Minio${NC}"
    fi
done

echo -e "${RED}Cleanup completed!${NC}"

echo -e "\n${YELLOW}Verifying Minio buckets after cleanup:${NC}"
docker exec storage mc ls renStorage/