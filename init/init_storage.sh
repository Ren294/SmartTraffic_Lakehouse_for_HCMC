#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

LAYERS=("bronze" "silver" "gold")

echo -e "${YELLOW}Configuring Minio client...${NC}"
docker exec storage mc alias set renStorage http://localhost:9000 ren294 trungnghia294

echo -e "\n${YELLOW}Cleaning up existing storage...${NC}"
./init/cleanup_storage.sh

echo -e "\n${YELLOW}Starting initialization...${NC}"

for layer in "${LAYERS[@]}"
do
    echo -e "${YELLOW}Creating $layer bucket in Minio...${NC}"
    docker exec storage mc mb renStorage/$layer
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Successfully created $layer bucket in Minio${NC}"
    fi
done

for layer in "${LAYERS[@]}"
do
    echo -e "${YELLOW}Creating $layer repository in LakeFS...${NC}"
    docker exec lakefs lakectl repo create lakefs://$layer s3://$layer
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Successfully created $layer repository in LakeFS${NC}"
    fi
done

echo -e "${GREEN}Setup completed!${NC}"

echo -e "\n${YELLOW}Verifying Minio buckets:${NC}"
docker exec storage mc ls renStorage/

echo -e "\n${YELLOW}Verifying LakeFS repositories:${NC}"
docker exec lakefs lakectl repo list