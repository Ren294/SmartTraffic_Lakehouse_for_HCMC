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

if [ "$#" -ne 2 ]; then
    echo -e "${RED}Error: Please provide both username and password${NC}"
    echo "Usage: $0 <access_key_id> <secret_access_key>"
    exit 1
fi

NEW_USERNAME=$1
NEW_PASSWORD=$2

echo -e "${YELLOW}Starting credentials update process...${NC}"

MODIFIED_COUNT=0

update_python_files() {
    echo -e "\n${YELLOW}Searching for lakefs_connector.py files...${NC}"
    
    PYTHON_FILES=$(find . -type f -name "lakefs_connector.py")

    if [ -z "$PYTHON_FILES" ]; then
        echo -e "${YELLOW}No lakefs_connector.py files found${NC}"
        return
    fi

    for file in $PYTHON_FILES; do
        echo -e "${YELLOW}Processing Python file: $file${NC}"
        
        cp "$file" "${file}.bak"
        
        sed -i.tmp \
            -e "s|'username': '[^']*'|'username': '$NEW_USERNAME'|" \
            -e "s|'password': '[^']*'|'password': '$NEW_PASSWORD'|" \
            "$file"
        
        if ! cmp -s "$file" "${file}.bak"; then
            echo -e "${GREEN}✓ Updated credentials in: $file${NC}"
            MODIFIED_COUNT=$((MODIFIED_COUNT + 1))
        else
            echo -e "${YELLOW}No changes needed in: $file${NC}"
        fi
        
        rm "${file}.tmp" "${file}.bak"
    done
}

update_yaml_file() {
    YAML_FILE="./lakeFs/auth/lakectl.yaml"
    
    echo -e "\n${YELLOW}Processing YAML file: $YAML_FILE${NC}"
    
    if [ ! -f "$YAML_FILE" ]; then
        echo -e "${YELLOW}YAML file not found at: $YAML_FILE${NC}"
        return
    fi
    
    cp "$YAML_FILE" "${YAML_FILE}.bak"
    
    sed -i.tmp \
        -e "s|access_key_id: .*|access_key_id: $NEW_USERNAME|" \
        -e "s|secret_access_key: .*|secret_access_key: $NEW_PASSWORD|" \
        "$YAML_FILE"
    
    if ! cmp -s "$YAML_FILE" "${YAML_FILE}.bak"; then
        echo -e "${GREEN}✓ Updated credentials in: $YAML_FILE${NC}"
        MODIFIED_COUNT=$((MODIFIED_COUNT + 1))
    else
        echo -e "${YELLOW}No changes needed in: $YAML_FILE${NC}"
    fi

    rm "${YAML_FILE}.tmp" "${YAML_FILE}.bak"
}

update_docker_compose() {
    DOCKER_COMPOSE_FILE="./docker-compose.yml"
    
    echo -e "\n${YELLOW}Processing Docker Compose file: $DOCKER_COMPOSE_FILE${NC}"
    
    if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
        echo -e "${YELLOW}Docker Compose file not found at: $DOCKER_COMPOSE_FILE${NC}"
        return
    fi
    
    cp "$DOCKER_COMPOSE_FILE" "${DOCKER_COMPOSE_FILE}.bak"
    
    sed -i.tmp \
        -e "s|S3_ACCESS_KEY: .*|S3_ACCESS_KEY: $NEW_USERNAME|" \
        -e "s|S3_SECRET_KEY: .*|S3_SECRET_KEY: $NEW_PASSWORD|" \
        "$DOCKER_COMPOSE_FILE"
    
    if ! cmp -s "$DOCKER_COMPOSE_FILE" "${DOCKER_COMPOSE_FILE}.bak"; then
        echo -e "${GREEN}✓ Updated credentials in: $DOCKER_COMPOSE_FILE${NC}"
        MODIFIED_COUNT=$((MODIFIED_COUNT + 1))
    else
        echo -e "${YELLOW}No changes needed in: $DOCKER_COMPOSE_FILE${NC}"
    fi

    rm "${DOCKER_COMPOSE_FILE}.tmp" "${DOCKER_COMPOSE_FILE}.bak"
}

update_python_files
update_yaml_file
update_docker_compose

echo -e "\n${GREEN}Operation completed!${NC}"
echo -e "Total modified files: ${MODIFIED_COUNT}"

if [ $MODIFIED_COUNT -gt 0 ]; then
    echo -e "\n${YELLOW}Summary of changes:${NC}"
    echo "New access_key_id: $NEW_USERNAME"
    echo "New secret_access_key: $NEW_PASSWORD"
    echo -e "\n${YELLOW}Note: If you've updated credentials in docker-compose.yml, you may need to restart your containers${NC}"
fi