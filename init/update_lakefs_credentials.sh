#!/bin/bash

# update_lakefs_credentials.sh

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Check if credentials are provided
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

update_python_files
update_yaml_file

echo -e "\n${GREEN}Operation completed!${NC}"
echo -e "Total modified files: ${MODIFIED_COUNT}"

if [ $MODIFIED_COUNT -gt 0 ]; then
    echo -e "\n${YELLOW}Summary of changes:${NC}"
    echo "New access_key_id: $NEW_USERNAME"
    echo "New secret_access_key: $NEW_PASSWORD"
fi