#!/bin/bash

SOURCE_DIR="/home/actions/silver"
REPO="silver"
BRANCH="main"
LAKEFS_PATH="_lakefs_actions"

if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory $SOURCE_DIR does not exist"
    exit 1
fi

echo "Finding YAML files in $SOURCE_DIR..."
yaml_files=$(find "$SOURCE_DIR" -name "*.yaml")

if [ -z "$yaml_files" ]; then
    echo "No YAML files found in $SOURCE_DIR"
    exit 0
fi

for file in $yaml_files; do
    filename=$(basename "$file")
    destination_uri="lakefs://${REPO}/${BRANCH}/${LAKEFS_PATH}/${filename}"
    echo "Uploading $filename to LakeFS..."
    
    lakectl fs upload --source "$file" "$destination_uri"
    
    if [ $? -eq 0 ]; then
        echo "Successfully uploaded $filename"
    else
        echo "Failed to upload $filename"
    fi
done

echo "Upload process completed"

echo "Committing changes..."
commit_uri="lakefs://${REPO}/${BRANCH}"
lakectl commit "$commit_uri" -m "Uploaded YAML files from silver actions directory"

if [ $? -eq 0 ]; then
    echo "Successfully committed changes"
else
    echo "Failed to commit changes"
fi