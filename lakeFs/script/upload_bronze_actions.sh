#!/bin/bash

# Đường dẫn source và destination
SOURCE_DIR="/home/actions"
REPO="bronze"
BRANCH="main"
LAKEFS_PATH="_lakefs_actions"

# Kiểm tra thư mục source tồn tại
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory $SOURCE_DIR does not exist"
    exit 1
fi

# Tìm tất cả file .yaml trong thư mục source
echo "Finding YAML files in $SOURCE_DIR..."
yaml_files=$(find "$SOURCE_DIR" -name "*.yaml")

if [ -z "$yaml_files" ]; then
    echo "No YAML files found in $SOURCE_DIR"
    exit 0
fi

# Upload từng file YAML
for file in $yaml_files; do
    filename=$(basename "$file")
    destination_uri="lakefs://${REPO}/${BRANCH}/${LAKEFS_PATH}/${filename}"
    echo "Uploading $filename to LakeFS..."
    
    # Sử dụng lakectl để upload file với cú pháp đúng
    lakectl fs upload --source "$file" "$destination_uri"
    
    if [ $? -eq 0 ]; then
        echo "Successfully uploaded $filename"
    else
        echo "Failed to upload $filename"
    fi
done

echo "Upload process completed"

# Commit các thay đổi
echo "Committing changes..."
commit_uri="lakefs://${REPO}/${BRANCH}"
lakectl commit "$commit_uri" -m "Uploaded YAML files from actions directory"

if [ $? -eq 0 ]; then
    echo "Successfully committed changes"
else
    echo "Failed to commit changes"
fi