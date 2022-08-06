#   /bin/bash

# create exe/keys/versions, zip package, sign, and move files to pyu-data/deploy ready for deployment
# run from terminal with: zsh build.sh

upload=$1
version="3.7.12"
echo "version: $version, upload: $upload"

# scripts in build_scripts for organization, but run commands from project root
cd .

echo "building..."
poetry run pyupdater build --log-level=WARN --app-version=$version guesttracker.spec

echo "packaging..."
poetry run pyupdater pkg --process --sign

if [ $upload = true ]; then
    echo "uploading..."
    bash scripts/upload.sh
fi