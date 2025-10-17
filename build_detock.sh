#!/bin/bash

IMAGE_NAME=${1:-detock}
BASE_REPO_ADDRESS=${2:-omraz}
REPO_NAME=${3:-seq_eval}

# Create a Python 3.8 virtual environment if it doesn't already exist
if [ ! -d "build_detock" ]; then
    python3.8 -m venv build_detock
fi
source build_detock/bin/activate

# Upgrade pip
pip install --upgrade pip

# Set 'python' to use Python 3.8 in this shell
alias python="python3.8"

pip install -r tools/requirements.txt

push_address=${BASE_REPO_ADDRESS}/${REPO_NAME}:${IMAGE_NAME}
docker build . -t ${push_address}
docker push ${push_address}

# Tag and push the Detock img
#docker tag $IMAGE_NAME ${push_address}
#docker push ${push_address}