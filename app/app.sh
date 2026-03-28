#!/bin/bash

cd /app

service ssh restart

bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate

pip install --quiet -r requirements.txt

venv-pack -o .venv.tar.gz

bash prepare_data.sh

bash index.sh

bash search.sh "machine learning"
