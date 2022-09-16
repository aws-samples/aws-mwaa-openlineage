#!/bin/bash
set -o errexit
set -o verbose

sudo npm install -g aws-cdk@latest
python3 -m venv .env
source .env/bin/activate
.env/bin/python -m pip install pip-tools
# build requirements.txt from setup.py
.env/bin/python -m piptools compile setup.py --extra dev 
# Install ckd project dependencies
.env/bin/python -m pip install -r requirements.txt
# compile runtime requirements
pip-compile -r ./orchestration/runtime/mwaa/requirements.in

# zip the plugins (if required)
cd ./orchestration/runtime/mwaa/plugins; zip -r ../plugins.zip ./; cd ../../../../
cd ./orchestration/runtime/mwaa/plugins; ls; cd ../../../../

cdk bootstrap


