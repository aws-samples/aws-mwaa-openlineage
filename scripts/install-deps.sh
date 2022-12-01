#!/bin/bash

set -o errexit
set -o verbose

# Install ckd project dependencies
python -m pip install -r requirements.txt

# zip the plugins (if required)
cd ./orchestration/runtime/mwaa/plugins; zip -r ../plugins.zip ./; cd ../../../../
cd ./orchestration/runtime/mwaa/plugins; ls; cd ../../../../
