#!/bin/bash

cd ~/datarobot/datagun || exit
pdm run extract --config ./datagun/config/adp.json

cd ~/datarobot/adp-sync || exit
pdm run update-workers
