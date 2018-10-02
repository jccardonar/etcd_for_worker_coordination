#!/bin/bash
FOLDER=`pwd`
LIBRARY_FOLDER=${FOLDER}/..
PYTHONPATH=$PYTHONPATH:$LIBRARY_FOLDER
# erase coverage data
coverage erase
echo "Testing worker, please have patience"
coverage run --source=.. try_worker.py || { echo 'Worker code failed. Check Code.' ; exit 1; }
echo "Testing coordinator, please have even more patience"
coverage run --source=.. -a try_coordinator.py || { echo 'Coordinator code failed. Check Code.' ; exit 1; }
coverage report
