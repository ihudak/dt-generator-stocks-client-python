#!/bin/sh

. venv/bin/activate

#export LOG_DEST=stocks_py
export LOG_DEST=stocks
export NUMLOOPS=1000
export CREATESTOCKS=10
export HARDWORK_INTENSITY=50

python ./stock_client.py

