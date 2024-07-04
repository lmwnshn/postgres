#!/usr/bin/env bash

# PG
sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache
# psycopg
sudo apt-get install libpq5

sudo apt install python3.10-venv
python3 -m venv venv
source ./venv/bin/activate
pip3 install pglast psycopg sqlalchemy tqdm
