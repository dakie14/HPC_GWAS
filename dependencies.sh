#!/usr/bin/env bash

ENV=base
set -eux

pip install pyarrow==0.14.1
pip install pandas==1.1.3
pip install statsmodels==0.11.0
pip install numpy==1.16.5
pip install patsy==0.5.1
pip install lxml==4.5.2
pip install flask==1.1.2
pip install requests==2.24.0
pip install click==7.1.2