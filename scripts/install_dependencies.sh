#!/usr/bin/env bash

INSTALL_COMMAND="sudo pip install"
dependencies="numpy==1.16.5 pandas==0.24.2 slackclient==1.3.2 statsmodels==0.10.1 pyarrow==0.14.1 boto3 botocore py4j"

sudo apt-get install -y python-pip gcc
for dep in $dependencies; do
    $INSTALL_COMMAND $dep
done;

sudo mkdir /opt/x13arima
wget https://www.census.gov/ts/x13as/unix/x13asall_V1.1_B39.tar.gz 
sudo tar xvf x13asall_V1.1_B39.tar.gz -C /opt/x13arima
sudo chmod -R 777 /opt/x13arima/
sudo ln -s /opt/x13arima/x13as /usr/bin/x13as
