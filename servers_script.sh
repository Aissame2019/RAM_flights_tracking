#! /bin/bash

sudo systemctl start zookeeper

echo "zookeeper runing..."

sudo systemctl start kafka
echo "KAFKA RUNINg ..."
sudo systemctl start mysql
echo "MYSQL RUNNING..."
sudo systemctl start grafana-server
echo " grafana running..."
