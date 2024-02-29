#!/bin/bash

# . ~/.bash_profile

source /home/hadoop/python38.dbimport/bin/activate
export DBIMPORT_HOME=/usr/local/dbimport
# export HADOOP_CLIENT_OPTS="-Xmx4g"

$DBIMPORT_HOME/bin/import $@
