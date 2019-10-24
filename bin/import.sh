#!/bin/bash

. ~/.bash_profile

export HADOOP_CLIENT_OPTS="-Xmx4g"

$DBIMPORT_HOME/bin/import $@
