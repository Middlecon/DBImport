#!/bin/bash

source ~/python38.dbimport/bin/activate
export DBIMPORT_HOME=/usr/local/dbimport

$DBIMPORT_HOME/bin/import $@
if [ $? -ne 0 ]; then
	echo "ERROR: import command was not successful"
