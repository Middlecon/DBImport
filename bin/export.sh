#!/bin/bash

source ~/python38.dbimport/bin/activate
export DBIMPORT_HOME=/usr/local/dbimport

$DBIMPORT_HOME/bin/export $@
if [ $? -ne 0 ]; then
	echo "ERROR: export command was not successful"
fi
