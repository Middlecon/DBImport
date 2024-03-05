#!/bin/bash

source ~/python38.dbimport/bin/activate
export DBIMPORT_HOME=/usr/local/dbimport

$DBIMPORT_HOME/bin/manage $@
if [ $? -ne 0 ]; then
	echo "ERROR: Manage command was not successful"
