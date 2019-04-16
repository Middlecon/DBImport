#!/bin/bash

CONFIG_FILE=${DBIMPORT_HOME}/conf/dbimport.cfg
KEYTAB_FILE="$(cat $CONFIG_FILE | grep "^keytab=" | awk 'BEGIN{FS="="}{print $2}')"
KERBEROS_PRINCIPAL="$(cat $CONFIG_FILE | grep "^principal=" | awk 'BEGIN{FS="="}{print $2}')"

if [ "$1" == "-renew" ]; then
	echo "Renewing the kerberos ticket"
	kinit -R
	exit $?
fi

if [ "$1" == "-new" ]; then
	echo "Creating a kerberos ticket"
	kinit -kt $KEYTAB_FILE $KERBEROS_PRINCIPAL
	exit $?
fi

if [ "$1" == "-delete" ]; then
	echo "Removing the kerberos ticket"
	kdestroy
	exit $?
fi

klist -s
if [ $? -ne 0 ]; then
	# No initialy ticket generated.
	echo "Creating a kerberos ticket"
	kinit -kt $KEYTAB_FILE $KERBEROS_PRINCIPAL
fi

