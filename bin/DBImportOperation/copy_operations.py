# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
import re
import logging
import subprocess
import errno, os, pty
import shlex
import fnmatch
import Crypto
import binascii
from subprocess import Popen, PIPE
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from common.Singleton import Singleton
from common import constants as constant
from DBImportConfig import import_config
from DBImportConfig import configSchema
from DBImportOperation import common_operations
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select
from sqlalchemy.orm import aliased, sessionmaker, Query


class operation(object, metaclass=Singleton):
	def __init__(self):
		logging.debug("Executing etl_operations.__init__()")
		self.Hive_DB = None
		self.Hive_Table = None
		self.startDate = None
		self.configDBSession = None
		self.instanceConfigDB = None
		self.instanceConfigDBSession = None
		self.copyDestinations = None

		self.common_operations = common_operations.operation()
		self.import_config = import_config.config()

		self.Hive_DB = self.common_operations.Hive_DB
		self.Hive_Table = self.common_operations.Hive_Table
		self.startDate = self.import_config.startDate

		logging.debug("Executing etl_operations.__init__() - Finished")

		self.import_config.common_config.connectSQLAlchemy()
		self.configDBSession = self.import_config.common_config.configDBSession
		self.crypto = self.import_config.common_config.crypto
		self.debugLogLevel = self.import_config.common_config.debugLogLevel
		
	def remove_temporary_files(self):
		self.import_config.remove_temporary_files()

	def checkDBImportInstance(self, instance):
		""" Checks if instance exists in the dbimport_instances table """

		session = self.configDBSession()
		dbimportInstances = aliased(configSchema.dbimportInstances)

		result = (session.query(
				dbimportInstances.name
			)
			.select_from(dbimportInstances)
			.filter(dbimportInstances.name == instance)
			.count())

		if result == 0:
			logging.error("No DBImport Instance with that name can be found in table 'dbimport_instances'")
			self.remove_temporary_files()
			sys.exit(1)

	def encryptUserPassword(self, instance, username, password):
		""" Encrypts the username and password and store them in the 'credentials column' of 'dbimport_instances' table """

		strToEncrypt = "%s %s\n"%(username, password)
		encryptedStr = self.crypto.encrypt(strToEncrypt)

		if encryptedStr == None or encryptedStr == "":
			logging.error("Cant encrypt username and password. Please contact support")
			self.remove_temporary_files()
			sys.exit(1)

		session = self.configDBSession()
		dbimportInstances = aliased(configSchema.dbimportInstances)

		DBImportInstance = (session.query(
				dbimportInstances
			)
			.filter(dbimportInstances.name == instance)
			.one())

		DBImportInstance.db_credentials = encryptedStr
		session.commit()

	def getCopyDestinations(self, hiveDB, hiveTable):
		""" Return the Copy Phase that the specified table have """

		session = self.configDBSession()
		copyTables = aliased(configSchema.copyTables)

		self.copyDestinations = []

		copyTablesResult = pd.DataFrame(session.query(copyTables.copy_id, copyTables.hive_filter, copyTables.destination, copyTables.data_transfer).all()).fillna('')
		for index, row in copyTablesResult.iterrows():
			if row['data_transfer'] == 'Asynchronous':
				logging.warning("Asynchronous transfer method is NOT supported yet")
				continue

			for hiveFilterSplit in row['hive_filter'].split(';'):

				if '.' not in hiveFilterSplit:
					logging.warning("The filter in table 'copy_tables' with copy_id = %s contains an invalid filter. Missing a . in the database.table format"%(row['copy_id']))
				else:
					filterDB = hiveFilterSplit.split('.')[0]
					filterTable = hiveFilterSplit.split('.')[1]
	
					if fnmatch.fnmatch(hiveDB, filterDB) and fnmatch.fnmatch(hiveTable, filterTable):
						if row['destination'] not in self.copyDestinations:
							self.copyDestinations.append(row['destination'])
	
		if self.copyDestinations == []:
			self.copyDestinations = None
		return self.copyDestinations


	def connectDBImportInstance(self, instance):
		""" Connects to the configuration database with SQLAlchemy """

		connectStatus = True
		session = self.configDBSession()
		dbimportInstances = aliased(configSchema.dbimportInstances)

		row = (session.query(
				dbimportInstances.db_hostname,
				dbimportInstances.db_port,
				dbimportInstances.db_database,
				dbimportInstances.db_credentials
			)
			.filter(dbimportInstances.name == instance)
			.one())

		if row[3] == None:
			logging.warning("There is no credentials saved in 'dbimport_instance' for %s"%(instance))
			return False

		try:
			db_credentials = self.crypto.decrypt(row[3])
		except binascii.Error as err:
			logging.warning("Decryption of credentials resulted in error with text: '%s'"%err)
			return False
		except:
			print("Unexpected warning: ")
			print(sys.exc_info())
			return False
			
		if db_credentials == None:
			logging.warning("Cant decrypt username and password. Check private/public key in config file")
			return False

		username = db_credentials.split(" ")[0]
		password = db_credentials.split(" ")[1]

		instanceConnectStr = "mysql+pymysql://%s:%s@%s:%s/%s"%(
			username,
			password,
			row[0],
			row[1],
			row[2])

		if self.instanceConfigDB != None:
			try:
				self.instanceConfigDB.dispose()
			except:
				print("Unexpected warning when closing connection to DBImport Instance database: ")
				print(sys.exc_info())

		try:
			self.instanceConfigDB = sa.create_engine(instanceConnectStr, echo = self.debugLogLevel)
			self.instanceConfigDB.connect()
			self.instanceConfigDBSession = sessionmaker(bind=self.instanceConfigDB)

		except sa.exc.OperationalError as err:
			logging.warning("%s"%err)
			connectStatus = False
#			self.remove_temporary_files()
#			sys.exit(1)
		except:
			print("Unexpected warning: ")
			print(sys.exc_info())
			connectStatus = False
#			self.remove_temporary_files()
#			sys.exit(1)

		return connectStatus

	def copyDataToDestinations(self):
		session = self.configDBSession()

		sourceHDFSaddress = self.common_operations.hdfs_address 
		sourceHDFSbasedir = self.common_operations.hdfs_basedir 

		if self.copyDestinations == None:	
			logging.warning("There are no destination for this table to receive a copy")
			return

		for destination in self.copyDestinations:
			if self.connectDBImportInstance(instance = destination):
				logging.info("Copy HDFS data to instance '%s'"%(destination))

				dbimportInstances = aliased(configSchema.dbimportInstances)

				row = (session.query(
						dbimportInstances.hdfs_address,
						dbimportInstances.hdfs_basedir
					)
					.filter(dbimportInstances.name == destination)
					.one())

				targetHDFSaddress = row[0]
				targetHDFSbasedir = row[1]

				sourceHDFSdir = (sourceHDFSbasedir + "/"+ self.Hive_DB + "/" + self.Hive_Table).replace('$', '').replace(' ', '')
				targetHDFSdir = (targetHDFSbasedir + "/"+ self.Hive_DB + "/" + self.Hive_Table).replace('$', '').replace(' ', '')

				distcpCommand = ["hadoop", "distcp", "-overwrite", "-delete", 
					"%s%s"%(sourceHDFSaddress, sourceHDFSdir),
					"%s%s"%(targetHDFSaddress, targetHDFSdir)]

				print(" ______________________ ")
				print("|                      |")
				print("| Hadoop distCp starts |")
				print("|______________________|")
				print("")


				# Start distcp
				sh_session = subprocess.Popen(distcpCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

				# Print Stdout and stderr while distcp is running
				while sh_session.poll() == None:
					row = sh_session.stdout.readline().decode('utf-8').rstrip()
					if row != "":
						print(row)
						sys.stdout.flush()

				# Print what is left in output after distcp is finished
				for row in sh_session.stdout.readlines():
					row = row.decode('utf-8').rstrip()
					if row != "":
						print(row)
						sys.stdout.flush()

				print(" _________________________ ")
				print("|                         |")
				print("| Hadoop distCp completed |")
				print("|_________________________|")
				print("")

	def copySchemaToDestinations(self):
		""" Copy the schema definitions to the target instances """
		localSession = self.configDBSession()

		if self.copyDestinations == None:	
			logging.warning("There are no destination for this table to receive a copy")
			return

		for destination in self.copyDestinations:
			if self.connectDBImportInstance(instance = destination):
				logging.info("Copy schema definitions to instance '%s'"%(destination))
				remoteSession = self.instanceConfigDBSession()

				jdbcConnections = aliased(configSchema.jdbcConnections)
				importTables = aliased(configSchema.importTables)
				importColumns = aliased(configSchema.importColumns)
				dbimportInstances = aliased(configSchema.dbimportInstances)

				# Check if the table exists on the remote DBImport instance
				result = (remoteSession.query(
						importTables
					)
					.filter(importTables.hive_db == self.import_config.Hive_DB)
					.filter(importTables.hive_table == self.import_config.Hive_Table)
					.count())

				if result == 0:
					newImportTable = configSchema.importTables(
						hive_db = self.import_config.Hive_DB,
						hive_table = self.import_config.Hive_Table,
						dbalias = '',
						source_schema = '',
						source_table = '')
					remoteSession.add(newImportTable)
					remoteSession.commit()

				# Get the table_id from the table at the remote instance
				remoteImportTableID = (remoteSession.query(
						importTables.table_id
					)
					.select_from(importTables)
					.filter(importTables.hive_db == self.import_config.Hive_DB)
					.filter(importTables.hive_table == self.import_config.Hive_Table)
					.one())

				remoteTableID =	remoteImportTableID[0]

				# Read the entire import_table row from the source database
				sourceTableDefinition = pd.DataFrame(localSession.query(configSchema.importTables.__table__)
					.filter(configSchema.importTables.table_id == self.import_config.table_id)
					)

				# Table to update with values from import_table source
				remoteTableDefinition = (remoteSession.query(configSchema.importTables.__table__)
					.filter(configSchema.importTables.table_id == remoteTableID)
					.one()
					)

				# Create dictonary to be used to update the values in import_table on the remote Instance
				updateDict = {}
				jdbcConnection = ""
				for name, values in sourceTableDefinition.iteritems():
					if name in ("table_id", "hive_db", "hive_table", "copy_finished", "copy_slave"):
						continue

					value = str(values[0])
					if value == "None":
						value = None

					if name == "dbalias":
						jdbcConnection = value

					updateDict["%s"%(name)] = value 
				updateDict["copy_finished"] = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')) 
				updateDict["copy_slave"] = 1


				# Update the values in import_table on the remote instance
				(remoteSession.query(configSchema.importTables)
					.filter(configSchema.importTables.table_id == remoteTableID)
					.update(updateDict))
				remoteSession.commit()

				##################################
				# Update jdbc_connections
				##################################

				# Check if the jdbcConnection exists on the remote DBImport instance
				result = (remoteSession.query(
						jdbcConnections
					)
					.filter(jdbcConnections.dbalias == jdbcConnection)
					.count())

				if result == 0:
					newJdbcConnection = configSchema.jdbcConnections(
						dbalias = jdbcConnection,
						jdbc_url = '')
					remoteSession.add(newJdbcConnection)
					remoteSession.commit()

				# Read the entire import_table row from the source database
				sourceJdbcConnection = pd.DataFrame(localSession.query(configSchema.jdbcConnections.__table__)
					.filter(configSchema.jdbcConnections.dbalias == jdbcConnection)
					)

				# Table to update with values from import_table source
				remoteJdbcConnection = (remoteSession.query(configSchema.jdbcConnections.__table__)
					.filter(configSchema.jdbcConnections.dbalias == jdbcConnection)
					.one()
					)

				# Create dictonary to be used to update the values in import_table on the remote Instance
				updateDict = {}
				for name, values in sourceJdbcConnection.iteritems():
					if name in ("dbalias", "credentials", "private_key_path", "public_key_path"):
						continue

					value = str(values[0])
					if value == "None":
						value = None

					updateDict["%s"%(name)] = value 


				# Update the values in import_table on the remote instance
				(remoteSession.query(configSchema.jdbcConnections)
					.filter(configSchema.jdbcConnections.dbalias == jdbcConnection)
					.update(updateDict))
				remoteSession.commit()

				##################################
				# Update import_colums 
				##################################

				# Read the entire import_table row from the source database
				sourceAllColumnDefinitions = pd.DataFrame(localSession.query(configSchema.importColumns.__table__)
					.filter(configSchema.importColumns.table_id == self.import_config.table_id)
					)

				for columnIndex, columnRow in sourceAllColumnDefinitions.iterrows():

					# Check if the column exists on the remote DBImport instance
					result = (remoteSession.query(
							importColumns
						)
						.filter(importColumns.table_id == remoteTableID)
						.filter(importColumns.source_column_name == columnRow['source_column_name'])
						.count())

					if result == 0:
						# Create a new row in importColumns if it doesnt exists
						newImportColumn = configSchema.importColumns(
							table_id = remoteTableID,
							column_name = columnRow['column_name'],
							hive_db = self.import_config.Hive_DB,
							hive_table = self.import_config.Hive_Table,
							source_column_name = columnRow['source_column_name'],
							column_type = '',
							source_column_type = '',
							last_update_from_source = str(columnRow['last_update_from_source']))
						remoteSession.add(newImportColumn)
						remoteSession.commit()

					# Get the table_id from the table at the remote instance
					remoteImportColumnID = (remoteSession.query(
							importColumns.column_id
						)
						.select_from(importColumns)
						.filter(importColumns.table_id == remoteTableID)
						.filter(importColumns.source_column_name == columnRow['source_column_name'])
						.one())
	
					remoteColumnID = remoteImportColumnID[0]

					# Read the entire import_columnis row from the source database
					sourceColumnDefinition = pd.DataFrame(localSession.query(configSchema.importColumns.__table__)
						.filter(configSchema.importColumns.column_id == columnRow['column_id'])
						)

					# Table to update with values from import_columns source
					remoteColumnDefinition = (remoteSession.query(configSchema.importColumns.__table__)
						.filter(configSchema.importColumns.column_id == remoteColumnID)
						.one()
						)

					# Create dictonary to be used to update the values in import_table on the remote Instance
					updateDict = {}
					for name, values in sourceColumnDefinition.iteritems():
						if name in ("table_id", "column_id", "source_column_name", "hive_db", "hive_table"):
							continue
	
						value = str(values[0])
						if value == "None":
							value = None
	
						updateDict["%s"%(name)] = value 

					# Update the values in import_table on the remote instance
					(remoteSession.query(configSchema.importColumns)
						.filter(configSchema.importColumns.column_id == remoteColumnID)
						.update(updateDict))
					remoteSession.commit()

#					# Table to update with values from import_table source
#					remoteTableDefinition = (remoteSession.query(configSchema.importTables.__table__)
#						.filter(configSchema.importTables.table_id == remoteTableID)
#						.one()
#						)
#					print(remoteColumnID)

			else:
				logging.warning("Connection failed! No data will be copied to instance '%s'"%(destination))

			






