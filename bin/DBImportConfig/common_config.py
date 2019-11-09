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

import os
import io
import sys
import logging
import subprocess 
import shutil
import jaydebeapi
import jpype
import base64
import re
import json
import ssl
import requests
from requests_kerberos import HTTPKerberosAuth
from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_OAEP
from subprocess import Popen, PIPE
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, time, timedelta
import pandas as pd
from sourceSchemaReader import schemaReader
from common.Singleton import Singleton
from common import constants as constant
from DBImportConfig import decryption as decryption
from common.Exceptions import *
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select
from sqlalchemy.orm import aliased, sessionmaker, Query


class config(object, metaclass=Singleton):
	def __init__(self, Hive_DB=None, Hive_Table=None):
		logging.debug("Executing common_config.__init__()")

		self.Hive_DB = Hive_DB
		self.Hive_Table = Hive_Table
		self.mysql_conn = None
		self.mysql_cursor = None
		self.tempdir = None
		self.startDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') 
		self.dbAlias = None

		# Variables used in lookupConnectionAlias
		self.db_mssql = False
		self.db_oracle = False
		self.db_mysql = False
		self.db_postgresql = False
		self.db_progress = False
		self.db_db2udb = False
		self.db_db2as400 = False
		self.db_mongodb = False
		self.jdbc_url = None
		self.jdbc_hostname = None
		self.jdbc_port = None
		self.jdbc_database = None
		self.jdbc_username = None
		self.jdbc_password = None
		self.jdbc_classpath = None
		self.jdbc_driver = None
		self.jdbc_driver_for_python = None
		self.jdbc_classpath_for_python = None
		self.jdbc_ad_domain = None
		self.jdbc_encrypt = None
		self.jdbc_encrypt_string = None
		self.jdbc_trustedservercert = None
		self.jdbc_trustedservercert_password = None
		self.jdbc_hostincert = None
		self.jdbc_logintimeout = None
		self.jdbc_servertype = None
		self.jdbc_oracle_sid = None
		self.jdbc_oracle_servicename = None
		self.jdbc_force_column_lowercase = None
		self.kerberosInitiated = False

		self.sparkPathAppend = None
		self.sparkPysparkSubmitArgs = None
		self.sparkJarFiles = None
		self.sparkPyFiles = None
		self.sparkMaster = None
		self.sparkDeployMode = None
		self.sparkYarnQueue = None
		self.sparkDynamicAllocation = None
		self.sparkMinExecutors = None
		self.sparkMaxExecutors = None
		self.sparkExecutorMemory = None
		self.sparkHDPversion = None
		self.sparkHiveLibrary = None

		self.atlasEnabled = False
		self.atlasSchemaChecked = False
		self.atlasAddress = None
		self.atlasHeaders = None
		self.atlasTimeout = None
		self.atlasSSLverify = None
		self.atlasRestTypeDefDBImportProcess = None
		self.atlasRestEntities = None
		self.atlasRestUniqueAttributeType = None

		self.sourceSchema = None

		self.source_columns_df = pd.DataFrame()
		self.source_keys_df = pd.DataFrame()

		self.JDBCConn = None
		self.JDBCCursor = None

		self.sourceSchema = schemaReader.source()

		self.debugLogLevel = False
		if logging.root.level == 10:        # DEBUG
			self.debugLogLevel = True

		self.kerberosPrincipal = configuration.get("Kerberos", "principal")
		self.kerberosKeytab = configuration.get("Kerberos", "keytab")

		self.sparkPathAppend = configuration.get("Spark", "path_append")
		self.sparkJarFiles = configuration.get("Spark", "jar_files")
		self.sparkPyFiles = configuration.get("Spark", "py_files")
		self.sparkMaster = configuration.get("Spark", "master")
		self.sparkDeployMode = configuration.get("Spark", "deployMode")
		self.sparkYarnQueue = configuration.get("Spark", "yarnqueue")
		self.sparkExecutorMemory = configuration.get("Spark", "executor_memory")
		self.sparkHDPversion = configuration.get("Spark", "hdp_version")
		self.sparkHiveLibrary = configuration.get("Spark", "hive_library")

		if configuration.get("Spark", "dynamic_allocation").lower() == "true":
			self.sparkDynamicAllocation = True
		else:
			self.sparkDynamicAllocation = False

		self.crypto = decryption.crypto()
		self.crypto.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
		self.crypto.setPublicKeyFile(configuration.get("Credentials", "public_key"))

		# Get Atlas config
		self.atlasAddress = configuration.get("Atlas", "address")
		if self.atlasAddress != None and self.atlasAddress.strip() != "": 
			self.atlasRestTypeDefDBImportProcess = "%s/api/atlas/v2/types/typedef/name/DBImport_Process"%(self.atlasAddress)
			self.atlasRestEntities = "%s/api/atlas/v2/entity/bulk"%(self.atlasAddress)
			self.atlasRestUniqueAttributeType = "%s/api/atlas/v2/entity/uniqueAttribute/type"%(self.atlasAddress)
			self.atlasEnabled = True
			self.atlasHeaders = {'Content-type': 'application/json', 'Accept': 'application/json'}
			try:
				self.atlasTimeout = int(configuration.get("Atlas", "timeout"))
			except ValueError:
				self.atlasTimeout = 5
				logging.warning("Atlas timeout configuration does not contain a valid number. Setting the timeout to 5 seconds")
			if configuration.get("Atlas", "ssl_verify").lower() == "false":
				self.atlasSSLverify = False
			else:
				if configuration.get("Atlas", "ssl_cert_path").strip() != "":
					self.atlasSSLverify = configuration.get("Atlas", "ssl_cert_path").strip()
				else:
					self.atlasSSLverify = True

		# Sets and create a temporary directory
		self.tempdir = "/tmp/dbimport." + str(os.getpid()) + ".tmp"
		try:
			os.mkdir(self.tempdir)
		except OSError: 
			logging.error("Creation of the temporary directory %s failed" % self.tempdir)
			sys.exit(1)

		try:
			os.chmod(self.tempdir, 0o700)
		except OSError:
			logging.error("Error while changing permission on %s to 700" % self.tempdir)
			self.remove_temporary_files()
			sys.exit(1)

#		# Fetch configuration about HDFS
#		self.hdfs_address = configuration.get("HDFS", "hdfs_address")

		# Fetch configuration about MySQL database and how to connect to it
		mysql_hostname = configuration.get("Database", "mysql_hostname")
		mysql_port =     configuration.get("Database", "mysql_port")
		mysql_database = configuration.get("Database", "mysql_database")
		mysql_username = configuration.get("Database", "mysql_username")
		mysql_password = configuration.get("Database", "mysql_password")

		if configuration.get("REST_statistics", "post_column_data").lower() == "true":
			self.post_column_data = True
		else:
			self.post_column_data = False

		# Esablish a connection to the DBImport database in MySQL
		try:
			self.mysql_conn = mysql.connector.connect(host=mysql_hostname, 
												 port=mysql_port, 
												 database=mysql_database, 
												 user=mysql_username, 
												 password=mysql_password)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				logging.error("Something is wrong with your user name or password")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				logging.error("Database does not exist")
			else:
				logging.error("%s"%err)
			logging.error("Error: There was a problem connecting to the MySQL database. Please check configuration and serverstatus and try again")
			self.remove_temporary_files()
			sys.exit(1)
		else:
			self.mysql_cursor = self.mysql_conn.cursor(buffered=True)

		logging.debug("startDate = %s"%(self.startDate))
		logging.debug("Executing common_config.__init__() - Finished")

	def getAtlasJdbcConnectionData(self, dbAlias=None):
		""" Reads the extended information in jdbc_connections needed by Atlas (contact_info, owner and more) """
		logging.debug("Executing common_config.getAtlasJdbcConnectionData()")
	
		if dbAlias == None:
			dbAlias = self.dbAlias

		# Fetch data from jdbc_connection table
		query = "select contact_info, description, owner from jdbc_connections where dbalias = %s "
		logging.debug("Executing the following SQL: %s" % (query))
		self.mysql_cursor.execute(query, (dbAlias, ))

		if self.mysql_cursor.rowcount != 1:
			logging.error("Error: Number of rows returned from query on 'jdbc_connections' was not one.")
			logging.error("Rows returned: %d" % (self.mysql_cursor.rowcount) )
			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor.statement) )
			raise Exception

		row = self.mysql_cursor.fetchone()
		returnDict = {}
		returnDict["contact_info"] = row[0]
		returnDict["description"] = row[1]
		returnDict["owner"] = row[2]

		if returnDict["contact_info"] == None:	returnDict["contact_info"] = ""
		if returnDict["description"] == None:	returnDict["description"] = ""
		if returnDict["owner"] == None:			returnDict["owner"] = ""

		logging.debug("Executing common_config.getAtlasJdbcConnectionData() - Finished")
		return returnDict

	def getAtlasRdbmsColumnURI(self, schemaName, tableName, columnName):
		""" Returns a string with the correct column URI for Atlas """

		if self.atlasEnabled == False:
			return None

		if self.db_oracle == True:
			if self.jdbc_oracle_sid != None and self.jdbc_oracle_sid != "None": oracleDB = self.jdbc_oracle_sid
			if self.jdbc_oracle_servicename != None and self.jdbc_oracle_servicename != "None": oracleDB = self.jdbc_oracle_servicename
			columnUri = "%s.%s.%s.%s@%s:%s"%(oracleDB, schemaName, tableName, columnName, self.jdbc_hostname, self.jdbc_port)

		elif self.db_mssql == True:
			columnUri = "%s.%s.%s.%s@%s:%s"%(self.jdbc_database, schemaName, tableName, columnName, self.jdbc_hostname, self.jdbc_port)

		elif self.db_mysql == True:
			columnUri = "%s.%s.%s@%s:%s"%(self.jdbc_database, tableName, columnName, self.jdbc_hostname, self.jdbc_port)

		elif self.db_postgresql == True:
			columnUri = "%s.%s.%s.%s@%s:%s"%(self.jdbc_database, schemaName, tableName, columnName, self.jdbc_hostname, self.jdbc_port)

		elif self.db_progress == True:
			columnUri = "%s.%s.%s.%s@%s:%s"%(self.jdbc_database, schemaName, tableName, columnName, self.jdbc_hostname, self.jdbc_port)

		elif self.db_db2udb == True:
			columnUri = "%s.%s.%s.%s@%s:%s"%(self.jdbc_database, schemaName, tableName, columnName, self.jdbc_hostname, self.jdbc_port)

		elif self.db_db2as400 == True:
			columnUri = "%s.%s.%s.%s@%s:%s"%(self.jdbc_database, schemaName, tableName, columnName, self.jdbc_hostname, self.jdbc_port)

		else:
			logging.warning("The Atlas integrations does not support this database type. Atlas integration disabled")
			self.atlasEnabled = False
			return None

		return columnUri

	def getAtlasRdbmsNames(self, schemaName, tableName):
		""" Returns a dict with the names needed for the rdbms_* configuration, depending on database type """

		tableUri = None
		dbUri = None
		instanceUri = None
		instanceName = None
		instanceType = None

		if self.atlasEnabled == False:
			return None

		if self.db_oracle == True:
			if self.jdbc_oracle_sid != None and self.jdbc_oracle_sid != "None": oracleDB = self.jdbc_oracle_sid
			if self.jdbc_oracle_servicename != None and self.jdbc_oracle_servicename != "None": oracleDB = self.jdbc_oracle_servicename

			tableUri = "%s.%s.%s@%s:%s"%(oracleDB, schemaName, tableName, self.jdbc_hostname, self.jdbc_port)
			dbUri = "%s@%s:%s"%(oracleDB, self.jdbc_hostname, self.jdbc_port)
			dbName = oracleDB
			instanceUri = "Oracle@%s:%s"%(self.jdbc_hostname, self.jdbc_port)
			instanceName = "Oracle"
			instanceType = "Oracle"

		elif self.db_mssql == True:
			tableUri = "%s.%s.%s@%s:%s"%(self.jdbc_database, schemaName, tableName, self.jdbc_hostname, self.jdbc_port)
			dbUri = "%s@%s:%s"%(self.jdbc_database, self.jdbc_hostname, self.jdbc_port)
			dbName = self.jdbc_database
			instanceUri = "MSSQL@%s:%s"%(self.jdbc_hostname, self.jdbc_port)
			instanceName = "MSSQL"
			instanceType = "MSSQL"

		elif self.db_mysql == True:
			tableUri = "%s.%s@%s:%s"%(self.jdbc_database, tableName, self.jdbc_hostname, self.jdbc_port)
			dbUri = "%s@%s:%s"%(self.jdbc_database, self.jdbc_hostname, self.jdbc_port)
			dbName = self.jdbc_database
			instanceUri = "MySQL@%s:%s"%(self.jdbc_hostname, self.jdbc_port)
			instanceName = "MySQL"
			instanceType = "MySQL"

		elif self.db_postgresql == True:
			tableUri = "%s.%s.%s@%s:%s"%(self.jdbc_database, schemaName, tableName, self.jdbc_hostname, self.jdbc_port)
			dbUri = "%s@%s:%s"%(self.jdbc_database, self.jdbc_hostname, self.jdbc_port)
			dbName = self.jdbc_database
			instanceUri = "PostgreSQL@%s:%s"%(self.jdbc_hostname, self.jdbc_port)
			instanceName = "PostgreSQL"
			instanceType = "PostgreSQL"

		elif self.db_progress == True:
			tableUri = "%s.%s.%s@%s:%s"%(self.jdbc_database, schemaName, tableName, self.jdbc_hostname, self.jdbc_port)
			dbUri = "%s@%s:%s"%(self.jdbc_database, self.jdbc_hostname, self.jdbc_port)
			dbName = self.jdbc_database
			instanceUri = "OpenEdge@%s:%s"%(self.jdbc_hostname, self.jdbc_port)
			instanceName = "OpenEdge"
			instanceType = "OpenEdge"

		elif self.db_db2udb == True:
			tableUri = "%s.%s.%s@%s:%s"%(self.jdbc_database, schemaName, tableName, self.jdbc_hostname, self.jdbc_port)
			dbUri = "%s@%s:%s"%(self.jdbc_database, self.jdbc_hostname, self.jdbc_port)
			dbName = self.jdbc_database
			instanceUri = "DB2UDB@%s:%s"%(self.jdbc_hostname, self.jdbc_port)
			instanceName = "DB2"
			instanceType = "DB2"

		elif self.db_db2as400 == True:
			tableUri = "%s.%s.%s@%s:%s"%(self.jdbc_database, schemaName, tableName, self.jdbc_hostname, self.jdbc_port)
			dbUri = "%s@%s:%s"%(self.jdbc_database, self.jdbc_hostname, self.jdbc_port)
			dbName = self.jdbc_database
			instanceUri = "DB2AS400@%s:%s"%(self.jdbc_hostname, self.jdbc_port)
			instanceName = "DB2"
			instanceType = "DB2"

		else:
			logging.warning("The Atlas integrations does not support this database type. Atlas integration disabled")
			self.atlasEnabled = False
			return None

		returnDict = {}
		returnDict["tableUri"] = tableUri
		returnDict["dbUri"] = dbUri
		returnDict["dbName"] = dbName
		returnDict["instanceUri"] = instanceUri
		returnDict["instanceName"] = instanceName
		returnDict["instanceType"] = instanceType

		return returnDict


	def getAtlasRdbmsReferredEntities(self, schemaName, tableName, tableComment):
		""" Returns a dict that contains the referredEntities part for the RDBMS update rest call """
		logging.debug("Executing common_config.getAtlasReferredEntities()")

		if self.atlasEnabled == False:
			return None

		returnDict = self.getAtlasRdbmsNames(schemaName = schemaName, tableName = tableName)
		if returnDict == None:
			return

		tableUri = returnDict["tableUri"]
		dbUri = returnDict["dbUri"]
		dbName = returnDict["dbName"]
		instanceUri = returnDict["instanceUri"]
		instanceName = returnDict["instanceName"]
		instanceType = returnDict["instanceType"]

		# TODO: Get the following information from dbimport and source system
		# create_time from source database on table
		# Foreign Keys

		# Get extended data from jdbc_connections table
		jdbcConnectionDict = self.getAtlasJdbcConnectionData()
		contactInfo = jdbcConnectionDict["contact_info"]
		description = jdbcConnectionDict["description"]
		owner = jdbcConnectionDict["owner"]

		jsonData = {}
		jsonData["referredEntities"] = {}
		jsonData["referredEntities"]["-100"] = {}
		jsonData["referredEntities"]["-100"]["guid"] = "-100"
		jsonData["referredEntities"]["-100"]["typeName"] = "rdbms_table"
		jsonData["referredEntities"]["-100"]["attributes"] = {}
		jsonData["referredEntities"]["-100"]["attributes"]["qualifiedName"] = tableUri
		jsonData["referredEntities"]["-100"]["attributes"]["uri"] = tableUri
		jsonData["referredEntities"]["-100"]["attributes"]["name"] = tableName
		jsonData["referredEntities"]["-100"]["attributes"]["contact_info"] = contactInfo
		jsonData["referredEntities"]["-100"]["attributes"]["owner"] = owner
		jsonData["referredEntities"]["-100"]["attributes"]["comment"] = tableComment
		jsonData["referredEntities"]["-100"]["attributes"]["db"] = { "guid": "-300", "typeName": "rdbms_db" }
		jsonData["referredEntities"]["-300"] = {}
		jsonData["referredEntities"]["-300"]["guid"] = "-300"
		jsonData["referredEntities"]["-300"]["typeName"] = "rdbms_db"
		jsonData["referredEntities"]["-300"]["attributes"] = {}
		jsonData["referredEntities"]["-300"]["attributes"]["qualifiedName"] = dbUri
		jsonData["referredEntities"]["-300"]["attributes"]["uri"] = dbUri
		jsonData["referredEntities"]["-300"]["attributes"]["name"] = dbName
		jsonData["referredEntities"]["-300"]["attributes"]["contact_info"] = contactInfo
		jsonData["referredEntities"]["-300"]["attributes"]["owner"] = owner
		jsonData["referredEntities"]["-300"]["attributes"]["description"] = description
		jsonData["referredEntities"]["-300"]["attributes"]["instance"] = { "guid": "-400", "typeName": "rdbms_instance" }
		jsonData["referredEntities"]["-400"] = {}
		jsonData["referredEntities"]["-400"]["guid"] = "-400"
		jsonData["referredEntities"]["-400"]["typeName"] = "rdbms_instance"
		jsonData["referredEntities"]["-400"]["attributes"] = {}
		jsonData["referredEntities"]["-400"]["attributes"]["qualifiedName"] = instanceUri
		jsonData["referredEntities"]["-400"]["attributes"]["uri"] = instanceUri
		jsonData["referredEntities"]["-400"]["attributes"]["name"] = instanceName
		jsonData["referredEntities"]["-400"]["attributes"]["rdbms_type"] = instanceType
		jsonData["referredEntities"]["-400"]["attributes"]["hostname"] = self.jdbc_hostname
		jsonData["referredEntities"]["-400"]["attributes"]["port"] = self.jdbc_port
		jsonData["referredEntities"]["-400"]["attributes"]["protocol"] = "jdbc"

		return jsonData

		logging.debug("Executing common_config.getAtlasReferredEntities() - Finished")

	def checkAtlasSchema(self):
		""" Checks if the Atlas schema for DBImport is available and at the correct version. Returns True or False """
		logging.debug("Executing common_config.checkAtlasSchema()")

		# We only check the Atlas schema ones per execution. This will otherwise add more requests to Atlas for the same data
		if self.atlasSchemaChecked == True:
			return self.atlasEnabled

		if self.atlasEnabled == True:
			logging.info("Checking Atlas connection")
			response = self.atlasGetData(URL=self.atlasRestTypeDefDBImportProcess)
			statusCode = response["statusCode"]
			responseData = json.loads(response["data"])

		if self.atlasEnabled == True and statusCode == 404:
			# The TypeDef was not found
			logging.warning("The Atlas typeDef 'DBImport_Process' was not found. Did you run the Atlas setup for DBImport?")
			self.atlasEnabled = False

		elif self.atlasEnabled == True and statusCode != 200:
			logging.warning("Atlas communication failed with response %s"%(statusCode))
			self.atlasEnabled = False

		if self.atlasEnabled == True:
			# if self.atlasEnabled = True at this stage, it means that we have a valid json response in DBImportProcessJSON
			typeDefVersion = responseData["typeVersion"]
			if typeDefVersion != "1.2":
				logging.warning("The version for Atlas typeDef 'DBImport_Process' is not correct. Version required is 1.2 and version found is %s"%(typeDefVersion))
				logging.warning("Atlas integration is Disabled")
				self.atlasEnabled = False
			else:
				self.atlasSchemaChecked = True

		return self.atlasEnabled

		logging.debug("Executing common_config.checkAtlasSchema() - Finished")

	def atlasGetData(self, URL):
		logging.debug("Executing common_config.atlasGetData()")
		return self.atlasCommunicate(URL=URL, requestType="GET")

	def atlasPostData(self, URL, data):
		logging.debug("Executing common_config.atlasPostData()")
		return self.atlasCommunicate(URL=URL, requestType="POST", data=data)

	def atlasDeleteData(self, URL):
		logging.debug("Executing common_config.atlasDeleteData()")
		return self.atlasCommunicate(URL=URL, requestType="DELETE")

	def atlasCommunicate(self, URL, requestType, data=None):
		logging.debug("Executing common_config.atlasCommunicate()")

		returnValue = None
		if self.atlasEnabled == True:
			try:
				if requestType == "POST":
					response = requests.post(URL,
						headers=self.atlasHeaders,
						data=data,
						timeout=self.atlasTimeout,
						auth=HTTPKerberosAuth(),
						verify=self.atlasSSLverify)

				elif requestType == "GET":
					response = requests.get(URL,
						headers=self.atlasHeaders,
						timeout=self.atlasTimeout,
						auth=HTTPKerberosAuth(),
						verify=self.atlasSSLverify)

				elif requestType == "DELETE":
					response = requests.delete(URL,
						headers=self.atlasHeaders,
						timeout=self.atlasTimeout,
						auth=HTTPKerberosAuth(),
						verify=self.atlasSSLverify)

				else:
					raise ValueError 

				returnValue = { "statusCode": response.status_code, "data": response.text }
				logging.debug("Atlas statusCode = %s"%(response.status_code))
				logging.debug("Atlas data = %s"%(response.text))

			except requests.exceptions.ConnectionError:
				logging.error("Connection error when communicating with Atlas on %s"%(URL))
				logging.warning("Atlas integration is Disabled")
				self.common_config.atlasEnabled = False
			except requests.exceptions.HTTPError:
				logging.error("HTTP error when communicating with Atlas on %s"%(URL))
				logging.warning("Atlas integration is Disabled")
				self.common_config.atlasEnabled = False
			except requests.exceptions.RequestException:
				logging.error("RequestException error when communicating with Atlas on %s"%(URL))
				logging.warning("Atlas integration is Disabled")
				self.common_config.atlasEnabled = False
			except requests.exceptions.SSLError:
				logging.error("Atlas connection failed due to SSL validation. Please check Atlas connection configuration in config file")
				logging.warning("Atlas integration is Disabled")
				self.atlasEnabled = False
			except ValueError:
				logging.error("The used requstType (%s) for atlasCommunicate() is not supported"%(requestType))
				logging.warning("Atlas integration is Disabled")
				self.atlasEnabled = False
#			else:
#				returnValue = response.text

		logging.debug("Executing common_config.atlasCommunicate() - Finished")
		return returnValue

	def connectSQLAlchemy(self):
		""" Connects to the configuration database with SQLAlchemy """

		self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s"%(
			configuration.get("Database", "mysql_username"),
			configuration.get("Database", "mysql_password"),
			configuration.get("Database", "mysql_hostname"),
			configuration.get("Database", "mysql_port"),
			configuration.get("Database", "mysql_database"))

		try:
			self.configDB = sa.create_engine(self.connectStr, echo = self.debugLogLevel)
			self.configDB.connect()
			self.configDBSession = sessionmaker(bind=self.configDB)

		except sa.exc.OperationalError as err:
			logging.error("%s"%err)
			self.common_config.remove_temporary_files()
			sys.exit(1)
		except:
			print("Unexpected error: ")
			print(sys.exc_info())
			self.common_config.remove_temporary_files()
			sys.exit(1)

	def setHiveTable(self, Hive_DB, Hive_Table):
		""" Sets the parameters to work against a new Hive database and table """
		self.Hive_DB = Hive_DB.lower()
		self.Hive_Table = Hive_Table.lower()

	def getMysqlCursor(self):
		return self.mysql_cursor

	def getMysqlConnector(self):
		return self.mysql_conn

	def remove_temporary_files(self):
		logging.debug("Executing common_config.remove_temporary_files()")

		# Remove the kerberos ticket file
		if self.kerberosInitiated == True:
			klistCommandList = ['kdestroy']
			klistProc = subprocess.Popen(klistCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			stdOut, stdErr = klistProc.communicate()

		# We add this check just to make sure that self.tempdir contains a valid path and that no exception forces us to remove example /
		if len(self.tempdir) > 13 and self.tempdir.startswith( '/tmp/' ):
			try:
				shutil.rmtree(self.tempdir)
			except FileNotFoundError:
				# This can happen as we might call this function multiple times during an error and we dont want an exception because of that
				pass

		logging.debug("Executing common_config.remove_temporary_files() - Finished")

	def checkTimeWindow(self, connection_alias):
		logging.debug("Executing common_config.checkTimeWindow()")
		logging.info("Checking if we are allowed to use this jdbc connection at this time")

		query = "select timewindow_start, timewindow_stop from jdbc_connections where dbalias = %s"
		self.mysql_cursor.execute(query, (connection_alias, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		row = self.mysql_cursor.fetchone()
		currentTime = str(datetime.now().strftime('%H:%M:%S'))
		timeWindowStart = None
		timeWindowStop = None

		if row[0] != None: timeWindowStart = str(row[0])
		if row[1] != None: timeWindowStop = str(row[1])

		if timeWindowStart != None and re.search('^[0-9]:', timeWindowStart): timeWindowStart = "0" + timeWindowStart
		if timeWindowStop  != None and re.search('^[0-9]:', timeWindowStop):  timeWindowStop  = "0" + timeWindowStop

		if timeWindowStart == None and timeWindowStop == None:
			logging.info("SUCCESSFUL: This import is allowed to run at any time during the day.")
			return
		elif timeWindowStart == None or timeWindowStop == None: 
			logging.error("Atleast one of the TimeWindow settings are NULL in the database. Only way to disable the Time Window")
			logging.error("function is to put NULL into both columns. Otherwise the configuration is marked as invalid and will exit")
			logging.error("as it's not running inside a correct Time Window.")
			logging.error("Invalid TimeWindow configuration")
			self.remove_temporary_files()
			sys.exit(1)
		elif timeWindowStart > timeWindowStop:
			logging.error("The value in timewindow_start column is larger than the value in timewindow_stop.")
			logging.error("Invalid TimeWindow configuration")
			self.remove_temporary_files()
			sys.exit(1)
		elif timeWindowStart == timeWindowStop:
			logging.error("The value in timewindow_start column is the same as the value in timewindow_stop.")
			logging.error("Invalid TimeWindow configuration")
			self.remove_temporary_files()
			sys.exit(1)
		elif currentTime < timeWindowStart or currentTime > timeWindowStop:
			logging.error("We are not allowed to run this import outside the configured Time Window")
			logging.info("    Current time:     %s"%(currentTime))
			logging.info("    TimeWindow Start: %s"%(timeWindowStart))
			logging.info("    TimeWindow Stop:  %s"%(timeWindowStop))
			self.remove_temporary_files()
			sys.exit(1)
		else:		
			logging.info("SUCCESSFUL: There is a configured Time Window for this operation, and we are running inside that window.")
 
		logging.debug("    currentTime = %s"%(currentTime))
		logging.debug("    timeWindowStart = %s"%(timeWindowStart))
		logging.debug("    timeWindowStop = %s"%(timeWindowStop))
		logging.debug("Executing common_config.checkTimeWindow() - Finished")

	def checkConnectionAlias(self, connection_alias):
		""" Will check if the connection alias exists in the jdbc_connection table """
		logging.debug("Executing common_config.checkConnectionAlias()")

		RC = False

		query = "select count(1) from jdbc_connections where dbalias = %s "
		logging.debug("Executing the following SQL: %s" % (query))
		self.mysql_cursor.execute(query, (connection_alias, ))

		row = self.mysql_cursor.fetchone()
		if row[0] == 1:
			RC = True

		logging.debug("Executing common_config.checkConnectionAlias() - Finished")
		return RC

	def checkKerberosTicket(self):
		""" Checks if there is a valid kerberos ticket or not. Runst 'klist -s' and checks the exitCode. Returns True or False """
		logging.debug("Executing common_config.checkKerberosTicket()")

		os.environ["KRB5CCNAME"] = "FILE:/tmp/krb5cc_%s_%s"%(os.getuid(), os.getpid())

		if self.kerberosPrincipal == "" or self.kerberosKeytab == "":
			logging.error("The kerberos information is not correct in configuration file.")
			self.remove_temporary_files()
			sys.exit(1)

		logging.info("Initialize Kerberos ticket")
		self.kerberosInitiated = True

		kinitCommandList = ['kinit', '-kt', self.kerberosKeytab, self.kerberosPrincipal]
		kinitProc = subprocess.Popen(kinitCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdOut, stdErr = kinitProc.communicate()
		stdOut = stdOut.decode('utf-8').rstrip()
		stdErr = stdErr.decode('utf-8').rstrip()

		klistCommandList = ['klist', '-s']
		klistProc = subprocess.Popen(klistCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdOut, stdErr = klistProc.communicate()
		stdOut = stdOut.decode('utf-8').rstrip()
		stdErr = stdErr.decode('utf-8').rstrip()

		if klistProc.returncode == 0:
			return True
		else:
			return False

		logging.debug("Executing common_config.checkKerberosTicket() - Finished")


	def encryptUserPassword(self, connection_alias, username, password):

		strToEncrypt = "%s %s\n"%(username, password)
		encryptedStr = self.crypto.encrypt(strToEncrypt)

		if encryptedStr != None and encryptedStr != "":
			query = "update jdbc_connections set credentials = %s where dbalias = %s "
			logging.debug("Executing the following SQL: %s" % (query))
			self.mysql_cursor.execute(query, (encryptedStr, connection_alias))
			self.mysql_conn.commit()

#		decryptedStr = self.crypto.decrypt(encryptedStr)
#
#		print("DECRYPTED VALUE USING STANDARD DECODING")
#		print(decryptedStr)

	def getJDBCDriverConfig(self, databaseType, version):
		logging.debug("Executing common_config.getJDBCDriverConfig()")
		query = "select driver, classpath from jdbc_connections_drivers where database_type = %s and version = %s"
		logging.debug("Executing the following SQL: %s" % (query))
		self.mysql_cursor.execute(query, (databaseType, version ))

		if self.mysql_cursor.rowcount != 1:
			raise invalidConfiguration("Error: Cant find JDBC driver with database_type as '%s' and version as '%s'"%(databaseType, version))

		row = self.mysql_cursor.fetchone()
		driver = row[0]
		classPath = row[1]

		if classPath == "add path to JAR file" or not classPath.startswith("/"):
			raise invalidConfiguration("Error: You need to specify the full path to the JAR files in the table 'jdbc_connections_drivers'")

		logging.debug("Executing common_config.getJDBCDriverConfig() - Finished")
		return driver, classPath

	def lookupConnectionAlias(self, connection_alias, decryptCredentials=True, copySlave=False):
		logging.debug("Executing common_config.lookupConnectionAlias()")
	
		exit_after_function = False
		self.dbAlias = connection_alias

		# Fetch data from jdbc_connection table
		query = "select jdbc_url, credentials from jdbc_connections where dbalias = %s "
		logging.debug("Executing the following SQL: %s" % (query))
		self.mysql_cursor.execute(query, (connection_alias, ))

		if self.mysql_cursor.rowcount != 1:
			logging.error("Error: Number of rows returned from query on 'jdbc_connections' was not one.")
			logging.error("Rows returned: %d" % (self.mysql_cursor.rowcount) )
			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor.statement) )
			raise Exception

		row = self.mysql_cursor.fetchone()

		self.jdbc_url = row[0]

#		if row[1] == "DBImport Slave Connection":
#			decryptCredentials = False

		if decryptCredentials == True and copySlave == False:
			credentials = self.crypto.decrypt(row[1])
			if credentials == None:
				logging.error("Cant decrypt username and password. Check private/public key in config file")
				raise Exception
		
			self.jdbc_username = credentials.split(" ")[0]
			self.jdbc_password = credentials.split(" ")[1]

			# Sets a creates the password file that is used by sqoop and other tools
			self.jdbc_password_file = self.tempdir + "/jdbc_passwd"
			f = open(self.jdbc_password_file, "w")
			f.write(self.jdbc_password)
			f.close()
			os.chmod(self.jdbc_password_file, 0o600)
		else:
			self.jdbc_username = None
			self.jdbc_password = None
			self.jdbc_password_file = None

		# Lookup Connection details based on JDBC STRING for all different types we support
		if self.jdbc_url.startswith( 'jdbc:sqlserver:'): 
			self.db_mssql = True
			self.jdbc_servertype = constant.MSSQL
			self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("SQL Server", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath
			self.jdbc_hostname = self.jdbc_url[17:].split(':')[0].split(';')[0]

			try:
				self.jdbc_port = self.jdbc_url[17:].split(':')[1].split(';')[0]
			except:
				self.jdbc_port = "1433"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "1433"

			try:
				self.jdbc_database = self.jdbc_url.split("database=")[1].split(';')[0]
			except:
				logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True

			try:
				self.jdbc_encrypt = self.jdbc_url.split("encrypt=")[1].split(';')[0].lower()
				if self.jdbc_encrypt == "true": self.jdbc_encrypt = True
			except:
				self.jdbc_encrypt = False

			try:
				self.jdbc_trustedservercert = self.jdbc_url.split("trustServerCertificate=")[1].split(';')[0]
			except:
				self.jdbc_trustedservercert = None

			try:
				self.jdbc_hostincert = self.jdbc_url.split("hostNameInCertificate=")[1].split(';')[0]
			except:
				self.jdbc_hostincert = None

			try:
				self.jdbc_logintimeout = self.jdbc_url.split("loginTimeout=")[1].split(';')[0]
			except:
				self.jdbc_logintimeout = None

			# If all encrypt settings are available, we create an encryption string that will be used by the Schema Python Program
			if self.jdbc_encrypt == True and self.jdbc_trustedservercert != None and self.jdbc_hostincert != None and self.jdbc_logintimeout != None:
				self.jdbc_encrypt_string = "encrypt=true;trustServerCertificate=" + self.jdbc_trustedservercert + ";hostNameInCertificate=" + self.jdbc_hostincert + ";loginTimeout=" + self.jdbc_logintimeout


		if self.jdbc_url.startswith( 'jdbc:jtds:sqlserver:'): 
			self.db_mssql = True
			self.jdbc_servertype = constant.MSSQL
			self.jdbc_force_column_lowercase = True
			self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("SQL Server", "jTDS")
			self.jdbc_classpath_for_python = self.jdbc_classpath
			self.jdbc_hostname = self.jdbc_url[22:].split(':')[0].split(';')[0]
			try:
				self.jdbc_port = self.jdbc_url[22:].split(':')[1].split(';')[0]
			except:
				self.jdbc_port = "1433"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "1433"

			try:
				self.jdbc_database = self.jdbc_url.split("databaseName=")[1].split(';')[0]
			except:
				logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:oracle:'): 
			self.db_oracle = True
			self.jdbc_servertype = constant.ORACLE
			self.jdbc_force_column_lowercase = False
			self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("Oracle", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath
			self.jdbc_database = "-"

			try:
				self.jdbc_hostname = self.jdbc_url.split("(HOST=")[1].split(')')[0]
			except:
				logging.error("Cant determine hostname based on jdbc_string")
				exit_after_function = True

			try:
				self.jdbc_port = self.jdbc_url.split("(PORT=")[1].split(')')[0]
			except:
				logging.error("Cant determine port based on jdbc_string")
				exit_after_function = True

			try:
				self.jdbc_oracle_sid = self.jdbc_url.split("(SID=")[1].split(')')[0]
			except:
				self.jdbc_oracle_sid = None

			try:
				self.jdbc_oracle_servicename = self.jdbc_url.split("(SERVICE_NAME=")[1].split(')')[0]
			except:
				self.jdbc_oracle_servicename = None

			if self.jdbc_oracle_sid == None and self.jdbc_oracle_servicename == None:
				logging.error("Cant find either SID or SERVICE_NAME in Oracle URL")
				exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:mysql:'): 
			self.db_mysql = True
			self.jdbc_servertype = constant.MYSQL
			self.jdbc_force_column_lowercase = True
			self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("MySQL", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[13:].split(':')[0].split(';')[0].split('/')[0]
			try:
				self.jdbc_port = self.jdbc_url[13:].split(':')[1].split('/')[0]
			except:
				self.jdbc_port = "3306"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "3306"

			try:
				self.jdbc_database = self.jdbc_url[13:].split('/')[1].split(';')[0]
			except:
				logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:postgresql:'): 
			self.db_postgresql = True
			self.jdbc_servertype = constant.POSTGRESQL
			self.jdbc_force_column_lowercase = True
			self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("PostgreSQL", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[18:].split(':')[0].split(';')[0].split('/')[0]
			try:
				self.jdbc_port = self.jdbc_url[18:].split(':')[1].split('/')[0]
			except:
				self.jdbc_port = "5432"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "5432"

			try:
				self.jdbc_database = self.jdbc_url[18:].split('/')[1].split(';')[0]
			except:
				logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:datadirect:openedge:'): 
			self.db_progress = True
			self.jdbc_servertype = constant.PROGRESS
			self.jdbc_force_column_lowercase = True
			self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("Progress DB", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[27:].split(':')[0].split(';')[0]
			try:
				self.jdbc_port = self.jdbc_url[27:].split(':')[1].split(';')[0]
			except:
				self.jdbc_port = "9999"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "9999"

			try:
				self.jdbc_database = self.jdbc_url.split("databaseName=")[1].split(';')[0]
			except:
				logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:db2://'): 
			self.db_db2udb = True
			self.jdbc_servertype = constant.DB2_UDB
			self.jdbc_force_column_lowercase = False
			self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("DB2 UDB", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[11:].split(':')[0].split(';')[0].split('/')[0]
			try:
				self.jdbc_port = self.jdbc_url[11:].split(':')[1].split('/')[0]
			except:
				self.jdbc_port = "50000"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "50000"

			try:
				self.jdbc_database = self.jdbc_url[11:].split('/')[1].split(';')[0].split(':')[0]
			except:
				logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True

			try:
				self.jdbc_encrypt = self.jdbc_url.split("sslConnection=")[1].split(';')[0].lower()
				if self.jdbc_encrypt == "true": self.jdbc_encrypt = True
			except:
				self.jdbc_encrypt = False

			try:
				self.jdbc_trustedservercert = self.jdbc_url.split("sslTrustStoreLocation=")[1].split(';')[0]
			except:
				self.jdbc_trustedservercert = False

			try:
				self.jdbc_trustedservercert_password = self.jdbc_url.split("sslTrustStorePassword=")[1].split(';')[0]
			except:
				self.jdbc_trustedservercert_password = False


		if self.jdbc_url.startswith( 'jdbc:as400://'): 
			self.db_db2as400 = True
			self.jdbc_servertype = constant.DB2_AS400
			self.jdbc_force_column_lowercase = False
			self.jdbc_driver, self.jdbc_classpath = self.getJDBCDriverConfig("DB2 AS400", "default")
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url[13:].split(':')[0].split(';')[0].split('/')[0]
			try:
				self.jdbc_port = self.jdbc_url[13:].split(':')[1].split('/')[0]
			except:
				self.jdbc_port = "446"
			if self.jdbc_port.isdigit() == False: self.jdbc_port = "446"

			try:
				self.jdbc_database = self.jdbc_url[13:].split('/')[1].split(';')[0].split(':')[0]
			except:
				logging.error("Cant determine database based on jdbc_string")
				exit_after_function = True



		if self.jdbc_url.startswith( 'mongo:'): 
			self.db_mongodb = True
			self.jdbc_servertype = constant.MONGO
			self.jdbc_driver = "no-jdbc-driver-for-mongodb"
			self.jdbc_classpath = ""
			self.jdbc_classpath_for_python = self.jdbc_classpath

			self.jdbc_hostname = self.jdbc_url.split(':')[1]
			self.jdbc_port = self.jdbc_url.split(':')[2]
			self.jdbc_database = self.jdbc_url.split(':')[3]

		# Check to make sure that we have a supported JDBC string
		if self.jdbc_servertype == "":
			logging.error("JDBC Connection '%s' is not supported."%(self.jdbc_url))
			exit_after_function = True

		if copySlave == True:
			self.jdbc_hostname = None
			self.jdbc_port = None
			self.jdbc_database = None

		logging.debug("    db_mssql = %s"%(self.db_mssql))
		logging.debug("    db_oracle = %s"%(self.db_oracle))
		logging.debug("    db_mysql = %s"%(self.db_mysql))
		logging.debug("    db_postgresql = %s"%(self.db_postgresql))
		logging.debug("    db_progress = %s"%(self.db_progress))
		logging.debug("    db_db2udb = %s"%(self.db_db2udb))
		logging.debug("    db_db2as400 = %s"%(self.db_db2as400))
		logging.debug("    db_mongodb = %s"%(self.db_mongodb))
		logging.debug("    jdbc_servertype = %s"%(self.jdbc_servertype))
		logging.debug("    jdbc_url = %s"%(self.jdbc_url))
		logging.debug("    jdbc_username = %s"%(self.jdbc_username))
		logging.debug("    jdbc_password = %s"%(self.jdbc_password))
		logging.debug("    jdbc_hostname = %s"%(self.jdbc_hostname))
		logging.debug("    jdbc_port = %s"%(self.jdbc_port))
		logging.debug("    jdbc_database = %s"%(self.jdbc_database))
		logging.debug("    jdbc_classpath = %s"%(self.jdbc_classpath))
		logging.debug("    jdbc_classpath_for_python = %s"%(self.jdbc_classpath_for_python))
		logging.debug("    jdbc_driver = %s"%(self.jdbc_driver))
		logging.debug("    jdbc_driver_for_python = %s"%(self.jdbc_driver_for_python))
		logging.debug("    jdbc_ad_domain = %s"%(self.jdbc_ad_domain))
		logging.debug("    jdbc_encrypt = %s"%(self.jdbc_encrypt))
		logging.debug("    jdbc_encrypt_string = %s"%(self.jdbc_encrypt_string))
		logging.debug("    jdbc_trustedservercert = %s"%(self.jdbc_trustedservercert))
		logging.debug("    jdbc_trustedservercert_password = %s"%(self.jdbc_trustedservercert_password))
		logging.debug("    jdbc_hostincert = %s"%(self.jdbc_hostincert))
		logging.debug("    jdbc_logintimeout = %s"%(self.jdbc_logintimeout))
		logging.debug("    jdbc_password_file = %s"%(self.jdbc_password_file))
		logging.debug("    jdbc_oracle_sid = %s"%(self.jdbc_oracle_sid))
		logging.debug("    jdbc_oracle_servicename = %s"%(self.jdbc_oracle_servicename))
		logging.debug("Executing common_config.lookupConnectionAlias() - Finished")

		if exit_after_function == True:
			raise Exception

	def getJDBCTableDefinition(self, source_schema, source_table, printInfo=True):
		logging.debug("Executing common_config.getJDBCTableDefinition()")
		if printInfo == True:
			logging.info("Reading SQL table definitions from source database")
		self.source_schema = source_schema
		self.source_table = source_table

		# Connect to the source database
		self.connectToJDBC()

		self.source_columns_df = self.sourceSchema.readTableColumns(  self.JDBCCursor, 
														serverType = self.jdbc_servertype, 
														database = self.jdbc_database,
														schema = self.source_schema,
														table = self.source_table)

		self.source_keys_df = self.sourceSchema.readTableKeys(	self.JDBCCursor,
														serverType = self.jdbc_servertype, 
														database = self.jdbc_database,
														schema = self.source_schema,
														table = self.source_table)

		logging.debug("Executing common_config.getSourceTableDefinition() - Finished")

	def connectToJDBC(self,):

		if self.JDBCCursor == None:
			logging.debug("Connecting to database over JDBC")
			logging.debug("	self.jdbc_username = %s"%(self.jdbc_username))
			logging.debug("	self.jdbc_password = %s"%(self.jdbc_password))
			logging.debug("	self.jdbc_driver = %s"%(self.jdbc_driver))
			logging.debug("	self.jdbc_url = %s"%(self.jdbc_url))
			logging.debug("	self.jdbc_classpath_for_python = %s"%(self.jdbc_classpath_for_python))
#			logging.debug("	self.jdbc_driver_for_python = %s"%(self.jdbc_driver_for_python))

			JDBCCredentials = [ self.jdbc_username, self.jdbc_password ]
			try:
				self.JDBCConn = jaydebeapi.connect(self.jdbc_driver, self.jdbc_url, JDBCCredentials , self.jdbc_classpath_for_python)
				self.JDBCCursor = self.JDBCConn.cursor()
			except jpype.JavaException as exception:
				print("Connection to database over JDBC failed with the following error:")
				print(exception.message())
				self.remove_temporary_files()
				sys.exit(1)

	def getJDBCcolumnMaxValue(self, source_schema, source_table, column):
		logging.debug("Executing common_config.getJDBCcolumnMaxValue()")

		self.connectToJDBC()
		query = None
	
		if self.db_mssql == True:
			query = "select max(%s) from [%s].[%s].[%s]"%(column, self.jdbc_database, source_schema, source_table) 

		if self.db_oracle == True:
			query = "select max(%s) from \"%s\".\"%s\""%(column, source_schema.upper(), source_table.upper())

		if self.db_mysql == True:
			query = "select max(%s) from %s"%(column, source_table)

		if self.db_postgresql == True:
			query = "select max(%s) from \"%s\".\"%s\""%(column, source_schema, source_table)

		if self.db_progress == True:
			query = "select max(%s) from \"%s\".\"%s\""%(column, source_schema, source_table)

		if self.db_db2udb == True:
			query = "select max(%s) from \"%s\".\"%s\""%(column, source_schema, source_table)

		if self.db_db2as400 == True:		
			query = "select max(%s) from \"%s\".\"%s\""%(column, source_schema, source_table)

		self.JDBCCursor.execute(query)
		logging.debug("SQL Statement executed: %s" % (query) )
		row = self.JDBCCursor.fetchone()

		return row[0]

		logging.debug("Executing common_config.getJDBCcolumnMaxValue() - Finished")

	def truncateJDBCTable(self, schema, table):
		""" Truncates a table on the JDBC connection """
		logging.debug("Executing common_config.truncateJDBCTable()")
		self.connectToJDBC()
		query = None

		if self.db_oracle == True:
			query = "truncate table \"%s\".\"%s\""%(schema.upper(), table.upper())

		if self.db_mssql == True:
			query = "truncate table %s.%s"%(schema, table)

		if self.db_db2udb == True:
			query = "truncate table \"%s\".\"%s\" immediate"%(schema.upper(), table.upper())

		if self.db_mysql == True:
			query = "truncate table %s"%(table)

		if query == None:
			raise undevelopedFeature("There is no support for this database type in common_config.checkJDBCTable()") 
		
		logging.debug("SQL Statement executed: %s" % (query) )
		self.JDBCCursor.execute(query)

		logging.debug("Executing common_config.truncateJDBCTable() - Finished")

	def checkJDBCTable(self, schema, table):
		""" Checks if a table exists on the JDBC connections. Return True or False """ 
		logging.debug("Executing common_config.checkJDBCTable()")
		self.connectToJDBC()
		query = None
	
		if self.db_oracle == True:
			query = "select count(owner) from all_tables where owner = '%s' and table_name = '%s'"%(schema.upper(), table.upper())

		if self.db_mssql == True:
			query = "select count(table_name) from INFORMATION_SCHEMA.COLUMNS where table_schema = '%s' and table_name = '%s'"%(schema, table)

		if self.db_db2udb == True:
			query = "select count(name) from SYSIBM.SYSTABLES where upper(creator) = '%s' and upper(name) = '%s'"%(schema.upper(), table.upper())
		if self.db_mysql == True:
			query = "select count(table_name) from information_schema.tables where table_schema = '%s' and table_name = '%s'"%(self.jdbc_database, table)

		if query == None:
			raise undevelopedFeature("There is no support for this database type in common_config.checkJDBCTable()") 


		logging.debug("SQL Statement executed: %s" % (query) )
		self.JDBCCursor.execute(query)
		row = self.JDBCCursor.fetchone()

		tableExists = False

		if int(row[0]) > 0:
			tableExists = True

		logging.debug("Executing common_config.checkJDBCTable() - Finished")
		return tableExists

	def executeJDBCquery(self, query):
		""" Executes a query against the JDBC database and return the values in a Pandas DF """
		logging.debug("Executing common_config.executeJDBCquery()")

		logging.debug("Query to execute: %s"%(query))
		try:
			self.connectToJDBC()
			self.JDBCCursor.execute(query)
		except jaydebeapi.DatabaseError as errMsg:
			raise SQLerror(errMsg)	

		result_df = pd.DataFrame()
		try:
			result_df = pd.DataFrame(self.JDBCCursor.fetchall())
			result_df_columns = []
			for columns in self.JDBCCursor.description:
				result_df_columns.append(columns[0])    # Name of the column is in the first position
			result_df.columns = result_df_columns
		except jaydebeapi.Error:
			logging.debug("An error was raised during JDBCCursor.fetchall(). This happens during SQL operations that dont return any rows like 'create table'")
			
		# Set the correct column namnes in the DataFrame

		logging.debug("Executing common_config.executeJDBCquery() - Finished")
		return result_df


	def getJDBCTableRowCount(self, source_schema, source_table, whereStatement=None):
		logging.debug("Executing common_config.getJDBCTableRowCount()")

		self.connectToJDBC()
		query = None
	
		if self.db_mssql == True:
			query = "select count_big(1) from [%s].[%s].[%s]"%(self.jdbc_database, source_schema, source_table) 

		if self.db_oracle == True:
			query = "select count(1) from \"%s\".\"%s\""%(source_schema.upper(), source_table.upper())

		if self.db_mysql == True:
			query = "select count(1) from %s" % source_table

		if self.db_postgresql == True:
			query = "select count(1) from \"%s\".\"%s\""%(source_schema, source_table)

		if self.db_progress == True:
			query = "select count(1) from \"%s\".\"%s\""%(source_schema, source_table)

		if self.db_db2udb == True:
			query = "select count(1) from \"%s\".\"%s\""%(source_schema, source_table)

		if self.db_db2as400 == True:		
			query = "select count(1) from \"%s\".\"%s\""%(source_schema, source_table)

		if whereStatement != None and whereStatement != "":
			query = query + " where " + whereStatement

		logging.debug("SQL Statement executed: %s" % (query) )
		self.JDBCCursor.execute(query)
		row = self.JDBCCursor.fetchone()

		return int(row[0])

		logging.debug("Executing common_config.getJDBCTableRowCount() - Finished")

	def dropJDBCTable(self, schema, table):
		logging.debug("Executing common_config.dropJDBCTable()")

		self.connectToJDBC()
		query = "drop table %s"%(self.getJDBCsqlFromTable(schema=schema, table=table))
		self.JDBCCursor.execute(query)

		logging.debug("Executing common_config.dropJDBCTable() - Finished")

	def logImportFailure(self, errorText, severity, importType=None, hiveDB=None, hiveTable=None):

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		query  = "insert into import_failure_log "
		query += "( hive_db, hive_table, eventtime, severity, import_type, error_text ) "
		query += "values "
		query += "( %s, %s, %s, %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), severity, importType, errorText))
		self.mysql_conn.commit()

	def logHiveColumnAdd(self, column, columnType=None, description=None, hiveDB=None, hiveTable=None):
		if description == None:
			description = "Column '%s' added to table with type '%s'"%(column, columnType)

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

#		query  = "insert into hive_table_change_history "
		query  = "insert into table_change_history "
		query += "( hive_db, hive_table, column_name, eventtime, event, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, 'column_added', %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, column, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), columnType, description))
		self.mysql_conn.commit()

	def logHiveColumnTypeChange(self, column, columnType, previous_columnType=None, description=None, hiveDB=None, hiveTable=None):

		if description == None:
			if previous_columnType == None:
				description = "Column '%s' type changed to %s"%(column, columnType)
			else:
				description = "Column '%s' type changed from %s to %s"%(column, previous_columnType, columnType)

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

#		query  = "insert into hive_table_change_history "
		query  = "insert into table_change_history "
		query += "( hive_db, hive_table, column_name, eventtime, event, previous_value, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, 'column_type_change', %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, column, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), previous_columnType, columnType, description))
		self.mysql_conn.commit()

	def logHiveColumnRename(self, columnName, previous_columnName, description=None, hiveDB=None, hiveTable=None):

		if description == None:
			description = "Column '%s' renamed to %s"%(previous_columnName, columnName)

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

#		query  = "insert into hive_table_change_history "
		query  = "insert into table_change_history "
		query += "( hive_db, hive_table, column_name, eventtime, event, previous_value, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, 'column_rename', %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, columnName, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), previous_columnName, columnName, description))
		self.mysql_conn.commit()

	def logJDBCColumnAdd(self, column, columnType=None, description=None, dbAlias=None, database=None, schema=None, table=None):
		if description == None:
			description = "Column '%s' added to table with type '%s'"%(column, columnType)

		query  = "insert into jdbc_table_change_history "
		query += "( dbalias, db_name, schema_name, table_name, column_name, eventtime, event, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, %s, %s, 'column_added', %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (dbAlias, database, schema, table, column, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), columnType, description))
		self.mysql_conn.commit()

	def logJDBCColumnTypeChange(self, column, columnType, previous_columnType=None, description=None, dbAlias=None, database=None, schema=None, table=None):

		if description == None:
			if previous_columnType == None:
				description = "Column '%s' type changed to %s"%(column, columnType)
			else:
				description = "Column '%s' type changed from %s to %s"%(column, previous_columnType, columnType)

		query  = "insert into jdbc_table_change_history "
		query += "( dbalias, db_name, schema_name, table_name, column_name, eventtime, event, previous_value, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, %s, %s, 'column_type_change', %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (dbAlias, database, schema, table, column, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), previous_columnType, columnType, description))
		self.mysql_conn.commit()

	def logJDBCColumnRename(self, columnName, previous_columnName, description=None, dbAlias=None, database=None, schema=None, table=None):

		if description == None:
			description = "Column '%s' renamed to %s"%(previous_columnName, columnName)

		query  = "insert into jdbc_table_change_history "
		query += "( dbalias, db_name, schema_name, table_name, column_name, eventtime, event, previous_value, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, %s, %s, 'column_rename', %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (dbAlias, database, schema, table, columnName, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), previous_columnName, columnName, description))
		self.mysql_conn.commit()

	def getQuoteAroundColumn(self):
		quoteAroundColumn = ""
		if self.jdbc_servertype == constant.MSSQL:        quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.ORACLE:       quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.MYSQL:        quoteAroundColumn = "`"
		if self.jdbc_servertype == constant.POSTGRESQL:   quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.PROGRESS:     quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.DB2_UDB:      quoteAroundColumn = "\""
		if self.jdbc_servertype == constant.DB2_AS400:    quoteAroundColumn = "\""

		return quoteAroundColumn

	def getJDBCUpperCase(self):
		upperCase = False
		if self.jdbc_servertype == constant.ORACLE:       upperCase = True
		if self.jdbc_servertype == constant.DB2_UDB:      upperCase = True

		return upperCase

	def getJDBCsqlFromTable(self, schema, table):
		logging.debug("Executing common_config.getJDBCsqlFromTable()")

		fromTable = ""
		if self.jdbc_servertype == constant.MSSQL:        fromTable = "[%s].[%s].[%s]"%(self.jdbc_database, schema, table)
		if self.jdbc_servertype == constant.ORACLE:       fromTable = "\"%s\".\"%s\""%(schema.upper(), table.upper())
		if self.jdbc_servertype == constant.MYSQL:        fromTable = "%s"%(table)
		if self.jdbc_servertype == constant.POSTGRESQL:   fromTable = "\"%s\".\"%s\""%(schema, table)
		if self.jdbc_servertype == constant.PROGRESS:     fromTable = "\"%s\".\"%s\""%(schema, table)
		if self.jdbc_servertype == constant.DB2_UDB:      fromTable = "\"%s\".\"%s\""%(schema, table)
		if self.jdbc_servertype == constant.DB2_AS400:    fromTable = "\"%s\".\"%s\""%(schema, table)

		logging.debug("Executing common_config.getJDBCsqlFromTable() - Finished")
		return fromTable

	def getConfigValue(self, key):
		""" Returns a value from the configuration table based on the supplied key. Value returned can be Int, Str or DateTime""" 
		logging.debug("Executing common_config.getConfigValue()")
		returnValue = None
		boolValue = False
	
		if key in ("hive_remove_locks_by_force", "airflow_disable", "import_start_disable", "import_stage_disable", "export_start_disable", "export_stage_disable", "hive_validate_before_execution", "hive_print_messages"):
			valueColumn = "valueInt"
			boolValue = True
		elif key in ("sqoop_import_default_mappers", "sqoop_import_max_mappers", "sqoop_export_default_mappers", "sqoop_export_max_mappers", "spark_export_default_executors", "spark_export_max_executors", "spark_import_default_executors", "spark_import_max_executors"):
			valueColumn = "valueInt"
		elif key in ("import_staging_database", "export_staging_database", "hive_validate_table", "airflow_dbimport_commandpath", "airflow_dag_directory", "airflow_dag_staging_directory", "airflow_dag_file_group", "airflow_dag_file_permission", "hdfs_address", "hdfs_blocksize", "hdfs_basedir"):
			valueColumn = "valueStr"
		else:
			logging.error("There is no configuration with the name '%s'"%(key))
			self.remove_temporary_files()
			sys.exit(1)

		query = "select %s from configuration where configkey = '%s'"%(valueColumn, key)

		logging.debug("SQL Statement executed: %s" % (query) )
		self.mysql_cursor.execute(query)
		row = self.mysql_cursor.fetchone()

		if valueColumn == "valueInt":
			returnValue = int(row[0])

		if valueColumn == "valueStr":
			returnValue = row[0]
			if returnValue == None or returnValue.strip() == "":
				logging.error("Configuration Key '%s' must have a value in '%s'"%(key, valueColumn))
				self.remove_temporary_files()
				sys.exit(1)

		if boolValue == True:
			if returnValue == 1:
				returnValue = True
			elif returnValue == 0:
				returnValue = False
			else:
				logging.error("Configuration Key '%s' can only have 0 or 1 in column '%s'"%(key, valueColumn))
				self.remove_temporary_files()
				sys.exit(1)

		logging.debug("Fetched configuration '%s' as '%s'"%(key, returnValue))
		logging.debug("Executing common_config.getConfigValue() - Finished")
		return returnValue

	def getJDBCtablesAndViews(self, schemaFilter=None, tableFilter=None):
		logging.debug("Executing common_config.getJDBCtablesAndViews()")
		self.connectToJDBC()

		if schemaFilter != None:
			schemaFilter = schemaFilter.replace('*', '%')

		if tableFilter != None:
			tableFilter = tableFilter.replace('*', '%')

		if self.jdbc_servertype == constant.MSSQL:
			query = "select TABLE_SCHEMA, TABLE_NAME from INFORMATION_SCHEMA.TABLES "
			if schemaFilter != None:
				query += "where TABLE_SCHEMA like '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "and TABLE_NAME like '%s' "%(tableFilter)
				else:
					query += "where TABLE_NAME like '%s' "%(tableFilter)
			query += "order by TABLE_SCHEMA, TABLE_NAME"

		if self.jdbc_servertype == constant.ORACLE:
			query  = "select OWNER, TABLE_NAME as NAME from all_tables "
			if schemaFilter != None:
				query += "where OWNER like '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "and TABLE_NAME like '%s' "%(tableFilter)
				else:
					query += "where TABLE_NAME like '%s' "%(tableFilter)

			query += "union all "
			query += "select OWNER, VIEW_NAME as NAME from all_views "
			if schemaFilter != None:
				query += "where OWNER like '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "and VIEW_NAME like '%s' "%(tableFilter)
				else:
					query += "where VIEW_NAME like '%s' "%(tableFilter)
			query += "order by OWNER, NAME "

		if self.jdbc_servertype == constant.MYSQL:
			query = "select '-', table_name from INFORMATION_SCHEMA.tables where table_schema = '%s' "%(self.jdbc_database)
			if tableFilter != None:
				query += "and table_name like '%s' "%(tableFilter)
			query += "order by table_name"

		if self.jdbc_servertype == constant.POSTGRESQL:
			query = "select TABLE_SCHEMA, TABLE_NAME from INFORMATION_SCHEMA.TABLES "
			if schemaFilter != None:
				query += "where TABLE_SCHEMA like '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "and TABLE_NAME like '%s' "%(tableFilter)
				else:
					query += "where TABLE_NAME like '%s' "%(tableFilter)
			query += "order by TABLE_SCHEMA, TABLE_NAME"

		if self.jdbc_servertype == constant.PROGRESS:
			query  = "select \"_Owner\", \"_File-Name\" from PUB.\"_File\" "
			if schemaFilter != None:
				query += "WHERE \"_Owner\" LIKE '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "AND \"_File-Name\" LIKE '%s' "%(tableFilter)
				else:
					query += "WHERE \"_File-Name\" LIKE '%s' "%(tableFilter)
			query += "ORDER BY \"_Owner\", \"_File-Name\""

		if self.jdbc_servertype == constant.DB2_UDB:
			query  = "SELECT CREATOR, NAME FROM SYSIBM.SYSTABLES "
			if schemaFilter != None:
				query += "WHERE CREATOR LIKE '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "AND NAME LIKE '%s' "%(tableFilter)
				else:
					query += "WHERE NAME LIKE '%s' "%(tableFilter)
			query += "ORDER BY CREATOR, NAME"

		if self.jdbc_servertype == constant.DB2_AS400:
			query  = "SELECT TABLE_SCHEM, TABLE_NAME FROM SYSIBM.SQLTABLES "
			if schemaFilter != None:
				query += "WHERE TABLE_SCHEM LIKE '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "AND TABLE_NAME LIKE '%s' "%(tableFilter)
				else:
					query += "WHERE TABLE_NAME LIKE '%s' "%(tableFilter)
			query += "ORDER BY TABLE_SCHEM, TABLE_NAME"

		logging.debug("SQL Statement executed: %s" % (query) )
		self.JDBCCursor.execute(query)

		result_df = pd.DataFrame(self.JDBCCursor.fetchall())
		if len(result_df) > 0:
			result_df.columns = ['schema', 'table']

		logging.debug("Executing common_config.getJDBCtablesAndViews() - Finished")
		return result_df
