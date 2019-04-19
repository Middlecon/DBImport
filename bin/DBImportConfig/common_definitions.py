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
import base64
from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_OAEP
from subprocess import Popen, PIPE
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, time, timedelta
import pandas as pd
from sourceSchemaReader import schemaReader
from DBImportConfig import constants as constant
from DBImportConfig import decryption as decryption

class config(object):
	def __init__(self, Hive_DB, Hive_Table):
		logging.debug("Executing common_definitions.__init__()")

		self.Hive_DB = Hive_DB.lower()	 
		self.Hive_Table = Hive_Table.lower()	 
		self.mysql_conn = None
		self.mysql_cursor = None
		self.tempdir = None
		self.startDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') 

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

		self.sourceSchema = None

		self.source_columns_df = pd.DataFrame()
		self.source_keys_df = pd.DataFrame()

		self.JDBCConn = None
		self.JDBCCursor = None

		self.sourceSchema = schemaReader.source()

		self.crypto = decryption.crypto()
		self.crypto.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
		self.crypto.setPublicKeyFile(configuration.get("Credentials", "public_key"))

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

		# Fetch configuration about HDFS
		self.hdfs_address = configuration.get("HDFS", "hdfs_address")

		# Fetch configuration about MySQL database and how to connect to it
		mysql_hostname = configuration.get("Database", "mysql_hostname")
		mysql_port =     configuration.get("Database", "mysql_port")
		mysql_database = configuration.get("Database", "mysql_database")
		mysql_username = configuration.get("Database", "mysql_username")
		mysql_password = configuration.get("Database", "mysql_password")

		self.rest_column_data_endpoint = configuration.get("REST_calls", "column_data_endpoint")
		if configuration.get("REST_calls", "post_column_data").lower() == "true":
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
		logging.debug("Executing common_definitions.__init__() - Finished")

	def getMysqlCursor(self):
		return self.mysql_cursor

	def getMysqlConnector(self):
		return self.mysql_conn

	def remove_temporary_files(self):
		# We add this check just to make sure that self.tempdir contains a valid path and that no exception forces us to remove example /
		if len(self.tempdir) > 13 and self.tempdir.startswith( '/tmp/' ):
			try:
				shutil.rmtree(self.tempdir)
			except FileNotFoundError:
				# This can happen as we might call this function multiple times during an error and we dont want an exception because of that
				return

	def checkTimeWindow(self, connection_alias):
		logging.debug("Executing common_definitions.checkTimeWindow()")
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
		logging.debug("Executing common_definitions.checkTimeWindow() - Finished")

	def lookupConnectionAlias(self, connection_alias):
		logging.debug("Executing common_definitions.lookupConnectionAlias()")
	
		exit_after_function = False

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

		credentials = self.crypto.decrypt(row[1])
		if credentials == None:
			logging.error("Cant decrypt username and passowrd. Check private/public key in config file")
			raise Exception
		
		self.jdbc_username = credentials.split(" ")[0]
		self.jdbc_password = credentials.split(" ")[1]

		# Sets a creates the password file that is used by sqoop and other tools
		self.jdbc_password_file = self.tempdir + "/jdbc_passwd"
		f = open(self.jdbc_password_file, "a")
		f.write(self.jdbc_password)
		f.close()
		os.chmod(self.jdbc_password_file, 0o400)

		# Lookup Connection details based on JDBC STRING for all different types we support
		if self.jdbc_url.startswith( 'jdbc:sqlserver:'): 
			self.db_mssql = True
			self.jdbc_servertype = constant.MSSQL
			self.jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
			self.jdbc_classpath = "/usr/share/java/mssql-jdbc-6.2.2.jre8.jar"
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
				self.jdbc_driver_for_python = "net.sourceforge.jtds.jdbc.Driver"
				self.jdbc_classpath_for_python = "/usr/hdp/current/sqoop-client/lib/jtds.jar"


		if self.jdbc_url.startswith( 'jdbc:jtds:sqlserver:'): 
			self.db_mssql = True
			self.jdbc_servertype = constant.MSSQL
			self.jdbc_driver = "net.sourceforge.jtds.jdbc.Driver"
			self.jdbc_classpath = "/usr/hdp/current/sqoop-client/lib/jtds.jar"
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

			try:
				self.jdbc_ad_domain = self.jdbc_url.split("domain=")[1].split(';')[0]
			except:
				logging.error("Cant determine AD Domain based on jdbc_string")
				exit_after_function = True


		if self.jdbc_url.startswith( 'jdbc:oracle:'): 
			self.db_oracle = True
			self.jdbc_servertype = constant.ORACLE
			self.jdbc_driver = "oracle.jdbc.driver.OracleDriver"
			self.jdbc_classpath = "/usr/share/java/ojdbc8.jar"
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
			self.jdbc_driver = "com.mysql.jdbc.Driver"
			self.jdbc_classpath = "/usr/share/java/mysql-connector-java.jar"
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
			self.jdbc_driver = "org.postgresql.Driver"
			self.jdbc_classpath = "/usr/share/java/postgresql-jdbc.jar"
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
			self.jdbc_driver = "com.ddtek.jdbc.openedge.OpenEdgeDriver"
			self.jdbc_classpath = "/usr/share/java/progress/openedge.jar:/usr/share/java/progress/pool.jar"
			self.jdbc_classpath_for_python = "/usr/share/java/progress/openedge.jar"

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
			self.jdbc_driver = "com.ibm.db2.jcc.DB2Driver"
			self.jdbc_classpath = "/usr/share/java/db2jcc4.jar"
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
			self.jdbc_driver = "com.ibm.as400.access.AS400JDBCDriver"
			self.jdbc_classpath = "/usr/share/java/db2_jt400.jar"
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
		logging.debug("Executing common_definitions.lookupConnectionAlias() - Finished")

		if exit_after_function == True:
			raise Exception

#	def connectToSourceDatabase(self):
#		if self.sourceSchema == None:
#			self.connectToJDBC()
#
#			self.sourceSchema.db_mssql = self.db_mssql
#			self.sourceSchema.db_oracle = self.db_oracle
#			self.sourceSchema.db_mysql = self.db_mysql
#			self.sourceSchema.db_postgresql = self.db_postgresql
#			self.sourceSchema.db_progress = self.db_progress
#			self.sourceSchema.db_db2udb = self.db_db2udb
#			self.sourceSchema.db_db2as400 = self.db_db2as400
#			self.sourceSchema.db_mongodb = self.db_mongodb
#			self.sourceSchema.jdbc_servertype = self.jdbc_servertype
#			self.sourceSchema.jdbc_url = self.jdbc_url
#			self.sourceSchema.jdbc_username = self.jdbc_username
#			self.sourceSchema.jdbc_password = self.jdbc_password
#			self.sourceSchema.jdbc_hostname = self.jdbc_hostname
#			self.sourceSchema.jdbc_port = self.jdbc_port
#			self.sourceSchema.jdbc_database = self.jdbc_database
#			self.sourceSchema.jdbc_classpath = self.jdbc_classpath
#			self.sourceSchema.jdbc_classpath_for_python = self.jdbc_classpath_for_python
#			self.sourceSchema.jdbc_driver = self.jdbc_driver
#			self.sourceSchema.jdbc_driver_for_python = self.jdbc_driver_for_python
#			self.sourceSchema.jdbc_ad_domain = self.jdbc_ad_domain
#			self.sourceSchema.jdbc_encrypt = self.jdbc_encrypt
#			self.sourceSchema.jdbc_encrypt_string = self.jdbc_encrypt_string
#			self.sourceSchema.jdbc_trustedservercert = self.jdbc_trustedservercert
#			self.sourceSchema.jdbc_trustedservercert_password = self.jdbc_trustedservercert_password
#			self.sourceSchema.jdbc_hostincert = self.jdbc_hostincert
#			self.sourceSchema.jdbc_logintimeout = self.jdbc_logintimeout
#			self.sourceSchema.jdbc_password_file = self.jdbc_password_file
#			self.sourceSchema.jdbc_oracle_sid = self.jdbc_oracle_sid
#			self.sourceSchema.jdbc_oracle_servicename = self.jdbc_oracle_servicename
#
#			sourceTable = self.sourceSchema.readTable(
#							serverType = self.jdbc_servertype,
#							host = self.jdbc_hostname,
#							port = self.jdbc_port,
#							database = self.jdbc_database,
#							table = self.source_table,
#							user = self.jdbc_username,
#							password = self.jdbc_password,
#							driver = self.jdbc_classpath_for_python)
#
#			print(type(self.JDBCCursor))
#			sourceTable = self.sourceSchema.readTable(self.JDBCCursor, serverType = self.jdbc_servertype, table = self.source_table)
#
#			print(sourceTable)

	def getJDBCTableDefinition(self, source_schema, source_table):
		logging.debug("Executing common_definitions.getJDBCTableDefinition()")
		logging.info("Reading SQL table definitions from source database")
		self.source_schema = source_schema
		self.source_table = source_table

		# Connect to the source database
		self.connectToJDBC()

#		# Create config file for the Schema Python Program 
#		if len(self.tempdir) < 13 or self.tempdir.startswith( '/tmp/' ) == False:
#			logging.error("tempdir is not based on /tmp")
#			raise Exception
#
#		connect_file = self.tempdir + "/connect"
#		f = open(connect_file, "a")
#		f.write("HOST = " + self.jdbc_hostname + "\n")
#		f.write("PORT = " + self.jdbc_port + "\n")
#		f.write("user = " + self.jdbc_username + "\n")
#		f.write("password = " + self.jdbc_password + "\n")
#		f.write("driver_location=" + self.jdbc_classpath_for_python + "\n")
#
#		# Temporary until other Python Code is gone
#		if self.jdbc_servertype == constant.MYSQL:      f.write("DB_TYPE = MYSQL\n")
#		if self.jdbc_servertype == constant.ORACLE:     f.write("DB_TYPE = ORACLE\n")
#		if self.jdbc_servertype == constant.MSSQL:      f.write("DB_TYPE = SQL_SERVER\n")
#		if self.jdbc_servertype == constant.POSTGRESQL: f.write("DB_TYPE = POSTGRESQL\n")
#		if self.jdbc_servertype == constant.PROGRESS:   f.write("DB_TYPE = PROGRESS_DB\n")
#		if self.jdbc_servertype == constant.DB2_UDB:    f.write("DB_TYPE = DB2_UDB\n")
#		if self.jdbc_servertype == constant.DB2_AS400:  f.write("DB_TYPE = DB2_AS400\n")
#		if self.jdbc_servertype == constant.MONGO:      f.write("DB_TYPE = MONGO\n")
#
#		if self.db_mssql == True and self.jdbc_ad_domain != None:
#			f.write("DOMAIN = " + self.jdbc_ad_domain + "\n")
#		
#		if self.db_mssql == True and self.jdbc_encrypt == True:
#			f.write("additional_properties = " + self.jdbc_encrypt_string + "\n")
#		
#		if self.db_oracle == True:
#			if self.jdbc_oracle_sid != None:
#				f.write("SID = " + self.jdbc_oracle_sid + "\n") 
#			else:
#				f.write("SERVICE_NAME = " + self.jdbc_oracle_servicename + "\n") 
#
##		if self.db_progress == True:
##			f.write("driver_location=" + self.self.jdbc_classpath_for_python + "\n")
#
#		if self.db_db2udb == True:
#			f.write("database=" + self.jdbc_database + "\n")
#			if self.jdbc_encrypt == True:
#				f.write("sslTrustStoreLocation = " + self.jdbc_trustedservercert + "\n")
##				f.write("sslTrustStorePassword=" + self.jdbc_trustedservercert_password + "\n")
#				
#		if self.db_db2as400 == True:
#			f.write("database=" + self.jdbc_database + "\n")
#
#		f.close()
#		os.chmod(connect_file, 0o400)
#
#		# Debug output of Connect file
#		f = open(connect_file, "r")
#		logging.debug("Connect file for Python Schema Program: \n%s"%(f.read()))
#		f.close()

		# Decrypt and get the username and password for the JDBC Connection

#		temp_column_df = pd.DataFrame()
#		temp_key_df = pd.DataFrame()

		# All that is converted to not use external Python code
#		if self.db_mssql == True or self.db_oracle == True or self.db_mysql == True or self.db_postgresql == True or self.db_db2udb == True or self.db_db2as400 == True: 
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

#		if self.db_db2as400 == True:		
#			self.source_columns_df = self.sourceSchema.readTableColumns(  self.JDBCCursor, 
#			temp_column_df = self.sourceSchema.readTableColumns(  self.JDBCCursor, 
#														serverType = self.jdbc_servertype, 
#														database = self.jdbc_database,
#														schema = self.source_schema,
#														table = self.source_table)

#			temp_key_df = self.sourceSchema.readTableKeys(	self.JDBCCursor,
#														serverType = self.jdbc_servertype, 
#														database = self.jdbc_database,
#														schema = self.source_schema,
#														table = self.source_table)
#			column_session = subprocess.Popen(["python", "/usr/local/db_import/read_schema/TableMetadataGenerator.py", "column", connect_file, self.jdbc_database, self.source_schema, self.source_table], stdout=PIPE, stderr=PIPE)
#			column_stdout, column_stderr = column_session.communicate()

#			key_session = subprocess.Popen(["python", "/usr/local/db_import/read_schema/TableMetadataGenerator.py", "key", connect_file, self.jdbc_database, self.source_schema, self.source_table], stdout=PIPE, stderr=PIPE)
#			key_stdout, key_stderr = key_session.communicate()


#		if self.db_progress == True:
#			temp_column_df = self.sourceSchema.readTableColumns(  self.JDBCCursor, 
#														serverType = self.jdbc_servertype, 
#														database = self.jdbc_database,
#														schema = self.source_schema,
#														table = self.source_table)

#			column_session = subprocess.Popen(["python", "/usr/local/db_import/read_schema/TableMetadataGenerator.py", "column", connect_file, self.jdbc_database, self.source_schema, self.source_table], stdout=PIPE, stderr=PIPE)
#			column_stdout, column_stderr = column_session.communicate()
#		key_session = subprocess.Popen(["python", "/usr/local/db_import/read_schema/TableMetadataGenerator.py", "key", connect_file, self.jdbc_database, self.source_schema, self.source_table], stdout=PIPE, stderr=PIPE)
#		key_stdout, key_stderr = key_session.communicate()

		# TODO: Add Mongo support here

#		if self.source_columns_df.empty == True and 1 == 0:
#			if column_session.returncode != 0:
#				logging.error("Error running Python Schema Program - Column Data")
#				print ("StdOut: %s"%(column_stdout))
#				print ("StdErr: %s"%(column_stderr))
#				raise Exception
#
#			rows_list = []
#			for line in column_stdout.decode('utf-8').strip().split("\n"):
#				if line.startswith('#'): continue
#				line_dict = {}
##				line_dict["SCHEMA_NAME"] = line.split('|')[0]
##				line_dict["TABLE_NAME"] = line.split('|')[1]
#				line_dict["TABLE_COMMENT"] = line.split('|')[2]
#				line_dict["SOURCE_COLUMN_NAME"] = line.split('|')[3]
#				line_dict["SOURCE_COLUMN_TYPE"] = line.split('|')[4]
#				line_dict["SOURCE_COLUMN_COMMENT"] = line.split('|')[5]
#				line_dict["IS_NULLABLE"] = line.split('|')[6]
#
#				if line_dict["TABLE_COMMENT"].lower() in ("none", "null"): line_dict["TABLE_COMMENT"] = None
#				if line_dict["SOURCE_COLUMN_COMMENT"].lower() in ("none", "null"): line_dict["SOURCE_COLUMN_COMMENT"] = None
#
#				rows_list.append(line_dict)
#			self.source_columns_df = pd.DataFrame(rows_list)
#			logging.debug("")
#			logging.debug("Columns data from Python Schema Program: \n%s"%(self.source_columns_df))
#
#		if self.source_keys_df.empty == True:
##		if self.source_keys_df.empty == True and 1 == 0:
#			if key_session.returncode != 0:
#				logging.error("Error running Python Schema Program - Key Data")
#				print ("StdOut: %s"%(key_stdout))
#				print ("StdErr: %s"%(key_stderr))
#				raise Exception
#
#			rows_list = []
#			for line in key_stdout.decode('utf-8').strip().split("\n"):
#				if line.startswith('#'): continue
#				line_dict = {}
##				line_dict["SCHEMA_NAME"] = line.split('|')[0]
##				line_dict["TABLE_NAME"] = line.split('|')[1]
#				line_dict["CONSTRAINT_NAME"] = line.split('|')[2]
#				line_dict["CONSTRAINT_TYPE"] = line.split('|')[3]
#				line_dict["COL_NAME"] = line.split('|')[4]
##				line_dict["COL_DATA_TYPE"] = line.split('|')[5]
#				if line.split('|')[6] == "None":
#					line_dict["REFERENCE_SCHEMA_NAME"] = None
#				else:
#					line_dict["REFERENCE_SCHEMA_NAME"] = line.split('|')[6]
#				if line.split('|')[7] == "None":
#					line_dict["REFERENCE_TABLE_NAME"] = None
#				else:
#					line_dict["REFERENCE_TABLE_NAME"] = line.split('|')[7]
#				if line.split('|')[8] == "None":
#					line_dict["REFERENCE_COL_NAME"] = None
#				else:
#					line_dict["REFERENCE_COL_NAME"] = line.split('|')[8]
#				line_dict["COL_KEY_POSITION"] = int(line.split('|')[9])
#				rows_list.append(line_dict)
#			self.source_keys_df = pd.DataFrame(rows_list)
#			logging.debug("")
#			logging.debug("Key data from Python Schema Program: \n%s"%(self.source_keys_df))
#	
#		if temp_column_df.empty == False:
#			print(temp_column_df)
#			print(self.source_columns_df)
#			print("________________________________________________________\n\n")
#
#			merge_df = pd.merge(temp_column_df, self.source_columns_df, on=None, how='outer', indicator='Exist')
#			nomatch_df = merge_df[merge_df['Exist'] != "both"]
#			print(nomatch_df)
#
#		if temp_key_df.empty == False:
#			print(temp_key_df)
#			print(self.source_keys_df)
#			print("________________________________________________________\n\n")
#
#			merge_df = pd.merge(temp_key_df, self.source_keys_df, on=None, how='outer', indicator='Exist')
#			nomatch_df = merge_df[merge_df['Exist'] != "both"]
#			print(nomatch_df)
#
#		logging.debug("")
#		logging.debug("Key data from Python Schema Program: \n%s"%(self.source_keys_df))

		logging.debug("Executing common_definitions.getSourceTableDefinition() - Finished")

	def connectToJDBC(self,):
#		self.JDBCConn = None
#		self.JDBCCursor = None

		if self.JDBCCursor == None:
			logging.debug("Connecting to database over JDBC")
			logging.debug("	self.jdbc_username = %s"%(self.jdbc_username))
			logging.debug("	self.jdbc_password = %s"%(self.jdbc_password))
			logging.debug("	self.jdbc_driver = %s"%(self.jdbc_driver))
			logging.debug("	self.jdbc_url = %s"%(self.jdbc_url))
			logging.debug("	self.jdbc_classpath_for_python = %s"%(self.jdbc_classpath_for_python))

			JDBCCredentials = [ self.jdbc_username, self.jdbc_password ]
			self.JDBCConn = jaydebeapi.connect(self.jdbc_driver, self.jdbc_url, JDBCCredentials , self.jdbc_classpath_for_python)
			self.JDBCCursor = self.JDBCConn.cursor()

	def getJDBCTableRowCount(self, source_schema, source_table, whereStatement):
		logging.debug("Executing common_definitions.getJDBCTableRowCount()")

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

		query = query + " " + whereStatement

		self.JDBCCursor.execute(query)
		logging.debug("SQL Statement executed: %s" % (query) )
		row = self.JDBCCursor.fetchone()

		return int(row[0])

		logging.debug("Executing common_definitions.getJDBCTableRowCount() - Finished")

#	def logColumnAdd(self, column, columnType=None, description=None, Hive_DB=self.Hive_DB, Hive_Table=self.Hive_Table):
	def logColumnAdd(self, column, columnType=None, description=None, hiveDB=None, hiveTable=None):
		if description == None:
			description = "Column '%s' added to table with type '%s'"%(column, columnType)

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		query  = "insert into table_change_history "
		query += "( hive_db, hive_table, column_name, eventtime, event, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, 'column_added', %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, column, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), columnType, description))
		self.mysql_conn.commit()

	def logColumnTypeChange(self, column, columnType, previous_columnType=None, description=None, hiveDB=None, hiveTable=None):

		if description == None:
			if previous_columnType == None:
				description = "Column '%s' type changed to %s"%(column, columnType)
			else:
				description = "Column '%s' type changed from %s to %s"%(column, previous_columnType, columnType)

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		query  = "insert into table_change_history "
		query += "( hive_db, hive_table, column_name, eventtime, event, previous_value, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, 'column_type_change', %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, column, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), previous_columnType, columnType, description))
		self.mysql_conn.commit()

	def logColumnRename(self, columnName, previous_columnName, description=None, hiveDB=None, hiveTable=None):

		if description == None:
			description = "Column '%s' renamed to %s"%(previous_columnName, columnName)

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		query  = "insert into table_change_history "
		query += "( hive_db, hive_table, column_name, eventtime, event, previous_value, value, description ) "
		query += "values "
		query += "( %s, %s, %s, %s, 'column_rename', %s, %s, %s )"

		logging.debug("SQL Statement executed: %s" % (query))
		self.mysql_cursor.execute(query, (hiveDB, hiveTable, columnName, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), previous_columnName, columnName, description))
		self.mysql_conn.commit()
