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
import re
import sys
import pty
import errno
import time
import logging
import signal
import subprocess
import shlex
import pandas as pd
import Crypto
import binascii
import json
import bcrypt
from fastapi.encoders import jsonable_encoder
from fastapi import HTTPException, status
from ConfigReader import configuration
from datetime import date, datetime, timedelta
from Server import dataModels
from common import constants as constant
from common.Exceptions import *
from DBImportConfig import configSchema
from DBImportConfig import common_config
import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select, func, update, delete
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import aliased, sessionmaker, Query
import sqlalchemy.orm.exc

class dbCalls:
	def __init__(self):

#		logFormat = '%(asctime)s %(levelname)s - %(message)s'
#		logging.basicConfig(format=logFormat, level=logging.INFO)
#		logPropagate = True

		self.logger = "gunicorn.error"
		log = logging.getLogger(self.logger)
		log.debug("Executing Server.restServerCalls.__init__()")

		self.mysql_conn = None
		self.mysql_cursor = None
		self.debugLogLevel = False
		self.configDBSession = None

		if logging.root.level == 10:        # DEBUG
			self.debugLogLevel = True

		self.common_config = common_config.config()
		# self.common_config.disconnectConfigDatabase() # This is needed as we must connect with buffered=False and that is only available during reconnect
		# self.common_config.reconnectConfigDatabase(printReconnectMessage=True, buffered=False)

		self.logdir = configuration.get("Server", "logdir")

		self.allConfigKeys = [ "airflow_aws_instanceids", "airflow_aws_pool_to_instanceid", "airflow_create_pool_with_task", "airflow_dag_directory", "airflow_dag_file_group", "airflow_dag_file_permission", "airflow_dag_staging_directory", "airflow_dbimport_commandpath", "airflow_default_pool_size", "airflow_disable", "airflow_dummy_task_queue", "airflow_major_version", "airflow_sudo_user", "atlas_discovery_interval", "cluster_name", "export_default_sessions", "export_max_sessions", "export_stage_disable", "export_staging_database", "export_start_disable", "hdfs_address", "hdfs_basedir", "hdfs_blocksize", "hive_acid_with_clusteredby", "hive_insert_only_tables", "hive_major_compact_after_merge", "hive_print_messages", "hive_remove_locks_by_force", "hive_validate_before_execution", "hive_validate_table", "impala_invalidate_metadata", "import_columnname_delete", "import_columnname_histtime", "import_columnname_import", "import_columnname_insert", "import_columnname_iud", "import_columnname_source", "import_columnname_update", "import_default_sessions", "import_history_database", "import_history_table", "import_max_sessions", "import_process_empty", "import_stage_disable", "import_staging_database", "import_staging_table", "import_start_disable", "import_work_database", "import_work_table", "kafka_brokers", "kafka_saslmechanism", "kafka_securityprotocol", "kafka_topic", "kafka_trustcafile", "post_airflow_dag_operations", "post_data_to_kafka", "post_data_to_kafka_extended", "post_data_to_rest", "post_data_to_rest_extended", "post_data_to_awssns", "post_data_to_awssns_extended", "post_data_to_awssns_topic", "restserver_admin_user", "restserver_authentication_method", "restserver_token_ttl", "rest_timeout", "rest_trustcafile", "rest_url", "rest_verifyssl", "spark_max_executors", "timezone" ]

		self.createDefaultAdminUser()


	def disconnectDBImportDB(self):
		""" Disconnects from the database and removes all sessions and engine """
		log = logging.getLogger(self.logger)
		self.common_config.disconnectSQLAlchemy(logger=self.logger)
		self.common_config.configDBSession = None


	def getDBImportSession(self):
		log = logging.getLogger(self.logger)
		if self.common_config.configDBSession == None:
			if self.common_config.connectSQLAlchemy(exitIfFailure=False, logger=self.logger) == False:
				raise SQLerror("Can't connect to DBImport database")

		return self.common_config.configDBSession()


	def disconnectRemoteSession(self, instance):
		""" Disconnects from the remote database and removes all sessions and engine """
		log = logging.getLogger(self.logger)

		try:
			engine = self.remoteDBImportEngines.get(instance)
			if engine != None:
				log.info("Disconnecting from remote DBImport database for '%s'"%(instance))
				engine.dispose()
			self.remoteDBImportEngines.pop(instance)
			self.remoteDBImportSessions.pop(instance)
		except KeyError:
			log.debug("Cant remove DBImport session or engine. Key does not exist")

	def createDefaultAdminUser(self):
		""" Creates the default admin user in the auth_users table if it doesnt exist """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None
		
		defaultAdminUser = self.common_config.getConfigValue("restserver_admin_user")
		userObject = self.getUser(defaultAdminUser)
		if userObject == None:
			# User does not exists, so lets create it
			log.info("Default admin user does not exists in the 'auth_users' table. Creating it with password 'admin'")

			password_encoded = "admin".encode('utf-8')
			salt = bcrypt.gensalt()
			hashed_password = bcrypt.hashpw(password=password_encoded, salt=salt).decode('utf-8')

			query = sa.insert(configSchema.authUsers).values(
				username=defaultAdminUser,
				password=hashed_password)
			session.execute(query)
			session.commit()

		session.close()


	def createUser(self, user):
		""" Creates a user in the auth_users table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None
		
		log.info("User '%s' created"%(user.username))

		query = sa.insert(configSchema.authUsers).values(
			username=user.username,
			password=user.password,
			disabled=user.disabled,
			fullname=user.fullname,
			department=user.department,
			email=user.email)
		session.execute(query)
		session.commit()

		session.close()


	def getUser(self, username):
		""" Returns a dict with user information from the database """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		tableAuthUsers = aliased(configSchema.authUsers)
		authUser = (session.query(
					tableAuthUsers.username,
					tableAuthUsers.password,
					tableAuthUsers.disabled,
					tableAuthUsers.fullname,
					tableAuthUsers.department,
					tableAuthUsers.email
				)
				.select_from(tableAuthUsers)
				.filter(tableAuthUsers.username == username)
				.first()
			)

		session.close()
		user = None

		if authUser != None:
			user = {}
			user["username"] = authUser[0]
			user["password"] = authUser[1]
			user["disabled"] = authUser[2]
			user["fullname"] = authUser[3]
			user["department"] = authUser[4]
			user["email"] = authUser[5]

		return user

	def updateUser(self, user, passwordChanged, currentUser):
		""" Update a user object """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		if passwordChanged == False:
			log.info("User '%s' updated details for user '%s'"%(currentUser, user["username"]))
		else:
			log.info("User '%s' changed the password for user '%s'"%(currentUser, user["username"]))
		
		tableAuthUsers = aliased(configSchema.authUsers)
		session.execute(update(tableAuthUsers),
			[
				{
				"username": user["username"], 
				"password": user["password"], 
				"disabled": user["disabled"], 
				"fullname": user["fullname"], 
				"department": user["department"], 
				"email": user["email"] 
				}
			],
			)
		session.commit()

		session.close()

	def deleteUser(self, username):
		""" Delete a user """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None
		
		tableAuthUsers = aliased(configSchema.authUsers)

		(session.query(tableAuthUsers)
			.filter(tableAuthUsers.username == username)
			.delete())
		session.commit()

		session.close()


	def getJDBCdrivers(self):
		""" Returns all JDBC Driver configuration """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		tableJDBCconnectionsDrivers = aliased(configSchema.jdbcConnectionsDrivers)
		jdbcConnectionsDrivers = (session.query(
					tableJDBCconnectionsDrivers.database_type,
					tableJDBCconnectionsDrivers.version,
					tableJDBCconnectionsDrivers.driver,
					tableJDBCconnectionsDrivers.classpath
				)
				.select_from(tableJDBCconnectionsDrivers)
				.order_by(tableJDBCconnectionsDrivers.database_type)
				.all()
			)


		listOfJDBCdrivers = []
		for row in jdbcConnectionsDrivers:
			jdbcDriver = {}
			jdbcDriver["databaseType"] = row[0]
			jdbcDriver["version"] = row[1]
			jdbcDriver["driver"] = row[2]
			jdbcDriver["classpath"] = row[3]
			listOfJDBCdrivers.append(jdbcDriver)

		jsonResult = json.loads(json.dumps(listOfJDBCdrivers))
		session.close()

		return jsonResult


	def updateJDBCdriver(self, jdbcDriver, currentUser):
		""" Returns all configuration items from the configuration table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		log.info("User '%s' updated JDBC driver configuration for '%s'"%(currentUser, getattr(configuration, "databaseType")))

		log.debug("databaseType: %s"%getattr(jdbcDriver, "databaseType"))
		log.debug("version: %s"%getattr(jdbcDriver, "version"))
		log.debug("driver: %s"%getattr(jdbcDriver, "driver"))
		log.debug("classpath: %s"%getattr(jdbcDriver, "classpath"))

		tableJDBCconnectionsDrivers = aliased(configSchema.jdbcConnectionsDrivers)
		result = "ok"
		returnCode = 200

		try:
			session.execute(update(tableJDBCconnectionsDrivers),
				[
					{
					"database_type": getattr(jdbcDriver, "databaseType"), 
					"version": getattr(jdbcDriver, "version"), 
					"driver": getattr(jdbcDriver, "driver"), 
					"classpath": getattr(jdbcDriver, "classpath") 
					}
				],
				)
			session.commit()
		# except SQLAlchemyError.StaleDataError:
		except sqlalchemy.orm.exc.StaleDataError:
			result = "JDBC Driver databaseType with version is not a supported combination"
			returnCode = 400
					
		resultDict = {}
		resultDict["status"] = result
		session.close()

		jsonResult = json.loads(json.dumps(resultDict))
		# return (jsonResult, returnCode)
		return (result, returnCode)

	def getConfiguration(self):
		""" Returns all configuration items from the configuration table """
		log = logging.getLogger(self.logger)


		# self.common_config.reconnectConfigDatabase(buffered=False)
		self.common_config.reconnectConfigDatabase(printReconnectMessage=False)

		resultDict = {}
		for key in self.allConfigKeys:
			resultDict[key] = self.common_config.getConfigValue(key)

		jsonResult = json.loads(json.dumps(resultDict))
		return jsonResult

	def updateConfiguration(self, configuration, currentUser):
		""" Update configuration items in the configuration table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		configurationTable = aliased(configSchema.configuration)

		# First check to make sure we have valid options in the changed options
		try:
			for key in self.allConfigKeys:
				if type(getattr(configuration, key)) in (str, bool, int):
					# At this point, only configuration items that exists in the class is iterrated
					if type(getattr(configuration, key)) == str:
						if getattr(configuration, key) is None or getattr(configuration, key).strip() == "":
							raise HTTPException(
								status_code=status.HTTP_400_BAD_REQUEST,
								detail="'%s' requres a string value"%(key))

					if key == "airflow_major_version" and getattr(configuration, key) not in(1, 2):
						raise HTTPException(
							status_code=status.HTTP_400_BAD_REQUEST,
							detail="only valid options for 'airflow_major_version' is '1' and '2'")
	
					if key == "restserver_authentication_method" and getattr(configuration, key) not in("local", "pam"):
						raise HTTPException(
							status_code=status.HTTP_400_BAD_REQUEST,
							detail="only valid options for 'restserver_authentication_method' is 'local' and 'pam'")
		except AttributeError as e:
			print(e)
			raise HTTPException(
				status_code=status.HTTP_400_BAD_REQUEST,
				detail="invalid configuration option")


		for key in self.allConfigKeys:
			if type(getattr(configuration, key)) in (str, bool, int):
				# At this point, only configuration items that exists in the class is iterrated
				log.info("User '%s' updated global configuration. %s = %s"%(currentUser, key, getattr(configuration, key)))

				valueColumn, boolValue = self.common_config.getConfigValueColumn(key)

				session.execute(update(configurationTable),
					[
						{"configKey": key, valueColumn: getattr(configuration, key) }
					],
					)
				session.commit()
					
		result = "ok"
		returnCode = 200
		resultDict = {}
		resultDict["status"] = "ok"
		session.close()

		jsonResult = json.loads(json.dumps(resultDict))
		# return jsonResult
		return (result, returnCode)

	def getAllConnections(self, listOnlyName):
		""" Returns all Connections """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		tableJDBCconnections = aliased(configSchema.jdbcConnections)
		jdbcConnections = (session.query(
					tableJDBCconnections.dbalias,
					tableJDBCconnections.jdbc_url
				)
				.select_from(tableJDBCconnections)
				.order_by(tableJDBCconnections.dbalias)
				.all()
			)

		listOfConnections = []
		for row in jdbcConnections:
			resultDict = {}
			resultDict["name"] = row[0]
			if listOnlyName == False:
				resultDict["connectionString"] = row[1]

				self.common_config.lookupConnectionAlias(row[0], decryptCredentials=False, jdbcURL=row[1])

				serverType = None
				if self.common_config.jdbc_servertype == constant.MYSQL:			serverType = "MySQL"
				if self.common_config.jdbc_servertype == constant.ORACLE:			serverType = "Oracle"	
				if self.common_config.jdbc_servertype == constant.MSSQL:			serverType = "MSSQL Server"
				if self.common_config.jdbc_servertype == constant.POSTGRESQL:		serverType = "PostgreSQL"
				if self.common_config.jdbc_servertype == constant.PROGRESS:			serverType = "Progress"
				if self.common_config.jdbc_servertype == constant.DB2_UDB:			serverType = "DB2 UDB"
				if self.common_config.jdbc_servertype == constant.DB2_AS400:		serverType = "DB2 AS400"
				if self.common_config.jdbc_servertype == constant.MONGO:			serverType = "MongoDB"
				if self.common_config.jdbc_servertype == constant.CACHEDB:			serverType = "Cache"
				if self.common_config.jdbc_servertype == constant.SNOWFLAKE:		serverType = "Snowflake"
				if self.common_config.jdbc_servertype == constant.AWS_S3:			serverType = "AWS S3"
				if self.common_config.jdbc_servertype == constant.INFORMIX:			serverType = "Informix"
				if self.common_config.jdbc_servertype == constant.SQLANYWHERE:		serverType = "SQL Anywhere"
				resultDict["serverType"] = serverType


			listOfConnections.append(resultDict)

		jsonResult = json.loads(json.dumps(listOfConnections))
		session.close()

		return jsonResult

	def updateConnection(self, connection, currentUser):
		""" Update or create a Connections """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		tableJDBCconnections = aliased(configSchema.jdbcConnections)
		result = "ok"
		returnCode = 200

		log.debug(connection)
		log.info("User '%s' updated/created connection '%s'"%(currentUser, getattr(connection, "name")))

		try:
			query = insert(configSchema.jdbcConnections).values(
				dbalias = getattr(connection, "name"),
				jdbc_url = getattr(connection, "connectionString"),
				private_key_path = getattr(connection, "privateKeyPath"),
				public_key_path = getattr(connection, "publicKeyPath"),
				credentials = getattr(connection, "credentials"),
				datalake_source = getattr(connection, "source"),
				force_string = getattr(connection, "forceString"),
				max_import_sessions = getattr(connection, "maxSessions"),
				create_datalake_import = getattr(connection, "createDatalakeImport"),
				timewindow_start = getattr(connection, "timeWindowStart"),
				timewindow_stop = getattr(connection, "timeWindowStop"),
				timewindow_timezone = getattr(connection, "timeWindowTimezone"),
				operator_notes = getattr(connection, "operatorNotes"),
				contact_info = getattr(connection, "contactInformation"),
				description = getattr(connection, "description"),
				owner = getattr(connection, "owner"),
				environment = getattr(connection, "environment"),
				seed_file = getattr(connection, "seedFile"),
				create_foreign_keys = getattr(connection, "createForeignKey"),
				atlas_discovery = getattr(connection, "atlasDiscovery"),
				atlas_include_filter = getattr(connection, "atlasIncludeFilter"),
				atlas_exclude_filter = getattr(connection, "atlasExcludeFilter"),
				atlas_last_discovery = getattr(connection, "atlasLastDiscovery"))

			query = query.on_duplicate_key_update(
				dbalias = getattr(connection, "name"),
				jdbc_url = getattr(connection, "connectionString"),
				private_key_path = getattr(connection, "privateKeyPath"),
				public_key_path = getattr(connection, "publicKeyPath"),
				credentials = getattr(connection, "credentials"),
				datalake_source = getattr(connection, "source"),
				force_string = getattr(connection, "forceString"),
				max_import_sessions = getattr(connection, "maxSessions"),
				create_datalake_import = getattr(connection, "createDatalakeImport"),
				timewindow_start = getattr(connection, "timeWindowStart"),
				timewindow_stop = getattr(connection, "timeWindowStop"),
				timewindow_timezone = getattr(connection, "timeWindowTimezone"),
				operator_notes = getattr(connection, "operatorNotes"),
				contact_info = getattr(connection, "contactInformation"),
				description = getattr(connection, "description"),
				owner = getattr(connection, "owner"),
				environment = getattr(connection, "environment"),
				seed_file = getattr(connection, "seedFile"),
				create_foreign_keys = getattr(connection, "createForeignKey"),
				atlas_discovery = getattr(connection, "atlasDiscovery"),
				atlas_include_filter = getattr(connection, "atlasIncludeFilter"),
				atlas_exclude_filter = getattr(connection, "atlasExcludeFilter"),
				atlas_last_discovery = getattr(connection, "atlasLastDiscovery"))

			session.execute(query)
			session.commit()
		except SQLerror as err:
			log.error(str(err))
			log.error(column)

			result = str(err)
			returnCode = 500

		session.close()
		return (result, returnCode)

	def getConnection(self, connection):
		""" Returns all Connections """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		tableJDBCconnections = aliased(configSchema.jdbcConnections)
		row = (session.query(
					tableJDBCconnections.dbalias,
					tableJDBCconnections.jdbc_url,
					tableJDBCconnections.private_key_path,
					tableJDBCconnections.public_key_path,
					tableJDBCconnections.credentials,
					tableJDBCconnections.datalake_source,
					tableJDBCconnections.force_string,
					tableJDBCconnections.max_import_sessions,
					tableJDBCconnections.create_datalake_import,
					tableJDBCconnections.timewindow_start,
					tableJDBCconnections.timewindow_stop,
					tableJDBCconnections.timewindow_timezone,
					tableJDBCconnections.operator_notes,
					tableJDBCconnections.contact_info,
					tableJDBCconnections.description,
					tableJDBCconnections.owner,
					tableJDBCconnections.environment,
					tableJDBCconnections.seed_file,
					tableJDBCconnections.create_foreign_keys,
					tableJDBCconnections.atlas_discovery,
					tableJDBCconnections.atlas_include_filter,
					tableJDBCconnections.atlas_exclude_filter,
					tableJDBCconnections.atlas_last_discovery
				)
				.select_from(tableJDBCconnections)
				.filter(tableJDBCconnections.dbalias == connection)
				.one_or_none()
			)

		if row == None:
			raise HTTPException(
				status_code=status.HTTP_404_NOT_FOUND,
				detail="connection does not exist")

		resultDict = {}
		resultDict["name"] = row[0]
		resultDict["connectionString"] = row[1]
		resultDict["private_key_path"] = row[2]
		resultDict["public_key_path"] = row[3]
		resultDict["credentials"] = row[4]
		resultDict["source"] = row[5]
		resultDict["forceString"] = row[6]
		if resultDict["forceString"] != 0 and resultDict["forceString"] != 1 and resultDict["forceString"] != -1:
			resultDict["forceString"] = -1
		resultDict["maxSessions"] = row[7]
		resultDict["createDatalakeImport"] = row[8]
		try:
			resultDict["timeWindowStart"] = row[9].strftime('%H:%M:%S')
		except AttributeError:
			resultDict["timeWindowStart"] = None
		try:
			resultDict["timeWindowStop"] = row[10].strftime('%H:%M:%S')
		except AttributeError:
			resultDict["timeWindowStop"] = None
		resultDict["timeWindowTimezone"] = row[11]
		resultDict["operatorNotes"] = row[12]
		resultDict["contactInformation"] = row[13]
		resultDict["description"] = row[14]
		resultDict["owner"] = row[15]
		resultDict["environment"] = row[16]
		resultDict["seedFile"] = row[17]
		resultDict["createForeignKey"] = row[18]
		resultDict["atlasDiscovery"] = row[19]
		resultDict["atlasIncludeFilter"] = row[20]
		resultDict["atlasExcludeFilter"] = row[21]
		try:
			resultDict["atlasLastDiscovery"] = row[22].strftime('%Y-%m-%d %H:%M:%S')
		except AttributeError:
			resultDict["atlasLastDiscovery"] = None

		jsonResult = json.loads(json.dumps(resultDict))
		session.close()

		return jsonResult

	def deleteConnection(self, connection, currentUser):
		""" Delete a connection """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		result = "ok"
		returnCode = 200

		log.info("User '%s' deleted connection '%s'"%(currentUser, connection))

		# Fetch the tableID as that is required to update/create data in import_columns
		row = (session.query(configSchema.jdbcConnections.dbalias)
				.select_from(configSchema.jdbcConnections)
				.filter(configSchema.jdbcConnections.dbalias == connection)
				.one_or_none()
				)
		session.commit()

		if row == None:
			result = "Connection does not exist"
			returnCode = 404
		else:
			(session.query(configSchema.jdbcConnections)
				.filter(configSchema.jdbcConnections.dbalias == connection)
				.delete())
			session.commit()

		session.close()
		return (result, returnCode)


	def getAllImportDatabases(self):
		""" Returns all databases that have imports configured in them """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importTables = aliased(configSchema.importTables)

		importTableDBs = (session.query(
					importTables.hive_db,
					func.count(importTables.hive_db),
					func.max(importTables.last_update_from_source),
					func.sum(importTables.sqoop_last_size),
					func.sum(importTables.sqoop_last_rows)
				)
				.select_from(importTables)
				.group_by(importTables.hive_db)
				.all()
			)

		listOfDBs = []
		for row in importTableDBs:
			resultDict = {}
			resultDict["name"] = row[0]
			resultDict["tables"] = row[1]
			if row[2] == None:
				resultDict["lastImport"] = ""
			else:
				resultDict["lastImport"] = row[2].strftime('%Y-%m-%d %H:%M:%S')

			if row[3] == None:
				resultDict["lastSize"] = 0
			else:
				resultDict["lastSize"] = int(row[3])

			if row[4] == None:
				resultDict["lastRows"] = 0
			else:
				resultDict["lastRows"] = int(row[4])
			listOfDBs.append(resultDict)

		jsonResult = json.loads(json.dumps(listOfDBs))
		session.close()

		return jsonResult
	

	def getImportTablesInDatabase(self, database):
		""" Returns all import tables in a specific database """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importTables = aliased(configSchema.importTables)
		listOfTables = []

		# Return a list of Hive tables with details
		importTablesData = (session.query(
					importTables.hive_db,
					importTables.hive_table,
					importTables.dbalias,
					importTables.source_schema,
					importTables.source_table,
					importTables.import_phase_type,
					importTables.etl_phase_type,
					importTables.import_tool,
					importTables.etl_engine,
					importTables.last_update_from_source
				)
				.select_from(importTables)
				.filter(importTables.hive_db == database)
				.all()
			)

		for row in importTablesData:
			resultDict = {}
			resultDict['database'] = row[0]
			resultDict['table'] = row[1]
			resultDict['connection'] = row[2]
			resultDict['sourceSchema'] = row[3]
			resultDict['sourceTable'] = row[4]
			resultDict['importPhaseType'] = row[5]
			resultDict['etlPhaseType'] = row[6]
			resultDict['importTool'] = row[7]
			resultDict['etlEngine'] = row[8]
			try:
				resultDict['lastUpdateFromSource'] = row[9].strftime("%Y-%m-%d %H:%M:%S")
			except AttributeError:
				resultDict['lastUpdateFromSource'] = None

			listOfTables.append(resultDict)


		jsonResult = json.loads(json.dumps(listOfTables))
		session.close()

		return jsonResult
	

	def getImportTableDetails(self, database, table):
		""" Returns all import table details """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importTables = aliased(configSchema.importTables)

		# Return a list of Hive tables with details
		row = (session.query(
					importTables.hive_db,
					importTables.hive_table,
					importTables.dbalias,
					importTables.source_schema,
					importTables.source_table,
					importTables.import_phase_type,
					importTables.etl_phase_type,
					importTables.import_tool,
					importTables.etl_engine,
					importTables.last_update_from_source,
					importTables.sqoop_sql_where_addition,
					importTables.nomerge_ingestion_sql_addition,
					importTables.include_in_airflow,
					importTables.airflow_priority,
					importTables.validate_import,
					importTables.validationMethod,
					importTables.validate_source,
					importTables.validate_diff_allowed,
					importTables.validationCustomQuerySourceSQL,
					importTables.validationCustomQueryHiveSQL,
					importTables.validationCustomQueryValidateImportTable,
					importTables.truncate_hive,
					importTables.mappers,
					importTables.soft_delete_during_merge,
					importTables.source_rowcount,
					importTables.source_rowcount_incr,
					importTables.hive_rowcount,
					importTables.validationCustomQuerySourceValue,
					importTables.validationCustomQueryHiveValue,
					importTables.incr_mode,
					importTables.incr_column,
					importTables.incr_validation_method,
					importTables.incr_minvalue,
					importTables.incr_maxvalue,
					importTables.incr_minvalue_pending,
					importTables.incr_maxvalue_pending,
					importTables.pk_column_override,
					importTables.pk_column_override_mergeonly,
					importTables.hive_merge_heap,
					importTables.hive_split_count,
					importTables.spark_executor_memory,
					importTables.spark_executors,
					importTables.split_by_column,
					importTables.sqoop_query,
					importTables.sqoop_options,
					importTables.sqoop_last_size,
					importTables.sqoop_last_rows,
					importTables.sqoop_last_mappers,
					importTables.sqoop_last_execution,
					importTables.sqoop_use_generated_sql,
					importTables.sqoop_allow_text_splitter,
					importTables.force_string,
					importTables.comment,
					importTables.generated_hive_column_definition,
					importTables.generated_sqoop_query,
					importTables.generated_sqoop_options,
					importTables.generated_pk_columns,
					importTables.generated_foreign_keys,
					importTables.datalake_source,
					importTables.operator_notes,
					importTables.copy_finished,
					importTables.copy_slave,
					importTables.create_foreign_keys,
					importTables.invalidate_impala,
					importTables.custom_max_query,
					importTables.mergeCompactionMethod,
					importTables.sourceTableType,
					importTables.import_database,
					importTables.import_table,
					importTables.history_database,
					importTables.history_table
				)
				.select_from(importTables)
				.filter((importTables.hive_db == database) & (importTables.hive_table == table))
				.one_or_none()
			)

		if row == None:
			raise HTTPException(
				status_code=status.HTTP_404_NOT_FOUND,
				detail="import table does not exist")

		resultDict = {}
		resultDict['database'] = row[0]
		resultDict['table'] = row[1]
		resultDict['connection'] = row[2]
		resultDict['sourceSchema'] = row[3]
		resultDict['sourceTable'] = row[4]
		resultDict['importPhaseType'] = row[5]
		resultDict['etlPhaseType'] = row[6]
		resultDict['importTool'] = row[7]
		resultDict['etlEngine'] = row[8]
		try:
			resultDict['lastUpdateFromSource'] = row[9].strftime("%Y-%m-%d %H:%M:%S")
		except AttributeError:
			resultDict['lastUpdateFromSource'] = None
		resultDict['sqlWhereAddition'] = row[10]
		resultDict['nomergeIngestionSqlAddition'] = row[11]
		resultDict['includeInAirflow'] = row[12]
		resultDict['airflowPriority'] = row[13]
		resultDict['validateImport'] = row[14]
		resultDict['validationMethod'] = row[15]
		resultDict['validateSource'] = row[16]
		resultDict['validateDiffAllowed'] = row[17]
		resultDict['validationCustomQuerySourceSQL'] = row[18]
		resultDict['validationCustomQueryHiveSQL'] = row[19]
		resultDict['validationCustomQueryValidateImportTable'] = row[20]
		resultDict['truncateTable'] = row[21]
		resultDict['mappers'] = row[22]
		resultDict['softDeleteDuringMerge'] = row[23]
		resultDict['sourceRowcount'] = row[24]
		resultDict['sourceRowcountIncr'] = row[25]
		resultDict['targetRowcount'] = row[26]
		resultDict['validationCustomQuerySourceValue'] = row[27]
		resultDict['validationCustomQueryHiveValue'] = row[28]
		if row[29] == None or row[29] == "":       
			resultDict['incrMode'] = "lastmodified"
		else:
			resultDict['incrMode'] = row[29]
		resultDict['incrColumn'] = row[30]
		resultDict['incrValidationMethod'] = row[31]
		resultDict['incrMinvalue'] = row[32]
		resultDict['incrMaxvalue'] = row[33]
		resultDict['incrMinvaluePending'] = row[34]
		resultDict['incrMaxvaluePending'] = row[35]
		resultDict['pkColumnOverride'] = row[36]
		resultDict['pkColumnOverrideMergeonly'] = row[37]
		resultDict['mergeHeap'] = row[38]
		resultDict['splitCount'] = row[39]
		resultDict['sparkExecutorMemory'] = row[40]
		resultDict['sparkExecutors'] = row[41]
		resultDict['splitByColumn'] = row[42]
		resultDict['customQuery'] = row[43]
		resultDict['sqoopOptions'] = row[44]
		resultDict['lastSize'] = row[45]
		resultDict['lastRows'] = row[46]
		resultDict['lastMappers'] = row[47]
		resultDict['lastExecution'] = row[48]
		if row[49] == -1:       # -1 is not allowed, but default in the database. Setting these to 1
			resultDict['useGeneratedSql'] = 1
		else:
			resultDict['useGeneratedSql'] = row[49]
		resultDict['allowTextSplitter'] = row[50]
		resultDict['forceString'] = row[51]
		if resultDict["forceString"] != 0 and resultDict["forceString"] != 1 and resultDict["forceString"] != -1:
			resultDict["forceString"] = -1
		resultDict['comment'] = row[52]
		resultDict['generatedHiveColumnDefinition'] = row[53]
		resultDict['generatedSqoopQuery'] = row[54]
		resultDict['generatedSqoopOptions'] = row[55]
		resultDict['generatedPkColumns'] = row[56]
		resultDict['generatedForeignKeys'] = row[57]
		resultDict['datalakeSource'] = row[58]
		resultDict['operatorNotes'] = row[59]
		try:
			resultDict['copyFinished'] = row[60].strftime("%Y-%m-%d %H:%M:%S")
		except AttributeError:
			resultDict['copyFinished'] = None
		resultDict['copySlave'] = row[61]
		resultDict['createForeignKeys'] = row[62]
		resultDict['invalidateImpala'] = row[63]
		resultDict['customMaxQuery'] = row[64]
		resultDict['mergeCompactionMethod'] = row[65]
		resultDict['sourceTableType'] = row[66]
		resultDict['importDatabase'] = row[67]
		resultDict['importTable'] = row[68]
		resultDict['historyDatabase'] = row[69]
		resultDict['historyTable'] = row[70]

		resultDict["columns"] = self.getImportTableColumns(database, table)

		jsonResult = json.loads(json.dumps(resultDict))
		session.close()

		return jsonResult

	def deleteImportTable(self, database, table, currentUser):
		""" Update or create an import table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importColumns = aliased(configSchema.importColumns)
		importTables = aliased(configSchema.importTables)
		result = "ok"
		returnCode = 200

		log.info("User '%s' deleted import table '%s.%s'"%(currentUser, database, table))

		# Fetch the tableID as that is required to update/create data in import_columns
		row = (session.query(configSchema.importTables.table_id)
				.select_from(configSchema.importTables)
				.filter((configSchema.importTables.hive_db == database) & (configSchema.importTables.hive_table == table))
				.one_or_none()
				)
		session.commit()

		if row == None:
			result = "Table does not exist"
			returnCode = 404
		else:
			tableID = row[0]

			(session.query(configSchema.importTables)
				.filter(configSchema.importTables.table_id == tableID)
				.delete())
			session.commit()

		session.close()
		return (result, returnCode)

	def updateImportTable(self, table, currentUser):
		""" Update or create an import table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importColumns = aliased(configSchema.importColumns)
		importTables = aliased(configSchema.importTables)
		result = "ok"
		returnCode = 200

		log.debug(table)
		log.info("User '%s' updated/created import table '%s.%s'"%(currentUser, getattr(table, "database"), getattr(table, "table")))

		# Set default values
		if getattr(table, "includeInAirflow") == None:							setattr(table, "includeInAirflow", 1)
		if getattr(table, "validateImport") == None:							setattr(table, "validateImport", 1)	
		if getattr(table, "validationMethod") == None:							setattr(table, "validationMethod", "rowCount")
		if getattr(table, "validateSource") == None:							setattr(table, "validateSource", "query")
		if getattr(table, "validateDiffAllowed") == None:						setattr(table, "validateDiffAllowed", -1)
		if getattr(table, "validationCustomQueryValidateImportTable") == None:	setattr(table, "validationCustomQueryValidateImportTable", 1)
		if getattr(table, "incrValidationMethod") == None:						setattr(table, "incrValidationMethod", "full")
		if getattr(table, "truncateTable") == None:								setattr(table, "truncateTable", 1)
		if getattr(table, "mappers") == None:									setattr(table, "mappers", -1)
		if getattr(table, "softDeleteDuringMerge") == None:						setattr(table, "softDeleteDuringMerge", 0)
		if getattr(table, "useGeneratedSql") == None:							setattr(table, "useGeneratedSql", -1)
		if getattr(table, "allowTextSplitter") == None:							setattr(table, "allowTextSplitter", 0)
		if getattr(table, "forceString") == None:								setattr(table, "forceString", -1)
		if getattr(table, "invalidateImpala") == None:							setattr(table, "invalidateImpala", -1)
		if getattr(table, "mergeCompactionMethod") == None:						setattr(table, "mergeCompactionMethod", "default")
		if getattr(table, "createForeignKeys") == None:							setattr(table, "createForeignKeys", -1)

		try:
			query = insert(configSchema.importTables).values(
				hive_db = getattr(table, "database"),
				hive_table = getattr(table, "table"),
				dbalias = getattr(table, "connection"),
				source_schema = getattr(table, "sourceSchema"),
				source_table = getattr(table, "sourceTable"),
				import_phase_type = getattr(table, "importPhaseType"),
				etl_phase_type = getattr(table, "etlPhaseType"),
				import_tool = getattr(table, "importTool"),
				etl_engine = getattr(table, "etlEngine"),
				last_update_from_source = getattr(table, "lastUpdateFromSource"),
				sqoop_sql_where_addition = getattr(table, "sqlWhereAddition"),
				nomerge_ingestion_sql_addition = getattr(table, "nomergeIngestionSqlAddition"),
				include_in_airflow = getattr(table, "includeInAirflow"),
				airflow_priority = getattr(table, "airflowPriority"),
				validate_import = getattr(table, "validateImport"),
				validationMethod = getattr(table, "validationMethod"),
				validate_source = getattr(table, "validateSource"),
				validate_diff_allowed = getattr(table, "validateDiffAllowed"),
				validationCustomQuerySourceSQL = getattr(table, "validationCustomQuerySourceSQL"),
				validationCustomQueryHiveSQL = getattr(table, "validationCustomQueryHiveSQL"),
				validationCustomQueryValidateImportTable = getattr(table, "validationCustomQueryValidateImportTable"),
				truncate_hive = getattr(table, "truncateTable"),
				mappers = getattr(table, "mappers"),
				soft_delete_during_merge = getattr(table, "softDeleteDuringMerge"),
				incr_mode = getattr(table, "incrMode"),
				incr_column = getattr(table, "incrColumn"),
				incr_validation_method = getattr(table, "incrValidationMethod"),
				pk_column_override = getattr(table, "pkColumnOverride"),
				pk_column_override_mergeonly = getattr(table, "pkColumnOverrideMergeonly"),
				hive_merge_heap = getattr(table, "mergeHeap"),
				hive_split_count = getattr(table, "splitCount"),
				spark_executor_memory = getattr(table, "sparkExecutorMemory"),
				spark_executors = getattr(table, "sparkExecutors"),
				split_by_column = getattr(table, "splitByColumn"),
				sqoop_query = getattr(table, "customQuery"),
				sqoop_options = getattr(table, "sqoopOptions"),
				sqoop_use_generated_sql = getattr(table, "useGeneratedSql"),
				sqoop_allow_text_splitter = getattr(table, "allowTextSplitter"),
				force_string = getattr(table, "forceString"),
				comment = getattr(table, "comment"),
				datalake_source = getattr(table, "datalakeSource"),
				operator_notes = getattr(table, "operatorNotes"),
				create_foreign_keys = getattr(table, "createForeignKeys"),
				invalidate_impala = getattr(table, "invalidateImpala"),
				custom_max_query = getattr(table, "customMaxQuery"),
				mergeCompactionMethod = getattr(table, "mergeCompactionMethod"),
				sourceTableType = getattr(table, "sourceTableType"),
				import_database = getattr(table, "importDatabase"),
				import_table = getattr(table, "importTable"),
				history_database = getattr(table, "historyDatabase"),
				history_table = getattr(table, "historyTable"))

			query = query.on_duplicate_key_update(
				dbalias = getattr(table, "connection"),
				source_schema = getattr(table, "sourceSchema"),
				source_table = getattr(table, "sourceTable"),
				import_phase_type = getattr(table, "importPhaseType"),
				etl_phase_type = getattr(table, "etlPhaseType"),
				import_tool = getattr(table, "importTool"),
				etl_engine = getattr(table, "etlEngine"),
				last_update_from_source = getattr(table, "lastUpdateFromSource"),
				sqoop_sql_where_addition = getattr(table, "sqlWhereAddition"),
				nomerge_ingestion_sql_addition = getattr(table, "nomergeIngestionSqlAddition"),
				include_in_airflow = getattr(table, "includeInAirflow"),
				airflow_priority = getattr(table, "airflowPriority"),
				validate_import = getattr(table, "validateImport"),
				validationMethod = getattr(table, "validationMethod"),
				validate_source = getattr(table, "validateSource"),
				validate_diff_allowed = getattr(table, "validateDiffAllowed"),
				validationCustomQuerySourceSQL = getattr(table, "validationCustomQuerySourceSQL"),
				validationCustomQueryHiveSQL = getattr(table, "validationCustomQueryHiveSQL"),
				validationCustomQueryValidateImportTable = getattr(table, "validationCustomQueryValidateImportTable"),
				truncate_hive = getattr(table, "truncateTable"),
				mappers = getattr(table, "mappers"),
				soft_delete_during_merge = getattr(table, "softDeleteDuringMerge"),
				incr_mode = getattr(table, "incrMode"),
				incr_column = getattr(table, "incrColumn"),
				incr_validation_method = getattr(table, "incrValidationMethod"),
				pk_column_override = getattr(table, "pkColumnOverride"),
				pk_column_override_mergeonly = getattr(table, "pkColumnOverrideMergeonly"),
				hive_merge_heap = getattr(table, "mergeHeap"),
				hive_split_count = getattr(table, "splitCount"),
				spark_executor_memory = getattr(table, "sparkExecutorMemory"),
				spark_executors = getattr(table, "sparkExecutors"),
				split_by_column = getattr(table, "splitByColumn"),
				sqoop_query = getattr(table, "customQuery"),
				sqoop_options = getattr(table, "sqoopOptions"),
				sqoop_use_generated_sql = getattr(table, "useGeneratedSql"),
				sqoop_allow_text_splitter = getattr(table, "allowTextSplitter"),
				force_string = getattr(table, "forceString"),
				comment = getattr(table, "comment"),
				datalake_source = getattr(table, "datalakeSource"),
				operator_notes = getattr(table, "operatorNotes"),
				create_foreign_keys = getattr(table, "createForeignKeys"),
				invalidate_impala = getattr(table, "invalidateImpala"),
				custom_max_query = getattr(table, "customMaxQuery"),
				mergeCompactionMethod = getattr(table, "mergeCompactionMethod"),
				sourceTableType = getattr(table, "sourceTableType"),
				import_database = getattr(table, "importDatabase"),
				import_table = getattr(table, "importTable"),
				history_database = getattr(table, "historyDatabase"),
				history_table = getattr(table, "historyTable"))

			session.execute(query)
			session.commit()
		except SQLerror as err:
			log.error(str(err))
			log.error(column)

			result = str(err)
			returnCode = 500

			session.close()
			return (result, returnCode)


		# Fetch the tableID as that is required to update/create data in import_columns
		row = (session.query(
					importTables.table_id
				)
				.select_from(importTables)
				.filter((importTables.hive_db == getattr(table, "database")) & (importTables.hive_table == getattr(table, "table")))
				.one()
			)

		session.execute(query)
		tableID = row[0]

		columns = getattr(table, "columns")
		for column in columns:
			# PK in import_columns is not the most optimal. So we need to check first if it exists and then insert or update. Upsert is not available
			row = (session.query(
						importColumns.column_id
					)
					.select_from(importColumns)
					.filter((importColumns.table_id == tableID) & (importColumns.column_name == getattr(column, "columnName")))
					.one_or_none()
				)
				
			try:
				if row == None:
					log.debug("Column does not exist")
	
					query = sa.insert(configSchema.importColumns).values(
						table_id = tableID,
						hive_db = getattr(table, "database"),
						hive_table = getattr(table, "table"),
						column_name = getattr(column, "columnName"),
						column_order = getattr(column, "columnOrder"),
						source_column_name = getattr(column, "sourceColumnName"),
						column_type = getattr(column, "columnType"),
						source_column_type = getattr(column, "sourceColumnType"),
						source_database_type = getattr(column, "sourceDatabaseType"),
						column_name_override = getattr(column, "columnNameOverride"),
						column_type_override = getattr(column, "columnTypeOverride"),
						sqoop_column_type = getattr(column, "sqoopColumnType"),
						sqoop_column_type_override = getattr(column, "sqoopColumnTypeOverride"),
						force_string = getattr(column, "forceString"),
						include_in_import = getattr(column, "includeInImport"),
						source_primary_key = getattr(column, "sourcePrimaryKey"),
						last_update_from_source = getattr(column, "lastUpdateFromSource"),
						comment = getattr(column, "comment"),
						operator_notes = getattr(column, "operatorNotes"),
						anonymization_function = getattr(column, "anonymizationFunction"))
					session.execute(query)

				else:
					columnID = row[0]
					log.debug("Import column with id '%s' was updated"%(columnID))
					session.execute(update(importColumns),
						[
							{
							"table_id": tableID,
							"column_id": columnID,
							"hive_db": getattr(table, "database"),
							"hive_table": getattr(table, "table"),
							"column_name": getattr(column, "columnName"),
							"column_order": getattr(column, "columnOrder"),
							"source_column_name": getattr(column, "sourceColumnName"),
							"column_type": getattr(column, "columnType"),
							"source_column_type": getattr(column, "sourceColumnType"),
							"source_database_type": getattr(column, "sourceDatabaseType"),
							"column_name_override": getattr(column, "columnNameOverride"),
							"column_type_override": getattr(column, "columnTypeOverride"),
							"sqoop_column_type": getattr(column, "sqoopColumnType"),
							"sqoop_column_type_override": getattr(column, "sqoopColumnTypeOverride"),
							"force_string": getattr(column, "forceString"),
							"include_in_import": getattr(column, "includeInImport"),
							"source_primary_key": getattr(column, "sourcePrimaryKey"),
							"last_update_from_source": getattr(column, "lastUpdateFromSource"),
							"comment": getattr(column, "comment"),
							"operator_notes": getattr(column, "operatorNotes"),
							"anonymization_function": getattr(column, "anonymizationFunction")
							}
						],
						)
	
				session.commit()
			except SQLerror as err:
				log.error(str(err))
				log.error(column)

				result = str(err)
				returnCode = 500

				session.close()
				return (result, returnCode)

			log.debug(column)

		session.close()
		return (result, returnCode)

	def getImportTableColumns(self, database, table):
		""" Returns all columns in an import table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importColumns = aliased(configSchema.importColumns)
		listOfColumns = []

		# Return a list of Hive tables with details
		importColumnsData = (session.query(
					importColumns.hive_db,
					importColumns.hive_table,
    				importColumns.column_name,
    				importColumns.column_order,
    				importColumns.source_column_name,
    				importColumns.column_type,
    				importColumns.source_column_type,
    				importColumns.source_database_type,
    				importColumns.column_name_override,
    				importColumns.column_type_override,
    				importColumns.sqoop_column_type,
    				importColumns.sqoop_column_type_override,
    				importColumns.force_string,
    				importColumns.include_in_import,
    				importColumns.source_primary_key,
    				importColumns.last_update_from_source,
    				importColumns.comment,
    				importColumns.operator_notes,
    				importColumns.anonymization_function
				)
				.select_from(importColumns)
				.filter((importColumns.hive_db == database) & (importColumns.hive_table == table))
				.order_by(importColumns.column_order)
				.all()
			)

		for row in importColumnsData:
			resultDict = {}
#			resultDict['database'] = row[0]
#			resultDict['table'] = row[1]
			resultDict['columnName'] = row[2]
			resultDict['columnOrder'] = row[3]
			resultDict['sourceColumnName'] = row[4]
			resultDict['columnType'] = row[5]
			resultDict['sourceColumnType'] = row[6]
			resultDict['sourceDatabaseType'] = row[7]
			resultDict['columnNameOverride'] = row[8]
			resultDict['columnTypeOverride'] = row[9]
			resultDict['sqoopColumnType'] = row[10]
			resultDict['sqoopColumnTypeOverride'] = row[11]
			resultDict['forceString'] = row[12]
			if resultDict["forceString"] != 0 and resultDict["forceString"] != 1 and resultDict["forceString"] != -1:
				resultDict["forceString"] = -1
			resultDict['includeInImport'] = row[13]
			resultDict['sourcePrimaryKey'] = row[14]
			try:
				resultDict['lastUpdateFromSource'] = row[15].strftime("%Y-%m-%d %H:%M:%S")
			except AttributeError:
				resultDict['lastUpdateFromSource'] = None
			resultDict['comment'] = row[16]
			resultDict['operatorNotes'] = row[17]
			resultDict['anonymizationFunction'] = row[18]

			listOfColumns.append(resultDict)
	
#		jsonResult = json.loads(json.dumps(listOfColumns))
		session.close()

#		return jsonResult
		return listOfColumns



	def getExportConnections(self):
		""" Returns all connections that have exports configured in them """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		exportTables = aliased(configSchema.exportTables)

		exportTableDBs = (session.query(
					exportTables.dbalias,
					func.count(exportTables.dbalias),
					func.max(exportTables.last_update_from_hive),
					func.sum(exportTables.sqoop_last_rows)
				)
				.select_from(exportTables)
				.group_by(exportTables.dbalias)
				.all()
			)

		listOfConnections = []
		for row in exportTableDBs:
			connection = {}
			connection["name"] = row[0]
			connection["tables"] = row[1]
			if row[2] == None:
				connection["laportImport"] = ""
			else:
				connection["lastExport"] = row[2].strftime('%Y-%m-%d %H:%M:%S')

			if row[3] == None:
				connection["lastRows"] = 0
			else:
				connection["lastRows"] = int(row[3])
			listOfConnections.append(connection)

		jsonResult = json.loads(json.dumps(listOfConnections))
		session.close()

		return jsonResult
	

	def getExportTables(self, connection, schema = None):
		""" Returns all connections that have exports configured in them """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		exportTables = aliased(configSchema.exportTables)

		if schema == None:
			exportTableDBs = (session.query(
						exportTables.dbalias,
						exportTables.target_schema,
						exportTables.target_table,
						exportTables.hive_db,
						exportTables.hive_table,
						exportTables.export_type,
						exportTables.export_tool,
						exportTables.last_update_from_hive
					)
					.select_from(exportTables)
					.filter(exportTables.dbalias == connection)
					.all()
				)
		else:
			exportTableDBs = (session.query(
						exportTables.dbalias,
						exportTables.target_schema,
						exportTables.target_table,
						exportTables.hive_db,
						exportTables.hive_table,
						exportTables.export_type,
						exportTables.export_tool,
						exportTables.last_update_from_hive
					)
					.select_from(exportTables)
					.filter((exportTables.dbalias == connection) & (exportTables.target_schema == schema))
					.all()
				)

		listOfExports = []
		for row in exportTableDBs:
			exportTable = {}
			exportTable["connection"] = row[0]
			exportTable["targetSchema"] = row[1]
			exportTable["targetTable"] = row[2]
			exportTable["database"] = row[3]
			exportTable["table"] = row[4]
			exportTable["exportType"] = row[5]
			exportTable["exportTool"] = row[6]
			try:
				exportTable['lastUpdateFromHive'] = row[7].strftime("%Y-%m-%d %H:%M:%S")
			except AttributeError:
				exportTable['lastUpdateFromHive'] = None
			listOfExports.append(exportTable)

		jsonResult = json.loads(json.dumps(listOfExports))
		session.close()

		return jsonResult
	

	def getExportTableDetails(self, connection, schema, table):
		""" Returns all connections that have exports configured in them """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		exportTables = aliased(configSchema.exportTables)

		row = (session.query(
					exportTables.dbalias,
					exportTables.target_schema,
					exportTables.target_table,
					exportTables.export_type,
					exportTables.export_tool,
					exportTables.hive_db,
					exportTables.hive_table,
					exportTables.last_update_from_hive,
					exportTables.sql_where_addition,
					exportTables.include_in_airflow,
					exportTables.airflow_priority,
					exportTables.forceCreateTempTable,
					exportTables.validate_export,
					exportTables.validationMethod,
					exportTables.validationCustomQueryHiveSQL,
					exportTables.validationCustomQueryTargetSQL,
					exportTables.uppercase_columns,
					exportTables.truncate_target,
					exportTables.mappers,
					exportTables.hive_rowcount,
					exportTables.target_rowcount,
					exportTables.validationCustomQueryHiveValue,
					exportTables.validationCustomQueryTargetValue,
					exportTables.incr_column,
					exportTables.incr_validation_method,
					exportTables.incr_minvalue,
					exportTables.incr_maxvalue,
					exportTables.incr_minvalue_pending,
					exportTables.incr_maxvalue_pending,
					exportTables.sqoop_options,
					exportTables.sqoop_last_size,
					exportTables.sqoop_last_rows,
					exportTables.sqoop_last_mappers,
					exportTables.sqoop_last_execution,
					exportTables.hive_javaheap,
					exportTables.create_target_table_sql,
					exportTables.operator_notes
				)
				.select_from(exportTables)
				.filter((exportTables.dbalias == connection) & (exportTables.target_schema == schema) & (exportTables.target_table == table))
				.one_or_none()
			)

		if row == None:
			raise HTTPException(
				status_code=status.HTTP_404_NOT_FOUND,
				detail="export table does not exist")

		resultDict = {}
		resultDict['connection'] = row[0]
		resultDict['targetSchema'] = row[1]
		resultDict['targetTable'] = row[2]
		resultDict['exportType'] = row[3]
		resultDict['exportTool'] = row[4]
		resultDict['database'] = row[5]
		resultDict['table'] = row[6]
		try:
			resultDict['lastUpdateFromHive'] = row[7].strftime("%Y-%m-%d %H:%M:%S")
		except AttributeError:
			resultDict['lastUpdateFromHive'] = None
		resultDict['sqlWhereAddition'] = row[8]
		resultDict['includeInAirflow'] = row[9]
		resultDict['airflowPriority'] = row[10]
		resultDict['forceCreateTempTable'] = row[11]
		resultDict['validateExport'] = row[12]
		resultDict['validationMethod'] = row[13]
		resultDict['validationCustomQueryHiveSQL'] = row[14]
		resultDict['validationCustomQueryTargetSQL'] = row[15]
		resultDict['uppercaseColumns'] = row[16]
		resultDict['truncateTarget'] = row[17]
		resultDict['mappers'] = row[18]
		resultDict['tableRowcount'] = row[19]
		resultDict['targetRowcount'] = row[20]
		resultDict['validationCustomQueryHiveValue'] = row[21]
		resultDict['validationCustomQueryTargetValue'] = row[22]
		resultDict['incrColumn'] = row[23]
		resultDict['incrValidationMethod'] = row[24]
		resultDict['incrMinvalue'] = row[25]
		resultDict['incrMaxvalue'] = row[26]
		resultDict['incrMinvaluePending'] = row[27]
		resultDict['incrMaxvaluePending'] = row[28]
		resultDict['sqoopOptions'] = row[29]
		resultDict['lastSize'] = row[30]
		resultDict['lastRows'] = row[31]
		resultDict['lastMappers'] = row[32]
		resultDict['lastExecution'] = row[33]
		resultDict['javaHeap'] = row[34]
		resultDict['createTargetTableSql'] = row[35]
		resultDict['operatorNotes'] = row[36]

		resultDict["columns"] = self.getExportTableColumns(connection, schema, table)

		jsonResult = json.loads(json.dumps(resultDict))
		session.close()

		return jsonResult


	def getExportTableColumns(self, connection, schema, table):
		""" Returns all columns in an export table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		exportColumns = aliased(configSchema.exportColumns)
		exportTables = aliased(configSchema.exportTables)
		listOfColumns = []

		# Return a list of Hive tables with details
		exportColumnsData = (session.query(
					exportTables.dbalias,
					exportTables.target_schema,
					exportTables.target_table,
					exportColumns.column_name,
					exportColumns.column_type,
					exportColumns.column_order,
					exportColumns.target_column_name,
					exportColumns.target_column_type,
					exportColumns.last_update_from_hive,
					exportColumns.include_in_export,
					exportColumns.comment,
					exportColumns.operator_notes
				)
				.select_from(exportTables)
				.join(exportColumns, exportTables.table_id == exportColumns.table_id, isouter=False)
				.filter((exportTables.dbalias == connection) & (exportTables.target_schema == schema) & (exportTables.target_table == table))
				.all()
			)


		for row in exportColumnsData:
			resultDict = {}

#			resultDict['connection'] = row[0]
#			resultDict['targetSchema'] = row[1]
#			resultDict['targetTable'] = row[2]
			resultDict['columnName'] = row[3]
			resultDict['columnType'] = row[4]
			resultDict['columnOrder'] = row[5]
			resultDict['targetColumnName'] = row[6]
			resultDict['targetColumnType'] = row[7]
			try:
				resultDict['lastUpdateFromHive'] = row[8].strftime("%Y-%m-%d %H:%M:%S")
			except AttributeError:
				resultDict['lastUpdateFromHive'] = None
			resultDict['includeInExport'] = row[9]
			resultDict['comment'] = row[10]
			resultDict['operatorNotes'] = row[11]

			listOfColumns.append(resultDict)
	
		# jsonResult = json.loads(json.dumps(listOfColumns))
		session.close()

		# return jsonResult
		return listOfColumns


	def deleteExportTable(self, connection, schema, table, currentUser):
		""" Update or create an export table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		exportColumns = aliased(configSchema.exportColumns)
		exportTables = aliased(configSchema.exportTables)
		result = "ok"
		returnCode = 200

		log.info("User '%s' deleted export table '%s.%s' on connection '%s'"%(currentUser, schema, table, connection))

		# Fetch the tableID as that is required to update/create data in import_columns
		row = (session.query(configSchema.exportTables.table_id)
				.select_from(configSchema.exportTables)
				.filter(
					(configSchema.exportTables.dbalias == connection) & 
					(configSchema.exportTables.target_schema == schema) & 
					(configSchema.exportTables.target_table == table))
				.one_or_none()
				)
		session.commit()

		if row == None:
			result = "Table does not exist"
			returnCode = 404
		else:
			tableID = row[0]
#			result = str(tableID)

			(session.query(configSchema.exportTables)
				.filter(configSchema.exportTables.table_id == tableID)
				.delete())
			session.commit()

		session.close()
		return (result, returnCode)


	def updateExportTable(self, table, currentUser):
		""" Update or create an import table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		exportColumns = aliased(configSchema.exportColumns)
		exportTables = aliased(configSchema.exportTables)
		result = "ok"
		returnCode = 200

		log.debug(table)
		log.info("User '%s' updated/created export table '%s.%s' on connection '%s'"%(
			currentUser, 
			getattr(table, "targetSchema"), 
			getattr(table, "targetTable"), 
			getattr(table, "connection")))

		try:
			query = insert(configSchema.exportTables).values(
				dbalias = getattr(table, "connection"),
				target_schema = getattr(table, "targetSchema"),
				target_table = getattr(table, "targetTable"),
				export_type = getattr(table, "exportType"),
				export_tool = getattr(table, "exportTool"),
				hive_db = getattr(table, "database"),
				hive_table = getattr(table, "table"),
				last_update_from_hive = getattr(table, "lastUpdateFromHive"),
				sql_where_addition = getattr(table, "sqlWhereAddition"),
				include_in_airflow = getattr(table, "includeInAirflow"),
				airflow_priority = getattr(table, "airflowPriority"),
				forceCreateTempTable = getattr(table, "forceCreateTempTable"),
				validate_export = getattr(table, "validateExport"),
				validationMethod = getattr(table, "validationMethod"),
				validationCustomQueryHiveSQL = getattr(table, "validationCustomQueryHiveSQL"),
				validationCustomQueryTargetSQL = getattr(table, "validationCustomQueryTargetSQL"),
				uppercase_columns = getattr(table, "uppercaseColumns"),
				truncate_target = getattr(table, "truncateTarget"),
				mappers = getattr(table, "mappers"),
				incr_column = getattr(table, "incrColumn"),
				incr_validation_method = getattr(table, "incrValidationMethod"),
				sqoop_options = getattr(table, "sqoopOptions"),
				hive_javaheap = getattr(table, "javaHeap"),
				create_target_table_sql = getattr(table, "createTargetTableSql"),
				operator_notes = getattr(table, "operatorNotes"))

			query = query.on_duplicate_key_update(
				dbalias = getattr(table, "connection"),
				target_schema = getattr(table, "targetSchema"),
				target_table = getattr(table, "targetTable"),
				export_type = getattr(table, "exportType"),
				export_tool = getattr(table, "exportTool"),
				hive_db = getattr(table, "database"),
				hive_table = getattr(table, "table"),
				last_update_from_hive = getattr(table, "lastUpdateFromHive"),
				sql_where_addition = getattr(table, "sqlWhereAddition"),
				include_in_airflow = getattr(table, "includeInAirflow"),
				airflow_priority = getattr(table, "airflowPriority"),
				forceCreateTempTable = getattr(table, "forceCreateTempTable"),
				validate_export = getattr(table, "validateExport"),
				validationMethod = getattr(table, "validationMethod"),
				validationCustomQueryHiveSQL = getattr(table, "validationCustomQueryHiveSQL"),
				validationCustomQueryTargetSQL = getattr(table, "validationCustomQueryTargetSQL"),
				uppercase_columns = getattr(table, "uppercaseColumns"),
				truncate_target = getattr(table, "truncateTarget"),
				mappers = getattr(table, "mappers"),
				incr_column = getattr(table, "incrColumn"),
				incr_validation_method = getattr(table, "incrValidationMethod"),
				sqoop_options = getattr(table, "sqoopOptions"),
				hive_javaheap = getattr(table, "javaHeap"),
				create_target_table_sql = getattr(table, "createTargetTableSql"),
				operator_notes = getattr(table, "operatorNotes"))

			session.execute(query)
			session.commit()
		except SQLerror as err:
			log.error(str(err))
			log.error(column)

			result = str(err)
			returnCode = 500

			session.close()
			return (result, returnCode)


		# Fetch the tableID as that is required to update/create data in export_columns
		row = (session.query(
					exportTables.table_id
				)
				.select_from(exportTables)
				.filter(
					(exportTables.dbalias == getattr(table, "connection")) & 
					(exportTables.target_schema == getattr(table, "targetSchema")) &
					(exportTables.target_table == getattr(table, "targetTable")))
				.one()
			)

		session.execute(query)
		tableID = row[0]

		columns = getattr(table, "columns")
		for column in columns:
			# PK in export_columns is not the most optimal. So we need to check first if it exists and then insert or update. 
			# Upsert is not available
			row = (session.query(
						exportColumns.column_id
					)
					.select_from(exportColumns)
					.filter((exportColumns.table_id == tableID) & (exportColumns.column_name == getattr(column, "columnName")))
					.one_or_none()
				)
				
			try:
				if row == None:
					log.debug("Column does not exist")
	
					query = sa.insert(configSchema.exportColumns).values(
						table_id = tableID,
						column_name = getattr(column, "columnName"),
						column_type = getattr(column, "columnType"),
						column_order = getattr(column, "columnOrder"),
						hive_db = getattr(table, "database"),
						hive_table = getattr(table, "table"),
						target_column_name = getattr(column, "targetColumnName"),
						target_column_type = getattr(column, "targetColumnType"),
						last_update_from_hive = getattr(column, "lastUpdateFromHive"),
						include_in_export = getattr(column, "includeInExport"),
						comment = getattr(column, "comment"),
						operator_notes = getattr(column, "operatorNotes"))

					session.execute(query)

				else:
					columnID = row[0]
					log.debug("Export column with id '%s' was updated"%(columnID))
					session.execute(update(exportColumns),
						[
							{
							"table_id": tableID,
							"column_id": columnID,
							"column_name": getattr(column, "columnName"),
							"column_type": getattr(column, "columnType"),
							"column_order": getattr(column, "columnOrder"),
							"hive_db": getattr(table, "database"),
							"hive_table": getattr(table, "table"),
							"target_column_name": getattr(column, "targetColumnName"),
							"target_column_type": getattr(column, "targetColumnType"),
							"last_update_from_hive": getattr(column, "lastUpdateFromHive"),
							"include_in_export": getattr(column, "includeInExport"),
							"comment": getattr(column, "comment"),
							"operator_notes": getattr(column, "operatorNotes")
							}
						],
						)
	
				session.commit()
			except SQLerror as err:
				log.error(str(err))
				log.error(column)

				result = str(err)
				returnCode = 500

				session.close()
				return (result, returnCode)

			log.debug(column)

		session.close()
		return (result, returnCode)


	def getAllAirflowDags(self): 
		""" Returns all Airflow DAGs """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None


		airflowCustomDags = aliased(configSchema.airflowCustomDags)
		airflowExportDags = aliased(configSchema.airflowExportDags)
		airflowImportDags = aliased(configSchema.airflowImportDags)
		listOfDAGs = []
		processedDAGs = []

		airflowCustomDagsQuery = (session.query(
					airflowCustomDags.dag_name,
					sa.sql.expression.literal_column("'custom'").label("dag_type"),
					airflowCustomDags.schedule_interval,
					airflowCustomDags.auto_regenerate_dag,
					airflowCustomDags.operator_notes,
					airflowCustomDags.application_notes
				)
				.select_from(airflowCustomDags)
			)

		airflowImportDagsQuery = (session.query(
					airflowImportDags.dag_name,
					sa.sql.expression.literal_column("'import'").label("dag_type"),
					airflowImportDags.schedule_interval,
					airflowImportDags.auto_regenerate_dag,
					airflowImportDags.operator_notes,
					airflowImportDags.application_notes
				)
				.select_from(airflowImportDags)
			)

		airflowExportDagsQuery = (session.query(
					airflowExportDags.dag_name,
					sa.sql.expression.literal_column("'export'").label("dag_type"),
					airflowExportDags.schedule_interval,
					airflowExportDags.auto_regenerate_dag,
					airflowExportDags.operator_notes,
					airflowExportDags.application_notes
				)
				.select_from(airflowExportDags)
			)

		airflowDagsData = airflowCustomDagsQuery.union(airflowImportDagsQuery, airflowExportDagsQuery).all()


		for row in airflowDagsData:
			resultDict = {}

			# Just to make sure that two DAGs dont exists with the same name in different tables
			if row[0] in processedDAGs:
				raise HTTPException(
					status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
					detail="Airflow DAG '%s' exists in more than one DAG table"%(row[0]))
			else:
				processedDAGs.append(row[0])

			resultDict['name'] = row[0]
			resultDict['type'] = row[1]
			resultDict['scheduleInterval'] = row[2]
			resultDict['autoRegenerateDag'] = row[3]
#			resultDict['operatorNotes'] = row[4]
#			resultDict['applicationNotes'] = row[5]
			listOfDAGs.append(resultDict)


		jsonResult = json.loads(json.dumps(listOfDAGs))
		session.close()

		return jsonResult


	def getAirflowImportDags(self): 
		""" Returns all Airflow Import DAGs """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None


		airflowImportDags = aliased(configSchema.airflowImportDags)
		listOfDAGs = []

		airflowDagsData = (session.query(
					airflowImportDags.dag_name,
					airflowImportDags.schedule_interval,
					airflowImportDags.auto_regenerate_dag,
					airflowImportDags.filter_hive
				)
				.select_from(airflowImportDags)
				.all()
			)

		for row in airflowDagsData:
			resultDict = {}

			resultDict['name'] = row[0]
			resultDict['scheduleInterval'] = row[1]
			resultDict['autoRegenerateDag'] = row[2]
			resultDict['filterTable'] = row[3]
			listOfDAGs.append(resultDict)


		jsonResult = json.loads(json.dumps(listOfDAGs))
		session.close()

		return jsonResult


	def getAirflowExportDags(self): 
		""" Returns all Airflow Export DAGs """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None


		airflowExportDags = aliased(configSchema.airflowExportDags)
		listOfDAGs = []

		airflowDagsData = (session.query(
					airflowExportDags.dag_name,
					airflowExportDags.schedule_interval,
					airflowExportDags.auto_regenerate_dag,
					airflowExportDags.filter_dbalias,
					airflowExportDags.filter_target_schema,
					airflowExportDags.filter_target_table 
				)
				.select_from(airflowExportDags)
				.all()
			)

		for row in airflowDagsData:
			resultDict = {}

			resultDict['name'] = row[0]
			resultDict['scheduleInterval'] = row[1]
			resultDict['autoRegenerateDag'] = row[2]
			resultDict['filterConnection'] = row[3]
			resultDict['filterTargetSchema'] = row[4]
			resultDict['filterTargetTable'] = row[5]

			listOfDAGs.append(resultDict)


		jsonResult = json.loads(json.dumps(listOfDAGs))
		session.close()

		return jsonResult


	def getAirflowCustomDags(self): 
		""" Returns all Airflow Custom DAGs """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None


		airflowCustomDags = aliased(configSchema.airflowCustomDags)
		listOfDAGs = []

		airflowDagsData = (session.query(
					airflowCustomDags.dag_name,
					airflowCustomDags.schedule_interval,
					airflowCustomDags.auto_regenerate_dag
				)
				.select_from(airflowCustomDags)
				.all()
			)


		for row in airflowDagsData:
			resultDict = {}

			resultDict['name'] = row[0]
			resultDict['scheduleInterval'] = row[1]
			resultDict['autoRegenerateDag'] = row[2]
			listOfDAGs.append(resultDict)


		jsonResult = json.loads(json.dumps(listOfDAGs))
		session.close()

		return jsonResult

	def getAirflowTasks(self, dagname):
		""" Returns a list with all airflow dags associated with a specified dag """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		airflowTasks = aliased(configSchema.airflowTasks)
		listOfTasks = []

		# Return a list of Hive tables with details
		airflowTasksData = (session.query(
					airflowTasks.task_name,
					airflowTasks.task_type,
					airflowTasks.placement,
					airflowTasks.jdbc_dbalias,
					airflowTasks.airflow_pool,
					airflowTasks.airflow_priority,
					airflowTasks.include_in_airflow,
					airflowTasks.task_dependency_downstream,
					airflowTasks.task_dependency_upstream,
					airflowTasks.task_config,
					airflowTasks.sensor_poke_interval,
					airflowTasks.sensor_timeout_minutes,
					airflowTasks.sensor_connection,
					airflowTasks.sensor_soft_fail,
					airflowTasks.sudo_user

				)
				.select_from(airflowTasks)
				.filter(airflowTasks.dag_name == dagname)
				.all()
			)

		for row in airflowTasksData:
			resultDict = {}
			resultDict['name'] = row[0]
			resultDict['type'] = row[1]
			resultDict['placement'] = row[2]
			resultDict['connection'] = row[3]
			resultDict['airflowPool'] = row[4]
			resultDict['airflowPriority'] = row[5]
			resultDict['includeInAirflow'] = row[6]
			resultDict['taskDependencyDownstream'] = row[7]
			resultDict['taskDependencyUpstream'] = row[8]
			resultDict['taskConfig'] = row[9]
			resultDict['sensorPokeInterval'] = row[10]
			resultDict['sensorTimeoutMinutes'] = row[11]
			resultDict['sensorConnection'] = row[12]
			resultDict['sensorSoftFail'] = row[13]
			resultDict['sudoUser'] = row[14]
			listOfTasks.append(resultDict)

		return listOfTasks


	def getAirflowCustomDag(self, dagname): 
		""" Returns an Airflow Custom DAG """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None


		airflowCustomDags = aliased(configSchema.airflowCustomDags)

		row = (session.query(
					airflowCustomDags.dag_name,
					airflowCustomDags.schedule_interval,
					airflowCustomDags.retries,
					airflowCustomDags.operator_notes,
					airflowCustomDags.application_notes,
					airflowCustomDags.airflow_notes,
					airflowCustomDags.auto_regenerate_dag,
					airflowCustomDags.sudo_user,
					airflowCustomDags.timezone,
					airflowCustomDags.email,
					airflowCustomDags.email_on_failure,
					airflowCustomDags.email_on_retries,
					airflowCustomDags.tags,
					airflowCustomDags.sla_warning_time,
					airflowCustomDags.retry_exponential_backoff,
					airflowCustomDags.concurrency
				)
				.select_from(airflowCustomDags)
				.filter(airflowCustomDags.dag_name == dagname)
				.one_or_none()
			)

		if row == None:
			raise HTTPException(
				status_code=status.HTTP_404_NOT_FOUND,
				detail="Airflow DAG does not exist")

		resultDict = {}
		resultDict["name"] = row[0]
		resultDict["scheduleInterval"] = row[1]
		resultDict["retries"] = row[2]
		resultDict["operatorNotes"] = row[3]
		resultDict["applicationNotes"] = row[4]
		resultDict["airflowNotes"] = row[5]
		resultDict["autoRegenerateDag"] = row[6]
		resultDict["sudoUser"] = row[7]
		resultDict["timezone"] = row[8]
		resultDict["email"] = row[9]
		resultDict["emailOnFailure"] = row[10]
		resultDict["emailOnRetries"] = row[11]
		resultDict["tags"] = row[12]
		try:
			resultDict["slaWarningTime"] = row[13].strftime('%H:%M:%S')
		except AttributeError:
			resultDict["slaWarningTime"] = None
		resultDict["retryExponentialBackoff"] = row[14]
		resultDict["concurrency"] = row[15]

		resultDict["tasks"] = self.getAirflowTasks(dagname)
		
		jsonResult = json.loads(json.dumps(resultDict))
		session.close()

		return jsonResult

	def checkDagNameAvailability(self, name, excludeImport = False, excludeExport = False, excludeCustom = False):
		""" Checks if the DAG name is available. Returns True or False """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		airflowCustomDags = aliased(configSchema.airflowCustomDags)
		airflowExportDags = aliased(configSchema.airflowExportDags)
		airflowImportDags = aliased(configSchema.airflowImportDags)
		listOfDAGs = []
		processedDAGs = []
		dagNameIsFree = True

		if excludeCustom == False:
			row = (session.query(airflowCustomDags.dag_name)
					.select_from(airflowCustomDags)
					.filter(airflowCustomDags.dag_name == name)
					.one_or_none()
					)

			if row != None:
				dagNameIsFree = False


		if excludeImport == False:
			row = (session.query(airflowImportDags.dag_name)
					.select_from(airflowImportDags)
					.filter(airflowImportDags.dag_name == name)
					.one_or_none()
					)

			if row != None:
				dagNameIsFree = False


		if excludeExport == False:
			row = (session.query(airflowExportDags.dag_name)
					.select_from(airflowExportDags)
					.filter(airflowExportDags.dag_name == name)
					.one_or_none()
					)

			if row != None:
				dagNameIsFree = False


		return dagNameIsFree
	
	def updateAirflowTasks(self, dagname, task, currentUser):
		""" Create or update Airflow tasks """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		airflowTasks = aliased(configSchema.airflowTasks)

		try:
			query = insert(configSchema.airflowTasks).values(
				dag_name = dagname,
				task_name = getattr(task, "name"),
				task_type = getattr(task, "type"),
				placement = getattr(task, "placement"),
				jdbc_dbalias = getattr(task, "connection"),
				airflow_pool = getattr(task, "airflowPool"),
				airflow_priority = getattr(task, "airflowPriority"),
				include_in_airflow = getattr(task, "includeInAirflow"),
				task_dependency_downstream = getattr(task, "taskDependencyDownstream"),
				task_dependency_upstream = getattr(task, "taskDependencyUpstream"),
				task_config = getattr(task, "taskConfig"),
				sensor_poke_interval = getattr(task, "sensorPokeInterval"),
				sensor_timeout_minutes = getattr(task, "sensorTimeoutMinutes"),
				sensor_connection = getattr(task, "sensorConnection"),
				sensor_soft_fail = getattr(task, "sensorSoftFail"),
				sudo_user = getattr(task, "sudoUser"))

			query = query.on_duplicate_key_update(
				dag_name = dagname,
				task_name = getattr(task, "name"),
				task_type = getattr(task, "type"),
				placement = getattr(task, "placement"),
				jdbc_dbalias = getattr(task, "connection"),
				airflow_pool = getattr(task, "airflowPool"),
				airflow_priority = getattr(task, "airflowPriority"),
				include_in_airflow = getattr(task, "includeInAirflow"),
				task_dependency_downstream = getattr(task, "taskDependencyDownstream"),
				task_dependency_upstream = getattr(task, "taskDependencyUpstream"),
				task_config = getattr(task, "taskConfig"),
				sensor_poke_interval = getattr(task, "sensorPokeInterval"),
				sensor_timeout_minutes = getattr(task, "sensorTimeoutMinutes"),
				sensor_connection = getattr(task, "sensorConnection"),
				sensor_soft_fail = getattr(task, "sensorSoftFail"),
				sudo_user = getattr(task, "sudoUser"))

			session.execute(query)
			session.commit()
		except SQLerror as err:
			log.error(str(err))
			log.error(column)

			session.close()
			return False

		return True



	def getAirflowExportDag(self, dagname): 
		""" Returns an Airflow Export DAG """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None


		airflowExportDags = aliased(configSchema.airflowExportDags)

		row = (session.query(
					airflowExportDags.dag_name,
					airflowExportDags.schedule_interval,
					airflowExportDags.filter_dbalias,
					airflowExportDags.filter_target_schema,
					airflowExportDags.filter_target_table,
					airflowExportDags.retries,
					airflowExportDags.operator_notes,
					airflowExportDags.application_notes,
					airflowExportDags.auto_regenerate_dag,
					airflowExportDags.airflow_notes,
					airflowExportDags.sudo_user,
					airflowExportDags.timezone,
					airflowExportDags.email,
					airflowExportDags.email_on_failure,
					airflowExportDags.email_on_retries,
					airflowExportDags.tags,
					airflowExportDags.sla_warning_time,
					airflowExportDags.retry_exponential_backoff,
					airflowExportDags.concurrency
				)
				.select_from(airflowExportDags)
				.filter(airflowExportDags.dag_name == dagname)
				.one_or_none()
			)

		if row == None:
			raise HTTPException(
				status_code=status.HTTP_404_NOT_FOUND,
				detail="Airflow DAG does not exist")

		resultDict = {}
		resultDict["name"] = row[0]
		resultDict["scheduleInterval"] = row[1]
		resultDict["filterConnection"] = row[2]
		resultDict["filterTargetSchema"] = row[3]
		resultDict["filterTargetTable"] = row[4]
		resultDict["retries"] = row[5]
		resultDict["operatorNotes"] = row[6]
		resultDict["applicationNotes"] = row[7]
		resultDict["autoRegenerateDag"] = row[8]
		resultDict["airflowNotes"] = row[9]
		resultDict["sudoUser"] = row[10]
		resultDict["timezone"] = row[11]
		resultDict["email"] = row[12]
		resultDict["emailOnFailure"] = row[13]
		resultDict["emailOnRetries"] = row[14]
		resultDict["tags"] = row[15]
		try:
			resultDict["slaWarningTime"] = row[16].strftime('%H:%M:%S')
		except AttributeError:
			resultDict["slaWarningTime"] = None
		resultDict["retryExponentialBackoff"] = row[17]
		resultDict["concurrency"] = row[18]

		resultDict["tasks"] = self.getAirflowTasks(dagname)

		jsonResult = json.loads(json.dumps(resultDict))
		session.close()

		return jsonResult


	def getAirflowImportDag(self, dagname): 
		""" Returns an Airflow Import DAG """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None


		airflowImportDags = aliased(configSchema.airflowImportDags)

		row = (session.query(
					airflowImportDags.dag_name,
					airflowImportDags.schedule_interval,
					airflowImportDags.filter_hive,
					airflowImportDags.finish_all_stage1_first,
					airflowImportDags.run_import_and_etl_separate,
					airflowImportDags.retries,
					airflowImportDags.retries_stage1,
					airflowImportDags.retries_stage2,
					airflowImportDags.pool_stage1,
					airflowImportDags.pool_stage2,
					airflowImportDags.operator_notes,
					airflowImportDags.application_notes,
					airflowImportDags.auto_regenerate_dag,
					airflowImportDags.airflow_notes,
					airflowImportDags.sudo_user,
					airflowImportDags.metadata_import,
					airflowImportDags.timezone,
					airflowImportDags.email,
					airflowImportDags.email_on_failure,
					airflowImportDags.email_on_retries,
					airflowImportDags.tags,
					airflowImportDags.sla_warning_time,
					airflowImportDags.retry_exponential_backoff,
					airflowImportDags.concurrency
				)
				.select_from(airflowImportDags)
				.filter(airflowImportDags.dag_name == dagname)
				.one_or_none()
			)

		if row == None:
			raise HTTPException(
				status_code=status.HTTP_404_NOT_FOUND,
				detail="Airflow DAG does not exist")

		resultDict = {}
		resultDict["name"] = row[0]
		resultDict["scheduleInterval"] = row[1]
		resultDict["filterTable"] = row[2]
		resultDict["finishAllStage1First"] = row[3]
		resultDict["runImportAndEtlSeparate"] = row[4]
		resultDict["retries"] = row[5]
		resultDict["retriesStage1"] = row[6]
		resultDict["retriesStage2"] = row[7]
		resultDict["poolStage1"] = row[8]
		resultDict["poolStage2"] = row[9]
		resultDict["operatorNotes"] = row[10]
		resultDict["applicationNotes"] = row[11]
		resultDict["autoRegenerateDag"] = row[12]
		resultDict["airflowNotes"] = row[13]
		resultDict["sudoUser"] = row[14]
		resultDict["metadataImport"] = row[15]
		resultDict["timezone"] = row[16]
		resultDict["email"] = row[17]
		resultDict["emailOnFailure"] = row[18]
		resultDict["emailOnRetries"] = row[19]
		resultDict["tags"] = row[20]
		try:
			resultDict["slaWarningTime"] = row[21].strftime('%H:%M:%S')
		except AttributeError:
			resultDict["slaWarningTime"] = None
		resultDict["retryExponentialBackoff"] = row[22]
		resultDict["concurrency"] = row[23]

		resultDict["tasks"] = self.getAirflowTasks(dagname)

		jsonResult = json.loads(json.dumps(resultDict))
		session.close()

		return jsonResult


	def deleteAirflowTasks(self, dagname):
		""" Delete all Airflow tasks for the specified DAG """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		(session.query(configSchema.airflowTasks)
			.filter(configSchema.airflowTasks.dag_name == dagname)
			.delete())
		session.commit()
		session.close()


	def deleteImportAirflowDag(self, dagname, currentUser):
		""" Delete an Airflow import DAG """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		airflowImportDags = aliased(configSchema.airflowImportDags)
		result = "ok"
		returnCode = 200

		log.info("User '%s' deleted Airflow import DAG '%s'"%(currentUser, dagname))

		# Fetch the tableID as that is required to update/create data in import_columns
		row = (session.query(configSchema.airflowImportDags.dag_name)
				.select_from(configSchema.airflowImportDags)
				.filter(configSchema.airflowImportDags.dag_name == dagname)
				.one_or_none()
				)
		session.commit()

		if row == None:
			result = "Airflow DAG does not exist"
			returnCode = 404
		else:
			(session.query(configSchema.airflowImportDags)
				.filter(configSchema.airflowImportDags.dag_name == dagname)
				.delete())
			session.commit()

			self.deleteAirflowTasks(dagname)

		session.close()
		return (result, returnCode)


	def updateImportAirflowDag(self, airflowDag, currentUser):
		""" Update or create an Airflow import DAG """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		airflowImportDags = aliased(configSchema.airflowImportDags)
		result = "ok"
		returnCode = 200

		if self.checkDagNameAvailability(name = getattr(airflowDag, "name"), excludeImport = True) == False:
			log.error("Airflow Import DAG creation was denied as the name was already existing on custom or export DAG")
			result = "Ariflow DAG with the specified name already exists as Custom or Export"
			returnCode = 400
			return (result, returnCode)

		log.debug(airflowDag)
		log.info("User '%s' updated/created Airflow import DAG '%s'"%(currentUser, getattr(airflowDag, "name")))

		try:
			query = insert(configSchema.airflowImportDags).values(
				dag_name = getattr(airflowDag, "name"),
				schedule_interval = getattr(airflowDag, "scheduleInterval"),
				filter_hive = getattr(airflowDag, "filterTable"),
				finish_all_stage1_first = getattr(airflowDag, "finishAllStage1First"),
				run_import_and_etl_separate = getattr(airflowDag, "runImportAndEtlSeparate"),
				retries = getattr(airflowDag, "retries"),
				retries_stage1 = getattr(airflowDag, "retriesStage1"),
				retries_stage2 = getattr(airflowDag, "retriesStage2"),
				pool_stage1 = getattr(airflowDag, "poolStage1"),
				pool_stage2 = getattr(airflowDag, "poolStage2"),
				operator_notes = getattr(airflowDag, "operatorNotes"),
				application_notes = getattr(airflowDag, "applicationNotes"),
				auto_regenerate_dag = getattr(airflowDag, "autoRegenerateDag"),
				airflow_notes = getattr(airflowDag, "airflowNotes"),
				sudo_user = getattr(airflowDag, "sudoUser"),
				metadata_import = getattr(airflowDag, "metadataImport"),
				timezone = getattr(airflowDag, "timezone"),
				email = getattr(airflowDag, "email"),
				email_on_failure = getattr(airflowDag, "emailOnFailure"),
				email_on_retries = getattr(airflowDag, "emailOnRetries"),
				tags = getattr(airflowDag, "tags"),
				sla_warning_time = getattr(airflowDag, "slaWarningTime"),
				retry_exponential_backoff = getattr(airflowDag, "retryExponentialBackoff"),
				concurrency = getattr(airflowDag, "concurrency"))

			query = query.on_duplicate_key_update(
				dag_name = getattr(airflowDag, "name"),
				schedule_interval = getattr(airflowDag, "scheduleInterval"),
				filter_hive = getattr(airflowDag, "filterTable"),
				finish_all_stage1_first = getattr(airflowDag, "finishAllStage1First"),
				run_import_and_etl_separate = getattr(airflowDag, "runImportAndEtlSeparate"),
				retries = getattr(airflowDag, "retries"),
				retries_stage1 = getattr(airflowDag, "retriesStage1"),
				retries_stage2 = getattr(airflowDag, "retriesStage2"),
				pool_stage1 = getattr(airflowDag, "poolStage1"),
				pool_stage2 = getattr(airflowDag, "poolStage2"),
				operator_notes = getattr(airflowDag, "operatorNotes"),
				application_notes = getattr(airflowDag, "applicationNotes"),
				auto_regenerate_dag = getattr(airflowDag, "autoRegenerateDag"),
				airflow_notes = getattr(airflowDag, "airflowNotes"),
				sudo_user = getattr(airflowDag, "sudoUser"),
				metadata_import = getattr(airflowDag, "metadataImport"),
				timezone = getattr(airflowDag, "timezone"),
				email = getattr(airflowDag, "email"),
				email_on_failure = getattr(airflowDag, "emailOnFailure"),
				email_on_retries = getattr(airflowDag, "emailOnRetries"),
				tags = getattr(airflowDag, "tags"),
				sla_warning_time = getattr(airflowDag, "slaWarningTime"),
				retry_exponential_backoff = getattr(airflowDag, "retryExponentialBackoff"),
				concurrency = getattr(airflowDag, "concurrency"))

			session.execute(query)
			session.commit()
		except SQLerror as err:
			log.error(str(err))
			log.error(column)

			result = str(err)
			returnCode = 500

			session.close()
			return (result, returnCode)

		session.close()

		tasks = getattr(airflowDag, "tasks")
		for task in tasks:
			if self.updateAirflowTasks(dagname = getattr(airflowDag, "name"), task = task, currentUser = currentUser) == False:
				result = "Error while updating/creating Airflow tasks. Please check logs on server"
				returnCode = 400
				return (result, returnCode)
	
		return (result, returnCode)


	def deleteExportAirflowDag(self, dagname, currentUser):
		""" Delete an Airflow export DAG """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		result = "ok"
		returnCode = 200

		log.info("User '%s' deleted Airflow export DAG '%s'"%(currentUser, dagname))

		# Fetch the tableID as that is required to update/create data in import_columns
		row = (session.query(configSchema.airflowExportDags.dag_name)
				.select_from(configSchema.airflowExportDags)
				.filter(configSchema.airflowExportDags.dag_name == dagname)
				.one_or_none()
				)
		session.commit()

		if row == None:
			result = "Airflow DAG does not exist"
			returnCode = 404
		else:
			(session.query(configSchema.airflowExportDags)
				.filter(configSchema.airflowExportDags.dag_name == dagname)
				.delete())
			session.commit()

			self.deleteAirflowTasks(dagname)

		session.close()
		return (result, returnCode)


	def updateExportAirflowDag(self, airflowDag, currentUser):
		""" Update or create an Airflow export DAG """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		airflowExportDags = aliased(configSchema.airflowExportDags)
		result = "ok"
		returnCode = 200

		if self.checkDagNameAvailability(name = getattr(airflowDag, "name"), excludeExport = True) == False:
			log.error("Airflow Import DAG creation was denied as the name was already existing on custom or export DAG")
			result = "Ariflow DAG with the specified name already exists as Custom or Export"
			returnCode = 400
			return (result, returnCode)

		log.debug(airflowDag)
		log.info("User '%s' updated/created Airflow export DAG '%s'"%(currentUser, getattr(airflowDag, "name")))

		try:
			query = insert(configSchema.airflowExportDags).values(
				dag_name = getattr(airflowDag, "name"),
				schedule_interval = getattr(airflowDag, "scheduleInterval"),
				filter_dbalias = getattr(airflowDag, "filterConnection"),
				filter_target_schema = getattr(airflowDag, "filterTargetSchema"),
				filter_target_table = getattr(airflowDag, "filterTargetTable"),
				retries = getattr(airflowDag, "retries"),
				operator_notes = getattr(airflowDag, "operatorNotes"),
				application_notes = getattr(airflowDag, "applicationNotes"),
				auto_regenerate_dag = getattr(airflowDag, "autoRegenerateDag"),
				airflow_notes = getattr(airflowDag, "airflowNotes"),
				sudo_user = getattr(airflowDag, "sudoUser"),
				timezone = getattr(airflowDag, "timezone"),
				email = getattr(airflowDag, "email"),
				email_on_failure = getattr(airflowDag, "emailOnFailure"),
				email_on_retries = getattr(airflowDag, "emailOnRetries"),
				tags = getattr(airflowDag, "tags"),
				sla_warning_time = getattr(airflowDag, "slaWarningTime"),
				retry_exponential_backoff = getattr(airflowDag, "retryExponentialBackoff"),
				concurrency = getattr(airflowDag, "concurrency"))

			query = query.on_duplicate_key_update(
				dag_name = getattr(airflowDag, "name"),
				schedule_interval = getattr(airflowDag, "scheduleInterval"),
				filter_dbalias = getattr(airflowDag, "filterConnection"),
				filter_target_schema = getattr(airflowDag, "filterTargetSchema"),
				filter_target_table = getattr(airflowDag, "filterTargetTable"),
				retries = getattr(airflowDag, "retries"),
				operator_notes = getattr(airflowDag, "operatorNotes"),
				application_notes = getattr(airflowDag, "applicationNotes"),
				auto_regenerate_dag = getattr(airflowDag, "autoRegenerateDag"),
				airflow_notes = getattr(airflowDag, "airflowNotes"),
				sudo_user = getattr(airflowDag, "sudoUser"),
				timezone = getattr(airflowDag, "timezone"),
				email = getattr(airflowDag, "email"),
				email_on_failure = getattr(airflowDag, "emailOnFailure"),
				email_on_retries = getattr(airflowDag, "emailOnRetries"),
				tags = getattr(airflowDag, "tags"),
				sla_warning_time = getattr(airflowDag, "slaWarningTime"),
				retry_exponential_backoff = getattr(airflowDag, "retryExponentialBackoff"),
				concurrency = getattr(airflowDag, "concurrency"))

			session.execute(query)
			session.commit()
		except SQLerror as err:
			log.error(str(err))
			log.error(column)

			result = str(err)
			returnCode = 500

			session.close()
			return (result, returnCode)

		session.close()

		tasks = getattr(airflowDag, "tasks")
		for task in tasks:
			if self.updateAirflowTasks(dagname = getattr(airflowDag, "name"), task = task, currentUser = currentUser) == False:
				result = "Error while updating/creating Airflow tasks. Please check logs on server"
				returnCode = 400
				return (result, returnCode)
	
		return (result, returnCode)


	def deleteCustomAirflowDag(self, dagname, currentUser):
		""" Delete an Airflow import DAG """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		result = "ok"
		returnCode = 200

		log.info("User '%s' deleted Airflow import DAG '%s'"%(currentUser, dagname))

		# Fetch the tableID as that is required to update/create data in import_columns
		row = (session.query(configSchema.airflowCustomDags.dag_name)
				.select_from(configSchema.airflowCustomDags)
				.filter(configSchema.airflowCustomDags.dag_name == dagname)
				.one_or_none()
				)
		session.commit()

		if row == None:
			result = "Airflow DAG does not exist"
			returnCode = 404
		else:
			(session.query(configSchema.airflowCustomDags)
				.filter(configSchema.airflowCustomDags.dag_name == dagname)
				.delete())
			session.commit()

			self.deleteAirflowTasks(dagname)

		session.close()
		return (result, returnCode)


	def updateCustomAirflowDag(self, airflowDag, currentUser):
		""" Update or create an Airflow custom DAG """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		airflowCustomDags = aliased(configSchema.airflowCustomDags)
		result = "ok"
		returnCode = 200

		if self.checkDagNameAvailability(name = getattr(airflowDag, "name"), excludeCustom = True) == False:
			log.error("Airflow Import DAG creation was denied as the name was already existing on custom or export DAG")
			result = "Ariflow DAG with the specified name already exists as Custom or Export"
			returnCode = 400
			return (result, returnCode)

		log.debug(airflowDag)
		log.info("User '%s' updated/created Airflow custom DAG '%s'"%(currentUser, getattr(airflowDag, "name")))

		try:
			query = insert(configSchema.airflowCustomDags).values(
				dag_name = getattr(airflowDag, "name"),
				schedule_interval = getattr(airflowDag, "scheduleInterval"),
				retries = getattr(airflowDag, "retries"),
				operator_notes = getattr(airflowDag, "operatorNotes"),
				application_notes = getattr(airflowDag, "applicationNotes"),
				auto_regenerate_dag = getattr(airflowDag, "autoRegenerateDag"),
				airflow_notes = getattr(airflowDag, "airflowNotes"),
				sudo_user = getattr(airflowDag, "sudoUser"),
				timezone = getattr(airflowDag, "timezone"),
				email = getattr(airflowDag, "email"),
				email_on_failure = getattr(airflowDag, "emailOnFailure"),
				email_on_retries = getattr(airflowDag, "emailOnRetries"),
				tags = getattr(airflowDag, "tags"),
				sla_warning_time = getattr(airflowDag, "slaWarningTime"),
				retry_exponential_backoff = getattr(airflowDag, "retryExponentialBackoff"),
				concurrency = getattr(airflowDag, "concurrency"))

			query = query.on_duplicate_key_update(
				dag_name = getattr(airflowDag, "name"),
				schedule_interval = getattr(airflowDag, "scheduleInterval"),
				retries = getattr(airflowDag, "retries"),
				operator_notes = getattr(airflowDag, "operatorNotes"),
				application_notes = getattr(airflowDag, "applicationNotes"),
				auto_regenerate_dag = getattr(airflowDag, "autoRegenerateDag"),
				airflow_notes = getattr(airflowDag, "airflowNotes"),
				sudo_user = getattr(airflowDag, "sudoUser"),
				timezone = getattr(airflowDag, "timezone"),
				email = getattr(airflowDag, "email"),
				email_on_failure = getattr(airflowDag, "emailOnFailure"),
				email_on_retries = getattr(airflowDag, "emailOnRetries"),
				tags = getattr(airflowDag, "tags"),
				sla_warning_time = getattr(airflowDag, "slaWarningTime"),
				retry_exponential_backoff = getattr(airflowDag, "retryExponentialBackoff"),
				concurrency = getattr(airflowDag, "concurrency"))

			session.execute(query)
			session.commit()
		except SQLerror as err:
			log.error(str(err))
			log.error(column)

			result = str(err)
			returnCode = 500

			session.close()
			return (result, returnCode)

		session.close()

		tasks = getattr(airflowDag, "tasks")
		for task in tasks:
			if self.updateAirflowTasks(dagname = getattr(airflowDag, "name"), task = task, currentUser = currentUser) == False:
				result = "Error while updating/creating Airflow tasks. Please check logs on server"
				returnCode = 400
				return (result, returnCode)
	
		return (result, returnCode)

