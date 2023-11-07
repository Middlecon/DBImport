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
from fastapi.encoders import jsonable_encoder

from ConfigReader import configuration
from datetime import date, datetime, timedelta
from common import constants as constant
from common.Exceptions import *
from DBImportConfig import configSchema
# from DBImportConfig import common_config
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select, func
from sqlalchemy.orm import aliased, sessionmaker, Query

class dbCalls:
	def __init__(self):

		logFormat = '%(asctime)s %(levelname)s - %(message)s'
		logging.basicConfig(format=logFormat, level=logging.INFO)
		logPropagate = True

		log = logging.getLogger()
		log.debug("Executing Server.restServerCalls.__init__()")

		self.mysql_conn = None
		self.mysql_cursor = None
		self.debugLogLevel = False

		if logging.root.level == 10:        # DEBUG
			self.debugLogLevel = True

#		self.common_config = common_config.config()

		self.logdir = configuration.get("Server", "logdir")

#		self.crypto = self.common_config.crypto
#		self.crypto.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
#		self.crypto.setPublicKeyFile(configuration.get("Credentials", "public_key"))

#		self.remoteDBImportEngines = {}
#		self.remoteDBImportSessions = {}
#		self.remoteInstanceConfigDB = None

		self.configDBSession = None
		self.configDBEngine = None

#		self.distCPreqQueue = Queue()
#		self.distCPresQueue = Queue()
#		self.threadStopEvent = threading.Event()

		# Start the Atlas Discovery Thread
#		self.atlasDiscoveryThread = atlasDiscovery.atlasDiscovery(self.threadStopEvent)
#		self.atlasDiscoveryThread.daemon = True
#		self.atlasDiscoveryThread.start()

		# Fetch configuration about MySQL database and how to connect to it
		self.configHostname = configuration.get("Database", "mysql_hostname")
		self.configPort =     configuration.get("Database", "mysql_port")
		self.configDatabase = configuration.get("Database", "mysql_database")
		self.configUsername = configuration.get("Database", "mysql_username")
		self.configPassword = configuration.get("Database", "mysql_password")

		session = self.getDBImportSession()
		return


	def disconnectDBImportDB(self):
		""" Disconnects from the database and removes all sessions and engine """
		log = logging.getLogger("server")

		if self.configDBEngine != None:
			log.info("Disconnecting from DBImport database")
			self.configDBEngine.dispose()
			self.configDBEngine = None

		self.configDBSession = None

	def getDBImportSession(self):
		log = logging.getLogger("server")
		if self.configDBSession == None:
			if self.connectDBImportDB() == False:
				raise SQLerror("Can't connect to DBImport database")

		return self.configDBSession()	


	def connectDBImportDB(self):
		# Esablish a SQLAlchemy connection to the DBImport database
		log = logging.getLogger("server")
		self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s"%(
			self.configUsername,
			self.configPassword,
			self.configHostname,
			self.configPort,
			self.configDatabase)

		try:
			self.configDBEngine = sa.create_engine(self.connectStr, echo = self.debugLogLevel, pool_pre_ping=True, pool_recycle=3600)
			self.configDBEngine.connect()
			self.configDBSession = sessionmaker(bind=self.configDBEngine)

		except sa.exc.OperationalError as err:
			log.error("%s"%err)
			self.configDBSession = None
			self.configDBEngine = None
			return False
#			self.common_config.remove_temporary_files()
#			sys.exit(1)

		except:
			print("Unexpected error: ")
			print(sys.exc_info())
			self.configDBSession = None
			self.configDBEngine = None
			return False
#			self.common_config.remove_temporary_files()
#			sys.exit(1)

		log.info("Connected successful against DBImport database")
		return True


	def disconnectRemoteSession(self, instance):
		""" Disconnects from the remote database and removes all sessions and engine """
		log = logging.getLogger("server")

		try:
			engine = self.remoteDBImportEngines.get(instance)
			if engine != None:
				log.info("Disconnecting from remote DBImport database for '%s'"%(instance))
				engine.dispose()
			self.remoteDBImportEngines.pop(instance)
			self.remoteDBImportSessions.pop(instance)
		except KeyError:
			log.debug("Cant remove DBImport session or engine. Key does not exist")


	def getDBImportImportTableDBs(self):
		""" Returns all databases that have imports configured in them """
		log = logging.getLogger()

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importTables = aliased(configSchema.importTables)

		importTableDBs = (session.query(
					importTables.hive_db
				)
				.select_from(importTables)
				.group_by(importTables.hive_db)
				.all()
			)

		listOfDBs = []
		for u in importTableDBs:
			listOfDBs.append(u[0])

		jsonResult = json.dumps(listOfDBs)
		session.close()

		return jsonResult
	

	def getDBImportImportTables(self, db, details):
		""" Returns all import tables in a specific database """
		log = logging.getLogger()

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importTables = aliased(configSchema.importTables)
		listOfTables = []

		if details == False:
			# Return a list of hive tables without the details
			importTablesData = (session.query(
						importTables.hive_table
					)
					.select_from(importTables)
					.filter(importTables.hive_db == db)
					.all()
				)

			for row in importTablesData:
				listOfTables.append(row[0])

		else:
			# Return a list of Hive tables with details
			importTablesData = (session.query(
						importTables.hive_table,
						importTables.dbalias,
						importTables.source_schema,
						importTables.source_table,
						importTables.import_phase_type,
						importTables.etl_phase_type,
						importTables.import_tool
					)
					.select_from(importTables)
					.filter(importTables.hive_db == db)
					.all()
				)

			for row in importTablesData:
				tempDict = {}
				tempDict['hiveTable'] = row[0]
				tempDict['dbAlias'] = row[1]
				tempDict['sourceSchema'] = row[2]
				tempDict['sourceTable'] = row[3]
				tempDict['importPhaseType'] = row[4]
				tempDict['etlPhaseType'] = row[5]
				tempDict['importTool'] = row[6]

				listOfTables.append(tempDict)


		jsonResult = json.dumps(listOfTables)
		session.close()

		return jsonResult
	

		
	def getDBImportImportTableDetails(self, db, table):
		""" Returns all import table details """
		log = logging.getLogger()

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importTables = aliased(configSchema.importTables)
		listOfTables = []

		# Return a list of hive tables without the details
		importTablesData = (session.query(*)
				.select_from(importTables)
				.filter((importTables.hive_db == db) & (importTables.hive_table == table))
				.all()).fillna('')
			)

			for row in importTablesData:
				listOfTables.append(row[0])

#		else:
#			# Return a list of Hive tables with details
#			importTablesData = (session.query(
#						importTables.hive_table,
#						importTables.dbalias,
#						importTables.source_schema,
#						importTables.source_table,
#						importTables.import_phase_type,
#						importTables.etl_phase_type,
#						importTables.import_tool
#					)
#					.select_from(importTables)
#					.filter(importTables.hive_db == db)
#					.all()
#				)
#
#			for row in importTablesData:
#				tempDict = {}
#				tempDict['hiveTable'] = row[0]
#				tempDict['dbAlias'] = row[1]
#				tempDict['sourceSchema'] = row[2]
#				tempDict['sourceTable'] = row[3]
#				tempDict['importPhaseType'] = row[4]
#				tempDict['etlPhaseType'] = row[5]
#				tempDict['importTool'] = row[6]
#
#				listOfTables.append(tempDict)


		jsonResult = json.dumps(listOfTables)
		session.close()

		return jsonResult
	

		
