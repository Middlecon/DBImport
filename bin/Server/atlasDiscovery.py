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
from queue import Queue
from queue import Empty
#import Queue
import threading
from daemons.prefab import run
from ConfigReader import configuration
from datetime import date, datetime, timedelta
from common import constants as constant
from common.Exceptions import *
from DBImportConfig import configSchema
from DBImportConfig import common_config
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select, func
from sqlalchemy.orm import aliased, sessionmaker, Query

class atlasDiscovery(threading.Thread):
	def __init__(self, threadStopEvent):
		threading.Thread.__init__(self)
		self.threadStopEvent = threadStopEvent

	def run(self):
		logger = "atlasDiscovery"
		log = logging.getLogger(logger)
#		log.info("atlasDiscovery started")
		self.mysql_conn = None
		self.mysql_cursor = None
		self.configDBSession = None
		self.configDBEngine = None
		self.debugLogLevel = False

		if logging.root.level == 10:        # DEBUG
			self.debugLogLevel = True

		# Fetch configuration about MySQL database and how to connect to it
		self.configHostname = configuration.get("Database", "mysql_hostname")
		self.configPort =     configuration.get("Database", "mysql_port")
		self.configDatabase = configuration.get("Database", "mysql_database")
		self.configUsername = configuration.get("Database", "mysql_username")
		self.configPassword = configuration.get("Database", "mysql_password")

		self.common_config = common_config.config()

		jdbcConnections = aliased(configSchema.jdbcConnections)

		failureLog = {}

		# The interval between the scans. This is in hours
		atlasDiscoveryInterval = self.common_config.getConfigValue(key = "atlas_discovery_interval")

		atlasEnabled = self.common_config.checkAtlasSchema(logger=logger)
		if atlasEnabled == True:
			log.info("atlasDiscovery started")
			log.info("Atlas discovery interval is set to %s hours"%(atlasDiscoveryInterval))

		while not self.threadStopEvent.isSet() and atlasEnabled == True:

			try:
				session = self.getDBImportSession()
				atlasDiscoveryCheckTime = datetime.utcnow() - timedelta(hours=atlasDiscoveryInterval)

				jdbcConnectionsDf = pd.DataFrame(session.query(
					jdbcConnections.dbalias,
					jdbcConnections.timewindow_start,
					jdbcConnections.timewindow_stop,
					jdbcConnections.atlas_last_discovery,
					jdbcConnections.atlas_discovery
					)
					.select_from(jdbcConnections)
					.filter(jdbcConnections.atlas_discovery == 1)
					.filter((jdbcConnections.atlas_last_discovery < atlasDiscoveryCheckTime) | (jdbcConnections.atlas_last_discovery == None))
					.order_by(jdbcConnections.atlas_last_discovery)
					.all())
				session.close()

			except SQLAlchemyError as e:
				log.error(str(e.__dict__['orig']))
				session.rollback()
				self.disconnectDBImportDB()

			else:

				for index, row in jdbcConnectionsDf.iterrows():
					dbAlias = row['dbalias']

					currentTime = str(datetime.now().strftime('%H:%M:%S'))
					timeWindowStart = None
					timeWindowStop = None
					dbAliasAllowedAtThisTime = False

					if row['timewindow_start'] != None: timeWindowStart = str(row['timewindow_start'])
					if row['timewindow_stop'] != None: timeWindowStop = str(row['timewindow_stop'])

					if timeWindowStart != None and re.search('^[0-9]:', timeWindowStart): timeWindowStart = "0" + timeWindowStart
					if timeWindowStop  != None and re.search('^[0-9]:', timeWindowStop):  timeWindowStop  = "0" + timeWindowStop

					if timeWindowStart == None and timeWindowStop == None:
						dbAliasAllowedAtThisTime = True

					elif currentTime > timeWindowStart and currentTime < timeWindowStop:
						dbAliasAllowedAtThisTime = True

					# Find out if the dbAlias is blacklisted
					if failureLog.get(dbAlias, None) != None:
						blackListEnableTime = failureLog[dbAlias]['blackListStart'] + timedelta(hours=failureLog[dbAlias]['blackListTime'])

						if datetime.now() < blackListEnableTime:
							# This dbAlias is still blacklisted
							continue

					if dbAliasAllowedAtThisTime == False:
						# Not allowed to access this connection at this time
						continue

					self.common_config.mysql_conn.commit()
					self.common_config.lookupConnectionAlias(dbAlias)

					if self.common_config.atlasJdbcSourceSupport == False:
						# This source type does not support Atlas discovery
						continue

					# We now have a valid connection in dbAlias that we can do a discovery on
					log.info("Starting a Atlas discovery on connection '%s'"%(dbAlias))

					altasOperationFailed = False
					if self.common_config.connectToJDBC(allJarFiles=True, exitIfFailure=False, logger=logger) == True:
						self.common_config.atlasEnabled = True
						response = self.common_config.discoverAtlasRdbms(dbAlias = dbAlias, logger=logger)
						if response == False:
							# Something went wrong when getting source system schema
							altasOperationFailed = True
							log.warning("There was an error/warning when discovering source schema")
						else:
							log.info("Finished Atlas discovery on connection '%s'"%(dbAlias))

						self.common_config.disconnectFromJDBC()

						if altasOperationFailed == False:
							updateDict = {}
							updateDict["atlas_last_discovery"] = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))

							try:
								session = self.getDBImportSession()
								(session.query(configSchema.jdbcConnections)
									.filter(configSchema.jdbcConnections.dbalias == dbAlias)
									.update(updateDict))
								session.commit()
								session.close()
	
							except SQLAlchemyError as e:
								log.error(str(e.__dict__['orig']))
								session.rollback()
								self.disconnectDBImportDB()

							else:
								failureLog.pop(dbAlias, None)
								break	
					else:
						altasOperationFailed = True

					if altasOperationFailed == True:

						# Connection failed. We need to blacklist this connection for some time
						blackListData = failureLog.get(dbAlias, None)
						if blackListData == None:
							blackListTime = 1
						else:
							blackListTime = failureLog[dbAlias]['blackListTime'] * 2

							# Max blacklist time is 24 hours
							if blackListTime > 24: blackListTime = 24

						failureLog[dbAlias] = { 'blackListTime': blackListTime, 'blackListStart': datetime.now() }

						log.warning("Atlas Discovery failed on connection '%s'"%(dbAlias))
						log.warning("This connection is now blacklisted for %s hours"%(failureLog[dbAlias]['blackListTime']))

			time.sleep(1)

		self.disconnectDBImportDB()
		if atlasEnabled == True:
			log.info("atlasDiscovery stopped")

	def getDBImportSession(self):
		log = logging.getLogger("atlasDiscovery")
		if self.configDBSession == None:
			self.connectDBImportDB()

		return self.configDBSession()


	def connectDBImportDB(self):
		# Esablish a SQLAlchemy connection to the DBImport database
		log = logging.getLogger("atlasDiscovery")
		self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s"%(
			self.configUsername,
			self.configPassword,
			self.configHostname,
			self.configPort,
			self.configDatabase)

		try:
			self.configDBEngine = sa.create_engine(self.connectStr, echo = self.debugLogLevel)
			self.configDBEngine.connect()
			self.configDBSession = sessionmaker(bind=self.configDBEngine)

		except sa.exc.OperationalError as err:
			log.error("%s"%err)
			self.common_config.remove_temporary_files()
			sys.exit(1)
		except:
			print("Unexpected error: ")
			print(sys.exc_info())
			self.common_config.remove_temporary_files()
			sys.exit(1)

		log.info("Connected successful against DBImport database")

	def disconnectDBImportDB(self):
		""" Disconnects from the database and removes all sessions and engine """
		log = logging.getLogger("atlasDiscovery")

		if self.configDBEngine != None:
			log.info("Disconnecting from DBImport database")
			self.configDBEngine.dispose()
			self.configDBEngine = None

		self.configDBSession = None


