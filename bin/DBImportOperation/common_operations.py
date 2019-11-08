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
import time 
import subprocess 
import re
from reprint import output
import requests
import puretransport
from pyhive import hive
from pyhive import exc
from common.Singleton import Singleton
from common.Exceptions import *
import common.Exceptions
from TCLIService.ttypes import TOperationState
from requests_kerberos import HTTPKerberosAuth, REQUIRED
import kerberos
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, timedelta
import pandas as pd
from DBImportConfig import common_config

import sqlalchemy as sa
from DBImportOperation import hiveSchema
#from sqlalchemy.orm import Session, sessionmaker
#from sqlalchemy.ext.automap import automap_base
# from setupOperation import schema
from sqlalchemy.sql import alias, select
from sqlalchemy.orm import aliased, sessionmaker 
from sqlalchemy.pool import QueuePool


class operation(object, metaclass=Singleton):
	def __init__(self, Hive_DB=None, Hive_Table=None):
		logging.debug("Executing common_operation.__init__()")

		self.Hive_DB = Hive_DB	 
		self.Hive_Table = Hive_Table	 
#		self.mysql_conn = None
#		self.mysql_cursor = None
		self.hive_conn = None
		self.hive_cursor = None
		self.debugLogLevel = False

		if logging.root.level == 10:		# DEBUG
			self.debugLogLevel = True

		# Fetch and initialize the Kerberos configuration
		self.kerberosPrincipal = configuration.get("Kerberos", "principal")
		self.webHCatAuth = HTTPKerberosAuth(force_preemptive=True, principal=self.kerberosPrincipal)

		self.common_config = common_config.config()

		# Fetch configuration details about Hive LLAP
		self.hive_hostname = configuration.get("Hive", "hostname")
		self.hive_port = configuration.get("Hive", "port")
		self.hive_kerberos_service_name = configuration.get("Hive", "kerberos_service_name")
		self.hive_kerberos_realm = configuration.get("Hive", "kerberos_realm")
		self.hive_print_messages = self.common_config.getConfigValue(key = "hive_print_messages")
		if configuration.get("Hive", "use_ssl").lower() == "true":
			self.hive_use_ssl = True
		else:
			self.hive_use_ssl = False

		self.hive_min_buckets = int(configuration.get("Hive", "min_buckets"))
		self.hive_max_buckets = int(configuration.get("Hive", "max_buckets"))

		# HDFS Settings
		self.hdfs_address = self.common_config.getConfigValue(key = "hdfs_address")
		self.hdfs_basedir = self.common_config.getConfigValue(key = "hdfs_basedir")
		self.hdfs_blocksize = self.common_config.getConfigValue(key = "hdfs_blocksize")

		self.hiveConnectStr = configuration.get("Hive", "hive_metastore_alchemy_conn")

		try:
			self.hiveMetaDB = sa.create_engine(self.hiveConnectStr, echo = self.debugLogLevel)
			self.hiveMetaDB.connect()
			self.hiveMetaSession = sessionmaker(bind=self.hiveMetaDB)
		except sa.exc.OperationalError as err:
			logging.error("%s"%err)
			self.common_config.remove_temporary_files()
			sys.exit(1)
		except:
			print("Unexpected error: ")
			print(sys.exc_info())
			self.common_config.remove_temporary_files()
			sys.exit(1)

		logging.debug("Executing common_operations.__init__() - Finished")

	def __del__(self):
		""" Make sure that the Hive connector is closed correctly """

		try:
			if self.hive_cursor != None:
				self.hive_cursor.close()
				self.hive_conn.close()
		except:
			# We dont really care. Close if you can. If you cant, well, we still are closing the application :)
			pass

	def reconnectHiveMetaStore(self):
		logging.debug("Reconnecting to Hive Metastore should not be needed anymore. Code is not really doing anything")

	def setHiveTable(self, Hive_DB, Hive_Table):
		""" Sets the parameters to work against a new Hive database and table """
		self.Hive_DB = Hive_DB.lower()	 
		self.Hive_Table = Hive_Table.lower()	 

	def checkHiveMetaStore(self):
		logging.debug("Executing common_operations.checkHiveMetaStore()")

		# Loop through all Hive webHCat servers from the configuration and checks if one of them works
		self.hiveMetaStore = None
		webHCatResponse = None
		for HiveURL in self.hiveMetaStoreList:
			try:
				URL = HiveURL.strip() + "/templeton/v1/status"
				logging.debug("Testing status for webHCat on URL: %s"%(URL))
				webHCatResponse = requests.get(URL, auth=self.webHCatAuth)
			except:
				continue
			if webHCatResponse.status_code == 200:
				self.hiveMetaStore = HiveURL.strip()
				break

		if self.hiveMetaStore == None:
			logging.error("Cant find a working Hive webHCat server. Please check configuration and server status")
			# TODO: Handle the exit here

		logging.debug("Executing common_operations.checkHiveMetaStore() - Finished")

#	def checkTimeWindow(self, connection_alias):
#		logging.debug("Executing common_operations.checkTimeWindow()")
#		logging.info("Checking if we are allowed to use this jdbc connection at this time")
#
#		query = "select timewindow_start, timewindow_stop from jdbc_connections where dbalias = %s"
#		self.mysql_cursor.execute(query, (connection_alias, ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
#
#		row = self.mysql_cursor.fetchone()
#		currentTime = str(datetime.now().strftime('%H:%M:%S'))
#		timeWindowStart = None
#		timeWindowStop = None
#
#		if row[0] != None: timeWindowStart = str(row[0])
#		if row[1] != None: timeWindowStop = str(row[1])
#
#		if timeWindowStart == None and timeWindowStop == None:
#			logging.info("SUCCESSFUL: This JDBC connection is allowed to be accessed at any time during the day.")
#			return
#		elif timeWindowStart == None or timeWindowStop == None: 
#			logging.error("Atleast one of the TimeWindow settings are NULL in the database. Only way to disable the Time Window")
#			logging.error("function is to put NULL into both columns. Otherwise the configuration is marked as invalid and will exit")
#			logging.error("as it's not running inside a correct Time Window.")
#			logging.error("Invalid TimeWindow configuration")
#			self.remove_temporary_files()
#			sys.exit(1)
#		elif timeWindowStart > timeWindowStop:
#			logging.error("The value in timewindow_start column is larger than the value in timewindow_stop.")
#			logging.error("Invalid TimeWindow configuration")
#			self.remove_temporary_files()
#			sys.exit(1)
#		elif timeWindowStart == timeWindowStop:
#			logging.error("The value in timewindow_start column is the same as the value in timewindow_stop.")
#			logging.error("Invalid TimeWindow configuration")
#			self.remove_temporary_files()
#			sys.exit(1)
#		elif currentTime < timeWindowStart or currentTime > timeWindowStop:
#			logging.error("We are not allowed to access this JDBC Connection outside the configured Time Window")
#			logging.info("    Current time:     %s"%(currentTime))
#			logging.info("    TimeWindow Start: %s"%(timeWindowStart))
#			logging.info("    TimeWindow Stop:  %s"%(timeWindowStop))
#			self.remove_temporary_files()
#			sys.exit(1)
#		else:		
#			logging.info("SUCCESSFUL: There is a configured Time Window for this operation, and we are running inside that window.")
# 
#		logging.debug("    currentTime = %s"%(currentTime))
#		logging.debug("    timeWindowStart = %s"%(timeWindowStart))
#		logging.debug("    timeWindowStop = %s"%(timeWindowStop))
#		logging.debug("Executing common_operations.checkTimeWindow() - Finished")

	def checkHiveDB(self, hiveDB):
		logging.debug("Executing common_operations.checkHiveDB()")

		session = self.hiveMetaSession()
		DBS = hiveSchema.DBS

		try:
			row = session.query(DBS.NAME).filter(DBS.NAME == hiveDB).one()
		except sa.orm.exc.NoResultFound:
			raise databaseNotFound("Can't find database '%s' in Hive"%(hiveDB))

		logging.debug("Executing common_operations.checkHiveDB() - Finished")

	def checkHiveTable(self, hiveDB, hiveTable):
		logging.debug("Executing common_operations.checkHiveTable()")

		session = self.hiveMetaSession()
		TBLS = aliased(hiveSchema.TBLS, name="T")
		DBS = aliased(hiveSchema.DBS, name="D")

		result = True
		try:
			row = session.query(TBLS.TBL_NAME, TBLS.TBL_TYPE, DBS.NAME).select_from(TBLS).join(DBS, TBLS.DBS).filter(TBLS.TBL_NAME == hiveTable.lower()).filter(DBS.NAME == hiveDB.lower()).one()
		except sa.orm.exc.NoResultFound:
			result = False

		return result

#		result_df = pd.DataFrame(self.hiveMetaSession.query(TBLS.TBL_NAME, TBLS.TBL_TYPE, DBS.NAME).select_from(TBLS).join(DBS, TBLS.DBS).filter(TBLS.TBL_NAME == hiveTable.lower()).all())
#		print(result_df)

#	def isHiveColumnQuotationNeeded(self, hiveDB, hiveTable, column):
#		""" Returns True or False if column quotation is needed for this column """
#		logging.debug("Executing common_operations.isHiveColumnQuotationNeeded()")
#		quotationNeeded = True
#
#		query  = "select c.TYPE_NAME "
#		query += "from COLUMNS_V2 c "
#		query += "   left join SDS s on c.CD_ID = s.CD_ID "
#		query += "   left join TBLS t on s.SD_ID = t.SD_ID "
#		query += "   left join DBS d on t.DB_ID = d.DB_ID "
#		query += "where "
#		query += "   c.COLUMN_NAME = %s "
#		query += "   and d.NAME = %s "
#		query += "   and t.TBL_NAME = %s"
#		self.mysql_cursor.execute(query, (column.lower(), hiveDB.lower(), hiveTable.lower() ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
#
#		row = self.mysql_cursor.fetchone()
#		columnType = row[0]
#
#		if columnType in ("int", "integer", "bigint", "tinyint", "smallint", "decimal", "double", "float", "boolean"):
#			quotationNeeded = False
#
#		logging.debug("Quotation needed: %s"%(quotationNeeded))
#		logging.debug("Executing common_operations.isHiveColumnQuotationNeeded() - Finished")
#		return quotationNeeded

	def getTableLocation(self, hiveDB, hiveTable):
		logging.debug("Executing common_operations.getExternalTableLocation()")

		session = self.hiveMetaSession()
		TBLS = aliased(hiveSchema.TBLS, name="T")
		DBS = aliased(hiveSchema.DBS, name="D")
		SDS = aliased(hiveSchema.SDS, name="S")

		try:
			row = session.query(SDS.LOCATION).select_from(TBLS).join(SDS).join(DBS).filter(TBLS.TBL_NAME == hiveTable.lower()).filter(DBS.NAME == hiveDB.lower()).one()
		except sa.orm.exc.NoResultFound:
			raise rowNotFound("Cant get SDS.LOCATION from Hive Meta Database"%(hiveDB))

		return row[0]

		logging.debug("Executing common_operations.getExternalTableLocation() - Finished")

	def isHiveTableExternalOrcFormat(self, hiveDB, hiveTable):
		logging.debug("Executing common_operations.isHiveTableExternalOrcFormat()")

		if self.isHiveTableExternal(hiveDB, hiveTable) == False: return False

		session = self.hiveMetaSession()
		TBLS = aliased(hiveSchema.TBLS, name="T")
		DBS = aliased(hiveSchema.DBS, name="D")
		SDS = aliased(hiveSchema.SDS, name="S")

		try:
			row = session.query(SDS.INPUT_FORMAT).select_from(TBLS).join(SDS).join(DBS).filter(TBLS.TBL_NAME == hiveTable.lower()).filter(DBS.NAME == hiveDB.lower()).one()
		except sa.orm.exc.NoResultFound:
			raise rowNotFound("Cant get SDS.INPUT_FORMAT from Hive Meta Database"%(hiveDB))

		if row[0] == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat":
			return True
		else:
			return False

	def isHiveTableExternalParquetFormat(self, hiveDB, hiveTable):
		logging.debug("Executing common_operations.isExternalHiveTableParquetFormat()")

		if self.isHiveTableExternal(hiveDB, hiveTable) == False: return False

		session = self.hiveMetaSession()
		TBLS = aliased(hiveSchema.TBLS, name="T")
		DBS = aliased(hiveSchema.DBS, name="D")
		SDS = aliased(hiveSchema.SDS, name="S")

		try:
			row = session.query(SDS.INPUT_FORMAT).select_from(TBLS).join(SDS).join(DBS).filter(TBLS.TBL_NAME == hiveTable.lower()).filter(DBS.NAME == hiveDB.lower()).one()
		except sa.orm.exc.NoResultFound:
			raise rowNotFound("Cant get SDS.INPUT_FORMAT from Hive Meta Database"%(hiveDB))

		if row[0] == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat":
			return True
		else:
			return False

	def isHiveTableExternal(self, hiveDB, hiveTable):
		logging.debug("Executing common_operations.isHiveTableExternal()")

		session = self.hiveMetaSession()
		TBLS = aliased(hiveSchema.TBLS, name="T")
		DBS = aliased(hiveSchema.DBS, name="D")

		try:
			row = session.query(TBLS.TBL_TYPE).select_from(TBLS).join(DBS).filter(TBLS.TBL_NAME == hiveTable.lower()).filter(DBS.NAME == hiveDB.lower()).one()
		except sa.orm.exc.NoResultFound:
			raise rowNotFound("Cant get TBLS.TBL_TYPE from Hive Meta Database"%(hiveDB))

#		query = "select t.TBL_TYPE from TBLS t left join DBS d on t.DB_ID = d.DB_ID where d.NAME = %s and t.TBL_NAME = %s"
#		self.mysql_cursor.execute(query, (hiveDB.lower(), hiveTable.lower() ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
#
#		row = self.mysql_cursor.fetchone()
#		print(row)

#		self.common_config.remove_temporary_files()
#		sys.exit(1)

		if row[0] == "EXTERNAL_TABLE":
			return True
		else:
			return False

#	def checkFK(self, FKname):
#		""" Checks if the ForeignKey with the specified name exists in Hive """
#		logging.debug("Executing common_operations.checkFK()")
#
#		query = "select count(CONSTRAINT_NAME) from KEY_CONSTRAINTS where CONSTRAINT_TYPE = 1 and CONSTRAINT_NAME = %s"
#		self.mysql_cursor.execute(query, (FKname.lower(), ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
#
#		row = self.mysql_cursor.fetchone()
#		if row[0] == 1:
#			return True
#		else:
#			return False
	
	def connectToHive(self, forceSkipTest=False, quiet=False):
		logging.debug("Executing common_operations.connectToHive()")

		# Make sure we only connect if we havent done so before. Reason is that this function can be called from many different places
		# due to the retry functionallity
		if self.hive_cursor == None:
			if quiet == False: logging.info("Connecting to Hive")
			try:
				# TODO: Remove error messages output from hive.connect. Check this by entering a wrong hostname or port
				if self.hive_use_ssl == True:
					transport = puretransport.transport_factory(host=self.hive_hostname, port = self.hive_port, kerberos_service_name = self.hive_kerberos_service_name, use_sasl=True, sasl_auth='GSSAPI', use_ssl=True, username='dummy', password='dummy')
					self.hive_conn = hive.connect(thrift_transport=transport, configuration = {'hive.llap.execution.mode': 'none'})
				else:
					self.hive_conn = hive.connect(host = self.hive_hostname, port = self.hive_port, database = "default", auth = "KERBEROS", kerberos_service_name = self.hive_kerberos_service_name, configuration = {'hive.llap.execution.mode': 'none'} )
			except Exception as ex:
				raise ValueError("Could not connect to Hive. Error message from driver is the following: \n%s"%(ex))

			self.hive_cursor = self.hive_conn.cursor()

#			if self.test_hive_execution == True and forceSkipTest == False:
			if self.common_config.getConfigValue(key = "hive_validate_before_execution") == True and forceSkipTest == False:
				self.testHiveQueryExecution()

		logging.debug("Executing common_operations.connectToHive() - Finished")
	
	def testHiveQueryExecution(self,):
		logging.debug("Executing common_operations.testHiveQueryExecution()")
		logging.info("Checking Hive functionality by querying the reference table")

		hiveTable = self.common_config.getConfigValue(key = "hive_validate_table")

#		result_df = self.executeHiveQuery("select id, value, count(value) as counter from hadoop.dbimport_check_hive group by value, id")
		result_df = self.executeHiveQuery("select id, value, count(value) as counter from %s group by id, value order by id"%(hiveTable))
#		result_df = self.executeHiveQuery("select id, value, count(value) as counter from %s order by id"%(hiveTable))
		reference_df = pd.DataFrame({'id':[1,2,3,4,5], 'value':['Hive', 'LLAP', 'is', 'working', 'fine'], 'counter':[1,1,1,1,1]})

		if reference_df.equals(result_df) == False:
			logging.debug("Hive Query Result:\n%s"%(result_df))
			logging.debug("Reference table:\n%s"%(reference_df))
			raise ValueError("The Hive Query Execution test failed. That means that the result from the reference table did not return the exact same result as it should")
		logging.debug("Executing common_operations.testHiveQueryExecution() - Finished")


	def executeHiveQuery(self, query, quiet=False):
		logging.debug("Executing common_operations.executeHiveQuery()")
		# This function executes a Hive query and check that the result is correct. It works agains a pre-defined table

		# Sets the start time. We use this to determine elapsed time for the query
		hiveQueryStartTime = time.monotonic()

		# Execute the query in async mode
		self.hive_cursor.execute(query, async_=True)
		status = self.hive_cursor.poll().operationState
		headers = self.hive_cursor.poll().progressUpdateResponse.headerNames

		# Read and print the output from the async Hive query
		result_df = None
		firstOutputLine = True
		errorsFound = False
		linesToJumpUp = 0
		while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
			# If the user configured to print the logs, we do it here
			logs = self.hive_cursor.fetch_logs()
			for message in logs:
				if self.hive_print_messages == True:
					print(message)

				if re.search('^ERROR ', message):	
					logging.error("An ERROR occured in the Hive execution")
					logging.error(message)
					self.hive_print_messages = True		# Turn on logging if there is an error
					errorsFound = True

			# Print the progress of the query
			for row in self.hive_cursor.poll().progressUpdateResponse.rows:
				if firstOutputLine == True:
					# If it's the first one, we print the header and read how many vertexes there is
					linesToJumpUp = len(self.hive_cursor.poll().progressUpdateResponse.rows) + 1
					print(u"\u001b[36m")		# Cyan
					try:
						print("%-16s %12s %12s %11s %11s %9s %9s %8s %8s"%(headers[0], headers[1], headers[2], headers[3], headers[4], headers[5], headers[6], headers[7], headers[8]))
					except IndexError:
						print(headers)
					sys.stdout.write(u"\u001b[0m")	# Reset
					sys.stdout.flush()
					firstOutputLine = False

				# Print the actual vertex information of the query
				try:
					print("%-16s %12s %12s %11s %11s %9s %9s %8s %8s"%(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
				except IndexError:
					print(headers)
			
			sys.stdout.write(u"\u001b[31m")		# Red
			sys.stdout.flush()
			print("ELAPSED TIME: %.2f seconds"%(time.monotonic() - hiveQueryStartTime))
			sys.stdout.write(u"\u001b[0m")		# Reset
			sys.stdout.flush()

			if firstOutputLine == False:
				# This means that we printed atleast one chunk of mappers and reducers. Because of that, we will now move the cursor up
				# This information was found on http://www.lihaoyi.com/post/BuildyourownCommandLinewithANSIescapecodes.html
				sys.stdout.write(u"\u001b[" + str(linesToJumpUp) + "A" + u"\u001b[1000D")
				sys.stdout.flush()

			status = self.hive_cursor.poll().operationState

		# At this point, the query is not running anymore. We now print the last result of the Vertex informaton
		rowsPrinted = False
		for row in self.hive_cursor.poll().progressUpdateResponse.rows:
			try:
				print("%-16s %12s %12s %11s %11s %9s %9s %8s %8s"%(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
			except IndexError:
				print(headers)
			rowsPrinted = True
		if rowsPrinted == True: print("")		# So we get passed the elapsed time message
		print("")

		# If the user configured to print the logs, we do it here
		logs = self.hive_cursor.fetch_logs()
		if self.hive_print_messages == True:
			for message in logs:
				print(message)
			print("")
		else:
			for message in logs:
				if message.lower().startswith("error"):
					errorsFound = True
				if errorsFound == True:
					print(message)
	
		if errorsFound == True:
			raise Exception("Hive Query Error")

		result_df = pd.DataFrame()
		# Retreive the data
		try:
			result_df = pd.DataFrame(self.hive_cursor.fetchall())

			# Set the correct column namnes in the DataFrame
			result_df_columns = []
			for columns in self.hive_cursor.description:
				result_df_columns.append(columns[0])	# Name of the column is in the first position
			result_df.columns = result_df_columns
		except exc.ProgrammingError:
			logging.debug("An error was raised during hive_cursor.fetchall(). This happens during SQL operations that dont return any rows like 'create table'")
				
		logging.debug("Executing common_operations.executeHiveQuery() - Finished")
		return result_df

	def executeBeelineScript(self, hiveScriptFile, useDB=None):
		logging.debug("Executing common_operations.executeBeelineScript()")

		if useDB == None:
			useDB=""
			
		beelineConnectStr = "jdbc:hive2://%s:%s/%s;principal=%s/_HOST@%s"%(self.hive_hostname, self.hive_port, useDB, self.hive_kerberos_service_name, self.hive_kerberos_realm)
		logging.debug("JDBC Connection String: %s"%(beelineConnectStr))

		beelineCommand = ["beeline", "--silent=true", "-u", beelineConnectStr, "-f", hiveScriptFile]

		# Start Beeline
		sh_session = subprocess.Popen(beelineCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

		# Print Stdout and stderr while sqoop is running
		while sh_session.poll() == None:
			row = sh_session.stdout.readline().decode('utf-8').rstrip()
			if row != "":
				print(row)
				sys.stdout.flush()

		# Print what is left in output after sqoop finished
		for row in sh_session.stdout.readlines():
			row = row.decode('utf-8').rstrip()
			if row != "":
				print(row)
				sys.stdout.flush()

		logging.debug("Executing common_operations.executeBeelineScript() - Finished")

	def getHiveTableRowCount(self, hiveDB, hiveTable, whereStatement=None):
		logging.debug("Executing common_operations.getHiveTableRowCount()")
		logging.info("Reading the number of rows from %s.%s"%(hiveDB, hiveTable))
		rowCount = 0

		query = "select count(1) as rowcount from `%s`.`%s` "%(hiveDB, hiveTable)
		if whereStatement != None:
			query += "where " + whereStatement

		result_df = self.executeHiveQuery(query)
		rowCount = int(result_df['rowcount'].iloc[0])
		logging.debug("Rowcount from %s.%s: %s"%(hiveDB, hiveTable, rowCount))

		logging.debug("Executing common_operations.getHiveTableRowCount() - Finished")
		return rowCount

	def dropHiveTable(self, hiveDB, hiveTable):
		logging.debug("Executing common_operations.dropHiveTable()")
		logging.info("Dropping table %s.%s"%(hiveDB, hiveTable))

		self.executeHiveQuery("drop table if exists `%s`.`%s`"%(hiveDB, hiveTable))

		logging.debug("Executing common_operations.dropHiveTable() - Finished")

	def getPKfromTable(self, hiveDB, hiveTable, quotedColumns=False):
		""" Reads the PK from the Hive Metadatabase and return a comma separated string with the information """
		logging.debug("Executing common_operations.getPKfromTable()")
		result = ""

		PKerrorFound = False

		session = self.hiveMetaSession()
		KEY_CONSTRAINTS = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
		TBLS = aliased(hiveSchema.TBLS, name="T")
		COLUMNS_V2 = aliased(hiveSchema.COLUMNS_V2, name="C")
		DBS = aliased(hiveSchema.DBS, name="D")

		for row in (session.query(COLUMNS_V2.COLUMN_NAME)
				.select_from(KEY_CONSTRAINTS)
				.join(TBLS, KEY_CONSTRAINTS.TBLS_PARENT)
				.join(COLUMNS_V2, (COLUMNS_V2.CD_ID == KEY_CONSTRAINTS.PARENT_CD_ID) & (COLUMNS_V2.INTEGER_IDX == KEY_CONSTRAINTS.PARENT_INTEGER_IDX))
				.join(DBS)
				.filter(KEY_CONSTRAINTS.CONSTRAINT_TYPE == 0)
				.filter(TBLS.TBL_NAME == hiveTable.lower())
				.filter(DBS.NAME == hiveDB.lower())
				.order_by(KEY_CONSTRAINTS.POSITION)
				.all()):

#		query  = "select c.COLUMN_NAME as column_name "
#		query += "from KEY_CONSTRAINTS k "
#		query += "   left join TBLS t on k.PARENT_TBL_ID = t.TBL_ID "
#		query += "   left join COLUMNS_V2 c on k.PARENT_CD_ID = c.CD_ID and k.PARENT_INTEGER_IDX = c.INTEGER_IDX "
#		query += "   left join DBS d on t.DB_ID = d.DB_ID "
#		query += "where k.CONSTRAINT_TYPE = 0 "
#		query += "   and d.NAME = %s "
#		query += "   and t.TBL_NAME = %s "
#		query += "order by k.POSITION" 
#
#		self.mysql_cursor.execute(query, (hiveDB.lower(), hiveTable.lower() ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
#
#		for row in self.mysql_cursor.fetchall():

			if result != "":
				result += ","
			if row[0] == None:
				PKerrorFound = True
				break
			if quotedColumns == False:
				result += row[0]
			else:
				result += "`%s`"%(row[0])

		if PKerrorFound == True: result = ""

		logging.debug("Primary Key columns: %s"%(result))
		logging.debug("Executing common_operations.getPKfromTable() - Finished")
		return result

	def getPKname(self, hiveDB, hiveTable):
		""" Returns the name of the Primary Key that exists on the table. Returns None if it doesnt exist """
		logging.debug("Executing common_operations.getPKname()")

		session = self.hiveMetaSession()
		KEY = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
		TBLS = aliased(hiveSchema.TBLS, name="T")
		DBS = aliased(hiveSchema.DBS, name="D")

		try:
			row = (session.query(KEY.CONSTRAINT_NAME)
				.select_from(KEY)
				.join(TBLS, KEY.TBLS_PARENT)
				.join(DBS)
				.filter(KEY.CONSTRAINT_TYPE == 0)
				.filter(TBLS.TBL_NAME == hiveTable.lower())
				.filter(DBS.NAME == hiveDB.lower())
				.group_by(KEY.CONSTRAINT_NAME)
				.one())
		except sa.orm.exc.NoResultFound:
			logging.debug("PK Name: <None>")
			logging.debug("Executing common_operations.getPKname() - Finished")
			return None

		logging.debug("PK Name: %s"%(row[0]))
		logging.debug("Executing common_operations.getPKname() - Finished")
		return row[0]

#		query  = "select k.CONSTRAINT_NAME "
#		query += "from KEY_CONSTRAINTS k "
#		query += "   left join TBLS t on k.PARENT_TBL_ID = t.TBL_ID "
#		query += "   left join DBS d on t.DB_ID = d.DB_ID "
#		query += "where k.CONSTRAINT_TYPE = 0 "
#		query += "   and d.NAME = %s "
#		query += "   and t.TBL_NAME = %s "
#		query += "group by k.CONSTRAINT_NAME" 
#
#		self.mysql_cursor.execute(query, (hiveDB.lower(), hiveTable.lower() ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
#
#		if self.mysql_cursor.rowcount == 0:
#			logging.debug("Executing common_operations.getPKname() - Finished")
#			return None
#		else:
#			row = self.mysql_cursor.fetchone()
#			print(row)
#			logging.debug("Executing common_operations.getPKname() - Finished")
#			return row[0]

	def getForeignKeysFromHive(self, hiveDB, hiveTable):
		""" Reads the ForeignKeys from the Hive Metastore tables and return the result in a Pandas DF """
		logging.debug("Executing common_operations.getForeignKeysFromHive()")
		result_df = None

		session = self.hiveMetaSession()
		KEY = aliased(hiveSchema.KEY_CONSTRAINTS, name="K")
		TBLS_TP = aliased(hiveSchema.TBLS, name="TP")
		TBLS_TC = aliased(hiveSchema.TBLS, name="TC")
		COLUMNS_CP = aliased(hiveSchema.COLUMNS_V2, name="CP")
		COLUMNS_CC = aliased(hiveSchema.COLUMNS_V2, name="CC")
		DBS_DP = aliased(hiveSchema.DBS, name="DP")
		DBS_DC = aliased(hiveSchema.DBS, name="DC")

		result_df = pd.DataFrame(session.query(
					KEY.POSITION.label("fk_index"),
					DBS_DC.NAME.label("source_hive_db"),
					TBLS_TC.TBL_NAME.label("source_hive_table"),
					DBS_DP.NAME.label("ref_hive_db"),
					TBLS_TP.TBL_NAME.label("ref_hive_table"),
					COLUMNS_CC.COLUMN_NAME.label("source_column_name"),
					COLUMNS_CP.COLUMN_NAME.label("ref_column_name"),
					KEY.CONSTRAINT_NAME.label("fk_name")
				)
				.select_from(KEY)
				.join(TBLS_TC, KEY.TBLS_CHILD, isouter=True)
				.join(TBLS_TP, KEY.TBLS_PARENT, isouter=True)
				.join(COLUMNS_CP, (COLUMNS_CP.CD_ID == KEY.PARENT_CD_ID) & (COLUMNS_CP.INTEGER_IDX == KEY.PARENT_INTEGER_IDX), isouter=True)
				.join(COLUMNS_CC, (COLUMNS_CC.CD_ID == KEY.CHILD_CD_ID) & (COLUMNS_CC.INTEGER_IDX == KEY.CHILD_INTEGER_IDX), isouter=True)
				.join(DBS_DC, isouter=True)
				.join(DBS_DP, isouter=True)
				.filter(KEY.CONSTRAINT_TYPE == 1)
				.filter(DBS_DC.NAME == hiveDB)
				.filter(TBLS_TC.TBL_NAME == hiveTable)
				.order_by(KEY.POSITION)
				.all()).fillna('')

		if len(result_df) == 0:
			return pd.DataFrame(columns=['fk_name', 'source_hive_db', 'source_hive_table', 'ref_hive_db', 'ref_hive_table', 'source_column_name', 'ref_column_name'])

		result_df = (result_df.groupby(by=['fk_name', 'source_hive_db', 'source_hive_table', 'ref_hive_db', 'ref_hive_table'] )
			.aggregate({'source_column_name': lambda a: ",".join(a),
						'ref_column_name': lambda a: ",".join(a)})
			.reset_index())

		logging.debug("Executing common_operations.getForeignKeysFromHive() - Finished")
		return result_df.sort_values(by=['fk_name'], ascending=True)

	def removeHiveLocksByForce(self, hiveDB, hiveTable):
		""" Removes the locks on the Hive table from the Hive Metadatabase """
		logging.debug("Executing common_operations.removeHiveLocksByForce()")

		session = self.hiveMetaSession()

		lockCount = (session.query(hiveSchema.HIVE_LOCKS)
					.filter(hiveSchema.HIVE_LOCKS.HL_DB == hiveDB)
					.filter(hiveSchema.HIVE_LOCKS.HL_TABLE == hiveTable)
					.count()
					) 

		if lockCount > 0:
			logging.warning("Removing %s locks from table %s.%s by force"%(lockCount, hiveDB, hiveTable))
			self.common_config.logImportFailure(hiveDB=hiveDB, hiveTable=hiveTable, severity="Warning", errorText="%s locks removed by force"%(lockCount))  

		(session.query(hiveSchema.HIVE_LOCKS)
			.filter(hiveSchema.HIVE_LOCKS.HL_DB == hiveDB)
			.filter(hiveSchema.HIVE_LOCKS.HL_TABLE == hiveTable)
			.delete()
			) 
		session.commit()
	
#		query  = "delete from HIVE_LOCKS where HL_DB = %s and HL_TABLE = %s " 
#		self.mysql_cursor.execute(query, (hiveDB, hiveTable ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
#		self.mysql_conn.commit()

		logging.debug("Executing common_operations.removeHiveLocksByForce() - Finished")

	def truncateHiveTable(self, hiveDB, hiveTable):
		""" Truncates a Hive table """
		logging.debug("Executing common_operations.truncateHiveTable()")

		self.executeHiveQuery("truncate table `%s`.`%s`"%(hiveDB, hiveTable))
		logging.debug("Executing common_operations.truncateHiveTable() - Finished")

	def getHiveColumns(self, hiveDB, hiveTable, includeType=False, includeComment=False, includeIdx=False, forceColumnUppercase=False, excludeDataLakeColumns=False):
		""" Returns a pandas DataFrame with all columns in the specified Hive table """		
		logging.debug("Executing common_operations.getHiveColumns()")

		session = self.hiveMetaSession()
		TBLS = aliased(hiveSchema.TBLS, name="T")
		COLUMNS = aliased(hiveSchema.COLUMNS_V2, name="C")
		DBS = aliased(hiveSchema.DBS, name="D")
		SDS = aliased(hiveSchema.SDS, name="S")
		CDS = aliased(hiveSchema.CDS)

		result_df = pd.DataFrame(session.query(
					COLUMNS.COLUMN_NAME.label("name"),
					COLUMNS.TYPE_NAME.label("type"),
					COLUMNS.COMMENT.label("comment"),
					COLUMNS.INTEGER_IDX.label("idx")
				)
				.select_from(CDS)
				.join(SDS)
				.join(COLUMNS)
				.join(TBLS)
				.join(DBS)
				.filter(TBLS.TBL_NAME == hiveTable)
				.filter(DBS.NAME == hiveDB)
				.order_by(COLUMNS.INTEGER_IDX)
				.all())

		if includeType == False:
			result_df.drop('type', axis=1, inplace=True)
			
		if includeComment == False:
			result_df.drop('comment', axis=1, inplace=True)
			
		if includeIdx == False:
			result_df.drop('idx', axis=1, inplace=True)
			
		if forceColumnUppercase == True:
			result_df['name'] = result_df['name'].str.upper()
			
		if excludeDataLakeColumns == True:
			result_df = result_df[~result_df['name'].astype(str).str.startswith('datalake_')]

		logging.debug("Executing common_operations.getHiveColumns() - Finished")
		return result_df

	def updateHiveTableStatistics(self, hiveDB, hiveTable):
		""" Compute Hive statistics on a Hive table """
		logging.debug("Executing common_operations.updateHiveTableStatistics()")

		self.executeHiveQuery("analyze table `%s`.`%s` compute statistics "%(hiveDB, hiveTable))
		logging.debug("Executing common_operations.updateHiveTableStatistics() - Finished")


	def convertHiveTableToACID(self, hiveDB, hiveTable, createDeleteColumn=False, createMergeColumns=False):
		""" Checks if a table is ACID or not, and it it isn't, it converts the table to an ACID table """
		# TODO: HDP3 shouldnt run this
		logging.debug("Executing common_operations.convertHiveTableToACID()")
		logging.info("Converting table to an ACID & Transactional enabled table")

#		buckets = self.calculateNumberOfBuckets(hiveDB, hiveTable)
		buckets = 1
		hiveTableBucketed = "%s_hive_bucketed"%(hiveTable)
		logging.info("The new table will use %s buckets"%(buckets))

		if self.checkHiveTable(hiveDB=hiveDB, hiveTable=hiveTableBucketed) == True:
			logging.warning("Temporary table used during conversion to ACID already exists. We will drop this table and recreate it again")
			self.executeHiveQuery("drop table `%s`.`%s` "%(hiveDB, hiveTableBucketed))

		columnDF = self.getHiveColumns(hiveDB=hiveDB, hiveTable=hiveTable, includeType=True, includeComment=True)
		hiveColumnsCreate = []
		hiveColumnsInsert = []
		hiveColumnsCreateAdding = []
		hiveColumnsInsertAdding = []
		datalakeImportExists = False

		PKColumns = self.getPKfromTable(hiveDB=hiveDB, hiveTable=hiveTable, quotedColumns=True)
		if PKColumns == None or PKColumns == "":
			logging.error("There are no PrimaryKey defined on the table. Please create one first before converting to ACID (PK is needed for bucket columns)")
			self.common_config.remove_temporary_files()
			sys.exit(1)

		for index, row in columnDF.iterrows():
			columnName = row['name']
			columnType = row['type']
			columnComment = row['comment']

			if columnName in ["datalake_import"] and createMergeColumns == True:
				datalakeImportExists = True
				continue

			if columnComment != None and columnComment != "":
				hiveColumnsCreate.append("%s %s COMMENT \"%s\""%(columnName, columnType, columnComment))
			else:
				hiveColumnsCreate.append("%s %s"%(columnName, columnType))
			hiveColumnsInsert.append(columnName)

		if createMergeColumns == True:
			hiveColumnsCreateAdding.append("datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\"")
			hiveColumnsCreateAdding.append("datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\"")
			hiveColumnsCreateAdding.append("datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\"")
			hiveColumnsInsertAdding.append("datalake_iud")
			hiveColumnsInsertAdding.append("datalake_insert")
			hiveColumnsInsertAdding.append("datalake_update")

			if createDeleteColumn == True:
				hiveColumnsCreateAdding.append("datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\"")
				hiveColumnsInsertAdding.append("datalake_delete")

		# Create the Bucketed table
		query  = "create table `%s`.`%s` "%(hiveDB, hiveTableBucketed)
		query += "( %s"%(", ".join(hiveColumnsCreate))	
		if createMergeColumns == True:
			query += ", %s"%(", ".join(hiveColumnsCreateAdding))	
		query += ") clustered by (%s) into %s buckets stored as orc tblproperties ('orc.compress'='ZLIB', 'transactional'='true')"%(PKColumns, buckets)
		self.executeHiveQuery(query)

		# Insert data into the Bucketed table
		query  = "insert into `%s`.`%s` "%(hiveDB, hiveTableBucketed)
		query += "( %s"%(", ".join(hiveColumnsInsert))	
		if createMergeColumns == True:
			query += ", %s"%(", ".join(hiveColumnsInsertAdding))	
		query += ") "
		query += "select "	
		query += "%s"%(", ".join(hiveColumnsInsert))
		if createMergeColumns == True:
			if datalakeImportExists == True:
				query += ", 'I', datalake_import, datalake_import"
			else:
				query += ", 'I', timestamp(\"1900-01-01 00:00:00.000\"), timestamp(\"1900-01-01 00:00:00.000\")"

			if createDeleteColumn == True:
				query += ", NULL"
		query += " from `%s`.`%s` "%(hiveDB, hiveTable)
		self.executeHiveQuery(query)

		# Drop Target table
		query = "drop table `%s`.`%s` "%(hiveDB, hiveTable)
		self.executeHiveQuery(query)

		# Rename bucketed table to Target table
		query = "alter table `%s`.`%s` rename to `%s`.`%s`"%(hiveDB, hiveTableBucketed, hiveDB, hiveTable)
		self.executeHiveQuery(query)

		self.reconnectHiveMetaStore()

		logging.debug("Executing common_operations.convertHiveTableToACID() - Finished")

	def isHiveTableTransactional(self, hiveDB, hiveTable):
		logging.debug("Executing common_operations.isHiveTableTransactional()")

		session = self.hiveMetaSession()
		TABLE_PARAMS = aliased(hiveSchema.TABLE_PARAMS, name="TP")
		TBLS = aliased(hiveSchema.TBLS, name="T")
		DBS = aliased(hiveSchema.DBS, name="D")

		row = (session.query(
				TABLE_PARAMS.PARAM_VALUE.label("param_value")
			)
			.select_from(TABLE_PARAMS)
			.join(TBLS)
			.join(DBS)
			.filter(TBLS.TBL_NAME == hiveTable)
			.filter(DBS.NAME == hiveDB)
			.filter(TABLE_PARAMS.PARAM_KEY == 'transactional')
			.one_or_none())

#		query  = "select tp.PARAM_VALUE "
#		query += "from TABLE_PARAMS tp "
#		query += "	left join TBLS t on tp.TBL_ID = t.TBL_ID "
#		query += "	left join DBS d on t.DB_ID = d.DB_ID "
#		query += "where "
#		query += "	d.NAME = %s "
#		query += "	and t.TBL_NAME = %s "
#		query += "	and tp.PARAM_KEY = 'transactional' "
#
#		self.mysql_cursor.execute(query, (hiveDB.lower(), hiveTable.lower() ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
#
#		row = self.mysql_cursor.fetchone()

		returnValue = False
		if row != None:
			if row[0].lower() == "true":
				returnValue = True

		logging.debug("isHiveTableTransactional = %s"%(returnValue))
		logging.debug("Executing common_operations.isHiveTableTransactional() - Finished")
		return returnValue

	def isHiveTableView(self, hiveDB, hiveTable):
		logging.debug("Executing common_operations.isHiveTableView()")

		session = self.hiveMetaSession()
		TBLS = aliased(hiveSchema.TBLS, name="T")
		DBS = aliased(hiveSchema.DBS, name="D")

		row = (session.query(
				TBLS.TBL_TYPE
			)
			.select_from(TBLS)
			.join(DBS)
			.filter(TBLS.TBL_NAME == hiveTable)
			.filter(DBS.NAME == hiveDB)
			.one_or_none())

		returnValue = False

		if row[0] == "VIRTUAL_VIEW":
			returnValue = True

		logging.debug("isHiveTableView = %s"%(returnValue))
		logging.debug("Executing common_operations.isHiveTableView() - Finished")
		return returnValue

	def getHiveColumnNameDiff(self, sourceDB, sourceTable, targetDB, targetTable, sourceIsImportTable=False):
		""" Returns a dataframe that includes the diff between the source and target table. If sourceIsImportTable=True, then there is additional columns that contains the correct name for the import table columns """
		logging.debug("Executing common_operations.getHiveColumnNameDiff()")

		sourceColumns = self.getHiveColumns(hiveDB=sourceDB, hiveTable=sourceTable, includeType=False, includeComment=False)
		targetColumns = self.getHiveColumns(hiveDB=targetDB, hiveTable=targetTable, includeType=False, includeComment=False)

		if sourceIsImportTable == True:
			# Logic here is to create a new column in both DF and call them sourceName vs targetName. These are the original names. Then we replace the values in targetColumn DF column col
			# with the name that the column should be called in the source system. This is needed to handle characters that is not supported in Parquet files, like SPACE
			sourceColumns['sourceName'] = sourceColumns['name']
			targetColumns['targetName'] = targetColumns['name']

			# If you change any of the name replace operations, you also need to change the same data in function self.updateColumnsForImportTable() and import_definitions.saveColumnData()
			targetColumns['name'] = targetColumns['name'].str.replace(r' ', '_')
			targetColumns['name'] = targetColumns['name'].str.replace(r'\%', 'pct')
			targetColumns['name'] = targetColumns['name'].str.replace(r'\(', '_')
			targetColumns['name'] = targetColumns['name'].str.replace(r'\)', '_')
			targetColumns['name'] = targetColumns['name'].str.replace(r'ü', 'u')
			targetColumns['name'] = targetColumns['name'].str.replace(r'å', 'a')
			targetColumns['name'] = targetColumns['name'].str.replace(r'ä', 'a')
			targetColumns['name'] = targetColumns['name'].str.replace(r'ö', 'o')
			targetColumns['name'] = targetColumns['name'].str.replace(r'#', 'hash')
#			targetColumns['name'] = targetColumns['name'].str.replace(r'^_', '')
			targetColumns['name'] = targetColumns['name'].str.replace(r'^_', 'underscore_')

		columnMerge = pd.merge(sourceColumns, targetColumns, on=None, how='outer', indicator='Exist')
		logging.debug("\n%s"%(columnMerge))

		logging.debug("Executing common_operations.getHiveColumnNameDiff() - Finished")
		return columnMerge

	def getHiveTables(self, dbFilter=None, tableFilter=None):
		""" Return all tables from specific Hive DB """
		logging.debug("Executing common_operations.getHiveTables()")
		result_df = None

		if dbFilter != None:
			dbFilter = dbFilter.replace('*', '%')
		else:
			dbFilter = "%"

		if tableFilter != None:
			tableFilter = tableFilter.replace('*', '%')
		else:
			tableFilter = "%"

		session = self.hiveMetaSession()
		TBLS = aliased(hiveSchema.TBLS, name="T")
		DBS = aliased(hiveSchema.DBS, name="D")

		result_df = pd.DataFrame(session.query(
				DBS.NAME.label('hiveDB'),
				TBLS.TBL_NAME.label('hiveTable')
			)
			.select_from(TBLS)
			.join(DBS)
			.filter(TBLS.TBL_NAME.like(tableFilter))
			.filter(DBS.NAME.like(dbFilter))
			.order_by(DBS.NAME, TBLS.TBL_NAME)
			.all())

		if len(result_df) == 0:
			return pd.DataFrame(columns=['hiveDB', 'hiveTable'])

#		query  = "select d.NAME as hiveDB, t.TBL_NAME as hiveTable "
#		query += "from TBLS t "
#		query += "	left join DBS d on t.DB_ID = d.DB_ID "
#
#		if dbFilter != None:
#			query += "where d.NAME like '%s' "%(dbFilter)
#		if tableFilter != None:
#			if dbFilter != None:
#				query += "and t.TBL_NAME like '%s' "%(tableFilter)
#			else:
#				query += "where t.TBL_NAME like '%s' "%(tableFilter)
#		query += "order by d.NAME, t.TBL_NAME"
#
#
#		self.mysql_cursor.execute(query)
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
#
#		if self.mysql_cursor.rowcount == 0:
#			return pd.DataFrame()
#
#		result_df = pd.DataFrame(self.mysql_cursor.fetchall())
#
#		if len(result_df) > 0:
#			result_df.columns = ['hiveDB', 'hiveTable']
#
#		print(result_df)

		logging.debug("Executing common_operations.getHiveTables() - Finished")
		return result_df


