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
# from time import sleep
import time 
from reprint import output
import requests
from pyhive import hive
from pyhive import exc
from TCLIService.ttypes import TOperationState
from requests_kerberos import HTTPKerberosAuth, REQUIRED
import kerberos
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, timedelta
import pandas as pd

class operation(object):
	def __init__(self, Hive_DB, Hive_Table):
		logging.debug("Executing common_operation.__init__()")

		self.Hive_DB = Hive_DB.lower()	 
		self.Hive_Table = Hive_Table.lower()	 
		self.mysql_conn = None
		self.mysql_cursor = None
		self.hive_conn = None
		self.hive_cursor = None

		# Fetch and initialize the Kerberos configuration
		self.kerberosPrincipal = configuration.get("Kerberos", "principal")
		self.webHCatAuth = HTTPKerberosAuth(force_preemptive=True, principal=self.kerberosPrincipal)

		# Fetch the Hive metastore configuration
#		self.hiveMetaStoreList = configuration.get("Hive", "metastore").split(",")
#		self.checkHiveMetaStore()

		# TODO: Handle Kerberos init and renewal

		# Fetch configuration about MySQL database for Hive and how to connect to it
		self.metastore_mysql_hostname = configuration.get("Hive", "metastore_hostname")
		self.metastore_mysql_port =     configuration.get("Hive", "metastore_port")
		self.metastore_mysql_database = configuration.get("Hive", "metastore_database")
		self.metastore_mysql_username = configuration.get("Hive", "metastore_username")
		self.metastore_mysql_password = configuration.get("Hive", "metastore_password")

		# Fetch configuration details about Hive LLAP
		self.hive_hostname = configuration.get("Hive", "hostname")
		self.hive_port = configuration.get("Hive", "port")
		self.hive_kerberos_service_name = configuration.get("Hive", "kerberos_service_name")
		if configuration.get("Hive", "print_hive_message").lower() == "true":
			self.hive_print_messages = True
		else:
			self.hive_print_messages = False
		if configuration.get("Hive", "test_hive_execution").lower() == "true":
			self.test_hive_execution = True
		else:
			self.test_hive_execution = False

#
		# Esablish a connection to the Hive Metastore database in MySQL
		try:
			self.mysql_conn = mysql.connector.connect(host=self.metastore_mysql_hostname, 
												 port=self.metastore_mysql_port, 
												 database=self.metastore_mysql_database, 
												 user=self.metastore_mysql_username, 
												 password=self.metastore_mysql_password)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				logging.error("Something is wrong with your user name or password")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				logging.error("Database does not exist")
			else:
				logging.error("%s"%err)
			logging.error("Error: There was a problem connecting to the Hive Metastore database. Please check configuration and try again")
			raise Exception
		else:
#			self.mysql_conn.autocommit(True)
			self.mysql_cursor = self.mysql_conn.cursor(buffered=True)
#			self.mysql_cursor_unbuffed = self.mysql_conn.cursor(buffered=False)

		logging.debug("Executing common_operations.__init__() - Finished")

	def reconnectHiveMetaStore(self):
		self.mysql_conn.close()
		self.mysql_conn = mysql.connector.connect(host=self.metastore_mysql_hostname, 
							 port=self.metastore_mysql_port, 
							 database=self.metastore_mysql_database, 
							 user=self.metastore_mysql_username, 
							 password=self.metastore_mysql_password)
		self.mysql_cursor = self.mysql_conn.cursor(buffered=True)

	def checkHiveMetaStore(self):
		logging.debug("Executing common_definitions.checkHiveMetaStore()")

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

		logging.debug("Executing common_definitions.checkHiveMetaStore() - Finished")

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

	def checkHiveDB(self, hiveDB):
		logging.debug("Executing common_definitions.checkHiveDB()")

		query = "select name from DBS where name = %s"
		self.mysql_cursor.execute(query, (hiveDB, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		row = self.mysql_cursor.fetchone()
		if row[0] != hiveDB:
			logging.error("Can't find database '%s' in Hive"%(hiveDB))
			raise Exception
	
		logging.debug("Executing common_definitions.checkHiveDB() - Finished")

	def checkHiveTable(self, hiveDB, hiveTable):
		logging.debug("Executing common_definitions.checkHiveTable()")

		query = "select t.TBL_NAME, t.TBL_TYPE from TBLS t left join DBS d on t.DB_ID = d.DB_ID where d.NAME = %s and t.TBL_NAME = %s"
		self.mysql_cursor.execute(query, (hiveDB.lower(), hiveTable.lower() ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		if self.mysql_cursor.rowcount == 1:
			return True
		else:
			return False
	
	def isHiveTableExternalParquetFormat(self, hiveDB, hiveTable):
		logging.debug("Executing common_definitions.isExternalHiveTableParquetFormat()")

		if self.isHiveTableExternal(hiveDB, hiveTable) == False: return False

		query  = "select s.INPUT_FORMAT from TBLS t "
		query += "   left join SDS s on t.SD_ID = s.SD_ID "
		query += "   left join DBS d on t.DB_ID = d.DB_ID "
		query += "where d.NAME = %s and t.TBL_NAME = %s"
		self.mysql_cursor.execute(query, (hiveDB.lower(), hiveTable.lower() ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		row = self.mysql_cursor.fetchone()
		if row[0] == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat":
			return True
		else:
			return False

	def isHiveTableExternal(self, hiveDB, hiveTable):
		logging.debug("Executing common_definitions.isHiveTableExternal()")

		query = "select t.TBL_TYPE from TBLS t left join DBS d on t.DB_ID = d.DB_ID where d.NAME = %s and t.TBL_NAME = %s"
		self.mysql_cursor.execute(query, (hiveDB.lower(), hiveTable.lower() ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		row = self.mysql_cursor.fetchone()
		if row[0] == "EXTERNAL_TABLE":
			return True
		else:
			return False

	def checkFK(self, FKname):
		""" Checks if the ForeignKey with the specified name exists in Hive """
		logging.debug("Executing common_definitions.checkFK()")

		query = "select count(CONSTRAINT_NAME) from KEY_CONSTRAINTS where CONSTRAINT_TYPE = 1 and CONSTRAINT_NAME = %s"
		self.mysql_cursor.execute(query, (FKname.lower(), ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		row = self.mysql_cursor.fetchone()
		if row[0] == 1:
			return True
		else:
			return False
	
	def connectToHive(self,):
		logging.debug("Executing common_definitions.connectToHive()")

		# Make sure we only connect if we havent done so before. Reason is that this function can be called from many different places
		# due to the retry functionallity
		if self.hive_cursor == None:
			logging.info("Connecting to Hive LLAP")
			try:
				# TODO: Remove error messages output from hive.connect. Check this by entering a wrong hostname or port
				self.hive_conn = hive.connect(host = self.hive_hostname, port = self.hive_port, database = "default", auth = "KERBEROS", kerberos_service_name = self.hive_kerberos_service_name, configuration = {'hive.llap.execution.mode': 'none'} )
			except Exception as ex:
				raise ValueError("Could not connect to Hive. Error message from driver is the following: \n%s"%(ex))

			self.hive_cursor = self.hive_conn.cursor()
			if self.test_hive_execution == True:
				self.testHiveQueryExecution()
		logging.debug("Executing common_definitions.connectToHive() - Finished")
	
	def testHiveQueryExecution(self,):
		logging.debug("Executing common_definitions.testHiveQueryExecution()")
		logging.info("Checking Hive functionality by querying the reference table")

		result_df = self.executeHiveQuery("select id, value, count(value) as counter from hadoop.dbimport_check_hive group by value, id")
		reference_df = pd.DataFrame({'id':[1,2,3,4,5], 'value':['Hive', 'LLAP', 'is', 'working', 'fine'], 'counter':[1,1,1,1,1]})

		if reference_df.equals(result_df) == False:
			logging.debug("Hive Query Result:\n%s"%(result_df))
			logging.debug("Reference table:\n%s"%(reference_df))
			raise ValueError("The Hive Query Execution test failed. That means that the result from the reference table did not return the exact same result as it should")
		logging.debug("Executing common_definitions.testHiveQueryExecution() - Finished")


	def executeHiveQuery(self, query):
		logging.debug("Executing common_definitions.executeHiveQuery()")
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
		linesToJumpUp = 0
		while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
			# If the user configured to print the logs, we do it here
			logs = self.hive_cursor.fetch_logs()
			if self.hive_print_messages == True:
				for message in logs:
					print(message)

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
			errorsFound = False
			for message in logs:
				if message.lower().startswith("error"):
					errorsFound = True
				if errorsFound == True:
					print(message)
	
		if errorsFound == True:
			raise Exception("Hive Query Error")

		result_df = None
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
				
		logging.debug("Executing common_definitions.executeHiveQuery() - Finished")
		return result_df

	def getHiveTableRowCount(self, hiveDB, hiveTable):
		logging.debug("Executing common_definitions.getHiveTableRowCount()")
		logging.info("Reading the number of rows from %s.%s"%(hiveDB, hiveTable))
		rowCount = 0

		result_df = self.executeHiveQuery("select count(1) as rowcount from %s.%s"%(hiveDB, hiveTable))
		rowCount = int(result_df['rowcount'].iloc[0])
		logging.debug("Rowcount from %s.%s: %s"%(hiveDB, hiveTable, rowCount))

		logging.debug("Executing common_definitions.getHiveTableRowCount() - Finished")
		return rowCount

	def dropHiveTable(self, hiveDB, hiveTable):
		logging.debug("Executing common_definitions.dropHiveTable()")
		logging.info("Dropping table %s.%s"%(hiveDB, hiveTable))

		self.executeHiveQuery("drop table if exists `%s`.`%s`"%(hiveDB, hiveTable))

		logging.debug("Executing common_definitions.dropHiveTable() - Finished")

	def getPKfromTable(self, hiveDB, hiveTable):
		""" Reads the PK from the Hive Metadatabase and return a comma separated string with the information """
		logging.debug("Executing common_definitions.getPKfromTable()")
		result = ""

		query  = "select c.COLUMN_NAME as column_name "
		query += "from KEY_CONSTRAINTS k "
		query += "   left join TBLS t on k.PARENT_TBL_ID = t.TBL_ID "
		query += "   left join COLUMNS_V2 c on k.PARENT_CD_ID = c.CD_ID and k.PARENT_INTEGER_IDX = c.INTEGER_IDX "
		query += "   left join DBS d on t.DB_ID = d.DB_ID "
		query += "where k.CONSTRAINT_TYPE = 0 "
		query += "   and d.NAME = %s "
		query += "   and t.TBL_NAME = %s "
		query += "order by k.POSITION" 

		self.mysql_cursor.execute(query, (hiveDB.lower(), hiveTable.lower() ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		PKerrorFound = False
		for row in self.mysql_cursor.fetchall():
			if result != "":
				result += ","
			if row[0] == None:
				PKerrorFound = True
				break
			result += row[0]

		if PKerrorFound == True: result = ""

		logging.debug("Executing common_definitions.getPKfromTable() - Finished")
		return result

	def getPKname(self, hiveDB, hiveTable):
		""" Returns the name of the Primary Key that exists on the table. Returns None if it doesnt exist """
		logging.debug("Executing common_definitions.getPKname()")

		query  = "select k.CONSTRAINT_NAME "
		query += "from KEY_CONSTRAINTS k "
		query += "   left join TBLS t on k.PARENT_TBL_ID = t.TBL_ID "
		query += "   left join DBS d on t.DB_ID = d.DB_ID "
		query += "where k.CONSTRAINT_TYPE = 0 "
		query += "   and d.NAME = %s "
		query += "   and t.TBL_NAME = %s "
		query += "group by k.CONSTRAINT_NAME" 

		self.mysql_cursor.execute(query, (hiveDB.lower(), hiveTable.lower() ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		if self.mysql_cursor.rowcount == 0:
			logging.debug("Executing common_definitions.getPKname() - Finished")
			return None
		else:
			row = self.mysql_cursor.fetchone()
			logging.debug("Executing common_definitions.getPKname() - Finished")
			return row[0]

	def getForeignKeysFromHive(self, hiveDB, hiveTable):
		""" Reads the ForeignKeys from the Hive Metastore tables and return the result in a Pandas DF """
		logging.debug("Executing import_definitions.getForeignKeysFromHive()")
		result_df = None

		query  = "select " 
		query += "   coalesce(k.POSITION, '') as fk_index, "
		query += "   coalesce(dc.NAME, '') as source_hive_db, "
		query += "   coalesce(tc.TBL_NAME, '') as source_hive_table, " 
		query += "   coalesce(dp.NAME, '') as ref_hive_db, "
		query += "   coalesce(tp.TBL_NAME, '') as ref_hive_table, " 
		query += "   coalesce(cc.COLUMN_NAME, '') as source_column_name, "
		query += "   coalesce(cp.COLUMN_NAME, '') as ref_column_name, "
		query += "   coalesce(k.CONSTRAINT_NAME, '') as fk_name "
		query += "from KEY_CONSTRAINTS k "
		query += "   left join TBLS tp on k.PARENT_TBL_ID = tp.TBL_ID "
		query += "   left join TBLS tc on k.CHILD_TBL_ID = tc.TBL_ID "
		query += "   left join COLUMNS_V2 cp on k.PARENT_CD_ID = cp.CD_ID and k.PARENT_INTEGER_IDX = cp.INTEGER_IDX "
		query += "   left join COLUMNS_V2 cc on k.CHILD_CD_ID = cc.CD_ID and k.CHILD_INTEGER_IDX = cc.INTEGER_IDX "
		query += "   left join DBS dc on tc.DB_ID = dc.DB_ID "
		query += "   left join DBS dp on tp.DB_ID = dp.DB_ID "
		query += "where k.CONSTRAINT_TYPE = 1 and dc.NAME = %s and tc.TBL_NAME = %s "
		query += "order by k.POSITION"

		self.mysql_cursor.execute(query, (hiveDB, hiveTable ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		if self.mysql_cursor.rowcount == 0:
			return pd.DataFrame(columns=['fk_name', 'source_hive_db', 'source_hive_table', 'ref_hive_db', 'ref_hive_table', 'source_column_name', 'ref_column_name'])

		result_df = pd.DataFrame(self.mysql_cursor.fetchall())

		# Set the correct column namnes in the DataFrame
		result_df_columns = []
		for columns in self.mysql_cursor.description:
			result_df_columns.append(columns[0])    # Name of the column is in the first position
		result_df.columns = result_df_columns

		result_df = (result_df.groupby(by=['fk_name', 'source_hive_db', 'source_hive_table', 'ref_hive_db', 'ref_hive_table'] )
			.aggregate({'source_column_name': lambda a: ",".join(a),
						'ref_column_name': lambda a: ",".join(a)})
			.reset_index())

		logging.debug("Executing import_definitions.getForeignKeysFromHive() - Finished")
		return result_df.sort_values(by=['fk_name'], ascending=True)

	def removeHiveLocksByForce(self, hiveDB, hiveTable):
		""" Removes the locks on the Hive table from the Hive Metadatabase """
		logging.debug("Executing import_definitions.removeHiveLocksByForce()")

		query  = "delete from HIVE_LOCKS where HL_DB = %s and HL_TABLE = %s " 
		self.mysql_cursor.execute(query, (hiveDB, hiveTable ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
		self.mysql_conn.commit()

		logging.debug("Executing import_definitions.removeHiveLocksByForce() - Finished")

	def truncateHiveTable(self, hiveDB, hiveTable):
		""" Truncates a Hive table """
		logging.debug("Executing import_definitions.truncateHiveTable()")

		self.executeHiveQuery("truncate table `%s`.`%s`"%(hiveDB, hiveTable))
		logging.debug("Executing import_definitions.truncateHiveTable() - Finished")

	def getHiveColumns(self, hiveDB, hiveTable):
		""" Returns a pandas DataFrame with all columns in the specified Hive table """		
		logging.debug("Executing import_definitions.getHiveColumns()")

		query  = "select c.COLUMN_NAME as col "
		query += "from COLUMNS_V2 c "
		query += "   left join SDS s on c.CD_ID = s.CD_ID "
		query += "   left join TBLS t on s.SD_ID = t.SD_ID "
		query += "   left join DBS d on t.DB_ID = d.DB_ID "
		query += "where d.NAME = %s and t.TBL_NAME = %s"

		self.mysql_cursor.execute(query, (hiveDB, hiveTable ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		result_df = pd.DataFrame(self.mysql_cursor.fetchall())

		# Set the correct column namnes in the DataFrame
		result_df_columns = []
		for columns in self.mysql_cursor.description:
			result_df_columns.append(columns[0])    # Name of the column is in the first position
		result_df.columns = result_df_columns

		logging.debug("Executing import_definitions.getHiveColumns() - Finished")
		return result_df

	def updateHiveTableStatistics(self, hiveDB, hiveTable):
		""" Compute Hive statistics on a Hive table """
		logging.debug("Executing import_definitions.updateHiveTableStatistics()")

		self.executeHiveQuery("analyze table `%s`.`%s` compute statistics "%(hiveDB, hiveTable))
		logging.debug("Executing import_definitions.updateHiveTableStatistics() - Finished")

	def waitForHiveMetadataSyncCreateTable(self, hiveDB, hiveTable):
		""" We wait until the specified table exists in the Hive Metastore. We need this as the Metadatabase write operation is async """  
		logging.debug("Executing import_definitions.waitForHiveMetadataSyncCreateTable()")
		
		counter = 0
		while self.checkHiveTable(hiveDB, hiveTable) == False:
			counter += 1
			time.sleep(1)
			if counter > 10:
				print("damit....")

		logging.debug("Executing import_definitions.waitForHiveMetadataSyncCreateTable() - Finished")


	def getColumnsFromHiveTable(self, hiveDB, hiveTable, excludeDataLakeColumns=False):
		""" Reads the columns from the Hive Metastore and returns the information in a Pandas DF with the columns name, type and comment """
		logging.debug("Executing import_definitions.getColumnsFromConfigDatabase()")
		hiveColumnDefinition = ""

		query  = "select lower(c.COLUMN_NAME) as name, c.TYPE_NAME as type, c.COMMENT as comment "
		query += "from COLUMNS_V2 c "
		query += "   left join SDS s on c.CD_ID = s.CD_ID "
		query += "   left join TBLS t on s.SD_ID = t.SD_ID "
		query += "   left join DBS d on t.DB_ID = d.DB_ID "
		query += "where d.NAME = %s and t.TBL_NAME = %s "
		if excludeDataLakeColumns == True:
			query += "and lower(c.COLUMN_NAME) not like 'datalake_%' "
		query += "order by c.INTEGER_IDX "
		self.mysql_cursor.execute(query, (hiveDB, hiveTable ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		if self.mysql_cursor.rowcount == 0:
			logging.error("Error: Zero rows returned from query on 'import_table'")
			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor.statement) )
			raise Exception

		result_df = pd.DataFrame(self.mysql_cursor.fetchall())

		# Set the correct column namnes in the DataFrame
		result_df_columns = []
		for columns in self.mysql_cursor.description:
			result_df_columns.append(columns[0])    # Name of the column is in the first position
		result_df.columns = result_df_columns

		logging.debug("Executing import_definitions.getColumnsFromConfigDatabase() - Finished")
		return result_df

