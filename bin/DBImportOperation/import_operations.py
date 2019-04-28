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
from subprocess import Popen, PIPE
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from DBImportConfig import common_definitions
from DBImportConfig import import_definitions
from DBImportOperation import common_operations
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time


class operation(object):
	def __init__(self, Hive_DB=None, Hive_Table=None):
		logging.debug("Executing import_operations.__init__()")
		self.Hive_DB = None
		self.Hive_Table = None
		self.mysql_conn = None
		self.mysql_cursor = None
		self.startDate = None
		self.import_config = None
		self.sqlGeneratedHiveColumnDefinition = None
		self.sqlGeneratedSqoopQuery = None
		self.sqoopStartTimestamp = None
		self.sqoopStartUTS = None
		self.sqoopSize = None
		self.sqoopRows = None
		self.sqoopIncrMaxValuePending = None
		self.sqoopIncrNoNewRows = None

		try:
			# Initialize the two core classes. import_config will initialize common_config aswell
			self.common_operations = common_operations.operation(Hive_DB, Hive_Table)
			self.import_config = import_definitions.config(Hive_DB, Hive_Table)
		except:
			sys.exit(1)

		if Hive_DB != None and Hive_Table != None:
			self.setHiveTable(Hive_DB, Hive_Table)

		logging.debug("Executing import_operations.__init__() - Finished")

	def setHiveTable(self, Hive_DB, Hive_Table):
		""" Sets the parameters to work against a new Hive database and table """
		self.Hive_DB = Hive_DB.lower()
		self.Hive_Table = Hive_Table.lower()

		self.common_operations.setHiveTable(self.Hive_DB, self.Hive_Table)
		self.import_config.setHiveTable(self.Hive_DB, self.Hive_Table)

		try:
			self.import_config.getImportConfig()
			self.startDate = self.import_config.startDate
			self.import_config.lookupConnectionAlias()
		except:
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def remove_temporary_files(self):
		self.import_config.remove_temporary_files()

	def checkTimeWindow(self):
		self.import_config.checkTimeWindow()
	
	def setStage(self, stage, force=False):
		self.import_config.setStage(stage, force=force)
	
	def getStage(self):
		return self.import_config.getStage()
	
	def clearStage(self):
		self.import_config.clearStage()
	
	def saveRetryAttempt(self, stage):
		self.import_config.saveRetryAttempt(stage)
	
	def setStageOnlyInMemory(self):
		self.import_config.setStageOnlyInMemory()
	
	def saveIncrPendingValues(self):
		self.import_config.saveIncrPendingValues()

	def saveIncrMinValue(self):
		self.import_config.saveIncrMinValue()

	def convertStageStatisticsToJSON(self):
		self.import_config.convertStageStatisticsToJSON()
	
	def checkHiveDB(self, hiveDB):
		try:
			self.common_operations.checkHiveDB(hiveDB)
		except:
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getJDBCTableRowCount(self):
		try:
			self.import_config.getJDBCTableRowCount()
		except:
			logging.exception("Fatal error when reading source table row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getImportTableRowCount(self):
		try:
			importTableRowCount = self.common_operations.getHiveTableRowCount(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
			self.import_config.saveHiveTableRowCount(importTableRowCount)
		except:
			logging.exception("Fatal error when reading Hive table row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getTargetTableRowCount(self):
		try:
			whereStatement = self.import_config.getIncrWhereStatement(ignoreIfOnlyIncrMax=True)
			targetTableRowCount = self.common_operations.getHiveTableRowCount(self.import_config.Hive_DB, self.import_config.Hive_Table, whereStatement=whereStatement)
			self.import_config.saveHiveTableRowCount(targetTableRowCount)
		except:
			logging.exception("Fatal error when reading Hive table row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def clearTableRowCount(self):
		try:
			self.import_config.clearTableRowCount()
		except:
			logging.exception("Fatal error when clearing row counts from previous imports")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def validateRowCount(self):
		try:
			validateResult = self.import_config.validateRowCount() 
		except:
			logging.exception("Fatal error when validating imported rows")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if validateResult == False:
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def validateSqoopRowCount(self):
		try:
			validateResult = self.import_config.validateRowCount(validateSqoop=True) 
		except:
			logging.exception("Fatal error when validating imported rows by sqoop")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if validateResult == False:
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def validateIncrRowCount(self):
		try:
			validateResult = self.import_config.validateRowCount(validateSqoop=True, incremental=True) 
		except:
			logging.exception("Fatal error when validating imported incremental rows")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if validateResult == False:
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getSourceTableSchema(self):
		try:
			self.import_config.getJDBCTableDefinition()
			self.import_config.updateLastUpdateFromSource()
			self.import_config.removeFKforTable()
			self.import_config.saveColumnData()
			self.import_config.setPrimaryKeyColumn()
			self.import_config.saveKeyData()
			self.import_config.saveGeneratedData()
			self.import_config.generateSqoopBoundaryQuery()
		except:
			logging.exception("Fatal error when reading and/or processing source table schema")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def runSqoop(self, PKOnlyImport):
		logging.debug("Executing import_operations.runSqoop()")

		self.sqoopStartTimestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
		self.sqoopStartUTS = int(time.time())

		# Fetch the number of mappers that should be used
		self.import_config.calculateJobMappers()

		# As sqoop dont allow us to use the --delete-target-dir when doing incremental loads, we need to remove
		# the HDFS dir first for those imports. Reason for not doing this here for full imports is because the
		# --delete-target-dir is ore efficient and take less resources
#		if self.import_config.import_is_incremental == True and PKOnlyImport == False:
#			hdfsCommandList = ['hdfs', 'dfs', '-rm', '-r', '-skipTrash', self.import_config.sqoop_hdfs_location]
#			hdfsProc = subprocess.Popen(hdfsCommandList , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#			stdOut, stdErr = hdfsProc.communicate()
#			stdOut = stdOut.decode('utf-8').rstrip()
#			stdErr = stdErr.decode('utf-8').rstrip()
#
#			refStdOutText = "Deleted %s"%(self.import_config.sqoop_hdfs_location)
#			refStdErrText = "No such file or directory"
#
#			if refStdOutText not in stdOut and refStdErrText not in stdErr:
#				logging.error("There was an error deleting the target HDFS directory (%s)"%(self.import_config.sqoop_hdfs_location))
#				logging.error("Please check the status of that directory and try again")
#				logging.debug("StdOut: %s"%(stdOut))
#				logging.debug("StdErr: %s"%(stdErr))
#				self.import_config.remove_temporary_files()
#				sys.exit(1)
##		if self.import_config.import_is_incremental == True and PKOnlyImport == False:
##			whereStatement = self.import_config.getIncrWhereStatement(ignoreIfOnlyIncrMax=True, whereForSourceTable=True)

		# Sets the correct sqoop table and schema that will be used if a custom SQL is not used
		sqoopQuery = ""
		sqoopSourceSchema = "" 
		sqoopSourceTable = ""
		sqoopDirectOption = ""
		if self.import_config.common_config.db_mssql == True: 
			sqoopSourceSchema = "--  --schema  %s"%(self.import_config.source_schema)
			sqoopSourceTable = self.import_config.source_table
			sqoopDirectOption = "--direct  "
		if self.import_config.common_config.db_oracle == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema.upper(), self.import_config.source_table.upper())
			sqoopDirectOption = "--direct  "
		if self.import_config.common_config.db_postgresql == True: 
			sqoopSourceSchema = "--  --schema  %s"%(self.import_config.source_schema)
			sqoopSourceTable = self.import_config.source_table
		if self.import_config.common_config.db_progress == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table)
			sqoopDirectOption = "--direct  "
		if self.import_config.common_config.db_mysql == True: 
			sqoopSourceTable = self.import_config.source_table
		if self.import_config.common_config.db_db2udb == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table.upper())
			sqoopDirectOption = "--direct  "
		if self.import_config.common_config.db_db2as400 == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table.upper())
			sqoopDirectOption = "--direct  "

		if self.import_config.sqoop_use_generated_sql == True:
			sqoopQuery = self.import_config.sqlGeneratedSqoopQuery

		# Handle mappers, split-by with custom query
		if sqoopQuery != "":
			if "split-by" not in self.import_config.sqoop_options.lower():
				self.import_config.generateSqoopSplitBy()
#				if self.import_config.sqoop_options != "": self.import_config.sqoop_options += " " 
#				self.import_config.sqoop_options += "--split-by %s"%(self.import_config.generateSqoopSplitBy())

			self.import_config.generateSqoopBoundaryQuery()

		# Handle the situation where the source dont have a PK and there is no split-by (force mappers to 1)
		if self.import_config.generatedPKcolumns == None and "split-by" not in self.import_config.sqoop_options.lower():
			logging.warning("There is no PrimaryKey in source system and no--split-by in the sqoop_options columns. This will force the import to use only 1 mapper")
			self.import_config.sqlSessions = 1	
	

		# From here and forward we are building the sqoop command with all options
		# You must specify two {space} between each argument
		sqoopCommand = ""
		sqoopCommand += "sqoop  import  -D  mapreduce.job.user.classpath.first=true  "
		sqoopCommand += "-D  mapreduce.job.queuename=%s  "%(configuration.get("Sqoop", "yarnqueue"))
		sqoopCommand += "-D  oraoop.disabled=true  " 
		sqoopCommand += "-D  org.apache.sqoop.splitter.allow_text_splitter=%s  "%(self.import_config.sqoop_allow_text_splitter)

		if "split-by" not in self.import_config.sqoop_options.lower():
			sqoopCommand += "--autoreset-to-one-mapper  "

		# Progress and DB2 AS400 imports needs to know the class name for the JDBC driver. 
		if self.import_config.common_config.db_progress == True or self.import_config.common_config.db_db2as400 == True:
			sqoopCommand += "--driver  %s  "%(self.import_config.common_config.jdbc_driver)

		sqoopCommand += "--class-name  dbimport  " 

#		if self.import_config.sqoop_import_type == "hive":
#			if PKOnlyImport == True:
#				sqoopCommand += "--hcatalog-database  %s  "%(self.import_config.Hive_Import_PKonly_DB)
#				sqoopCommand += "--hcatalog-table  %s  "%(self.import_config.Hive_Import_PKonly_Table)
#			else:
#				sqoopCommand += "--hcatalog-database  %s  "%(self.import_config.Hive_Import_DB)
#				sqoopCommand += "--hcatalog-table  %s  "%(self.import_config.Hive_Import_Table)
#			sqoopCommand += "--hive-drop-import-delims  "

#		if self.import_config.sqoop_import_type == "hdfs":

		sqoopCommand += "--as-parquetfile  " 
		sqoopCommand += "--compress  "
		sqoopCommand += "--compression-codec  snappy  "

		if PKOnlyImport == True:
			sqoopCommand += "--target-dir  %s  "%(self.import_config.sqoop_hdfs_location_pkonly)
		else:
			sqoopCommand += "--target-dir  %s  "%(self.import_config.sqoop_hdfs_location)

		sqoopCommand += "--outdir  %s  "%(self.import_config.common_config.tempdir)
		sqoopCommand += "--connect  \"%s\"  "%(self.import_config.common_config.jdbc_url)
		sqoopCommand += "--username  %s  "%(self.import_config.common_config.jdbc_username)
		sqoopCommand += "--password-file  file://%s  "%(self.import_config.common_config.jdbc_password_file)
		sqoopCommand += sqoopDirectOption

		sqoopCommand += "--num-mappers  %s  "%(self.import_config.sqlSessions)

		# If we dont have a SQL query to use for sqoop, then we need to specify the table instead
		if sqoopQuery == "":
			sqoopCommand += "--table  %s  "%(sqoopSourceTable)

#		if self.import_config.import_is_incremental == True and PKOnlyImport == False:	# Cant do incr for PK only imports. Needs to be full
#			self.sqoopIncrNoNewRows = False
#
#			sqoopCommand += "--incremental  %s  "%(self.import_config.sqoop_incr_mode)
#			sqoopCommand += "--check-column  %s  "%(self.import_config.sqoop_incr_column)
#			if self.import_config.sqoop_incr_lastvalue != None: 
#				sqoopCommand += "--last-value  %s  "%(self.import_config.sqoop_incr_lastvalue)
#		else:
#			sqoopCommand += "--delete-target-dir  " 
		sqoopCommand += "--delete-target-dir  " 

		if self.import_config.generatedSqoopOptions != "" and self.import_config.generatedSqoopOptions != None:
			sqoopCommand += "%s  "%(self.import_config.generatedSqoopOptions)
		if self.import_config.sqoop_options != "" and self.import_config.sqoop_options != None:
			sqoopCommand += "%s  "%(self.import_config.sqoop_options.replace(" ", "  "))

		if self.import_config.sqoopBoundaryQuery != "" and self.import_config.sqlSessions > 1:
			sqoopCommand += "--boundary-query  %s  "%(self.import_config.sqoopBoundaryQuery)

		if sqoopQuery == "":
			if self.import_config.import_is_incremental == True and PKOnlyImport == False:
				sqoopCommand += "--where  \"%s\"  "%(self.import_config.getIncrWhereStatement(whereForSqoop=True).replace('"', '\''))
			else:
				if self.import_config.sqoop_sql_where_addition != None:
					sqoopCommand += "--where  \"%s\"  "%(self.import_config.sqoop_sql_where_addition.replace('"', '\''))
			sqoopCommand += "%s  "%(sqoopSourceSchema)
		else:
# TODO: Handle the same stuff here
			if self.import_config.sqoop_sql_where_addition != None:
				sqoopCommand += "--query  \"%s where $CONDITION and %s\"  "%(sqoopQuery, self.import_config.sqoop_sql_where_addition.replace('"', '\''))
			else:
				sqoopCommand += "--query  \"%s where $CONDITIONS\"  "%(sqoopQuery)

		print(sqoopQuery)

		logging.info("Starting sqoop with the following command: %s"%(sqoopCommand.strip()))
		sqoopCommandList = sqoopCommand.strip().split("  ")
#		sqoopCommandList.append("--query")
#		sqoopCommandList.append("\"%s where $CONDITIONS\""%(sqoopQuery))
		sqoopOutput = ""

		print(" _____________________ ")
		print("|                     |")
		print("| Sqoop Import starts |")
		print("|_____________________|")
		print("")

		# Start Sqoop
		sh_session = subprocess.Popen(sqoopCommandList, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

		# Print Stdout and stderr while sqoop is running
		while sh_session.poll() == None:
			row = sh_session.stdout.readline().decode('utf-8').rstrip()
			if row != "": 
				print(row)
				sys.stdout.flush()
				sqoopOutput += row + "\n"
				self.parseSqoopOutput(row)

		# Print what is left in output after sqoop finished
		for row in sh_session.stdout.readlines():
			row = row.decode('utf-8').rstrip()
			if row != "": 
				print(row)
				sys.stdout.flush()
				sqoopOutput += row + "\n"
				self.parseSqoopOutput(row)

		print(" ________________________ ")
		print("|                        |")
		print("| Sqoop Import completed |")
		print("|________________________|")
		print("")

		# At this stage, the entire sqoop job is run and we fetched all data from the output. Lets parse and store it
		if self.sqoopSize == None: self.sqoopSize = 0
		if self.sqoopRows == None: self.sqoopRows = 0
		
		# Check for errors in output
		sqoopWarning = False
		if " ERROR " in sqoopOutput:
			for row in sqoopOutput.split("\n"):
				if "Arithmetic overflow error converting expression to data type int" in row:
					# This is not really an error, but we cant verify the table.
					logging.warning("Unable to verify the import due to errors from MsSQL server. Will continue regardless of this")
					sqoopWarning = True
		
				if "Not authorized to access topics: [" in row:
					# This is not really an error, but we cant verify the table.
					logging.warning("Problem sending data to Atlas")
					sqoopWarning = True
		
				if "ERROR hook.AtlasHook" in row:
					# This is not really an error, but we cant send to Atlas
					logging.warning("Problem sending data to Atlas")
					sqoopWarning = True
		
				if "clients.NetworkClient: [Producer clientId" in row:
					# Unknown Kafka error
					sqoopWarning = True

				# The following two tests is just to ensure that we dont mark the import as failed just because there is a column called error
				if " ERROR " in row and "Precision" in row and "Scale" in row:
					sqoopWarning = True

				if " ERROR " in row and "The HCatalog field" in row:
					sqoopWarning = True

				if sqoopWarning == False:
					# We detected the string ERROR in the output but we havent found a vaild reason for it in the IF statements above.
					# We need to mark the sqoop command as error and exit the program
					logging.error("Unknown error in sqoop import. Please check the output for errors and try again")
					self.remove_temporary_files()
					sys.exit(1)

		# No errors detected in the output and we are ready to store the result
		logging.info("Sqoop executed successfully")			
		
		if self.sqoopIncrNoNewRows == True:
			# If there is no new rows, we cant get the MaxValue from the output. 
			# So we need to set it to what we used to launch sqoop
			self.sqoopIncrMaxValuePending = self.import_config.sqoop_incr_lastvalue 
			self.sqoopSize = 0
			self.sqoopRows = 0
			logging.warning("There are no new rows in the source table.")

		if PKOnlyImport == False:
			try:
#				self.import_config.saveSqoopStatistics(self.sqoopStartUTS, self.sqoopSize, self.sqoopRows, self.sqoopIncrMaxValuePending)
				self.import_config.saveSqoopStatistics(self.sqoopStartUTS, self.sqoopSize, self.sqoopRows)
			except:
				logging.exception("Fatal error when saving sqoop statistics")
				self.import_config.remove_temporary_files()
				sys.exit(1)

		logging.debug("Executing import_operations.runSqoop() - Finished")


	def parseSqoopOutput(self, row ):
		# 19/03/28 05:15:44 HDFS: Number of bytes written=17
		if "HDFS: Number of bytes written" in row:
			self.sqoopSize = int(row.split("=")[1])
		
		# 19/03/28 05:15:44 INFO mapreduce.ImportJobBase: Retrieved 2 records.
		if "mapreduce.ImportJobBase: Retrieved" in row:
			self.sqoopRows = int(row.split(" ")[5])

		# 19/04/23 17:19:58 INFO tool.ImportTool:   --last-value 34
		if "tool.ImportTool:   --last-value" in row:
#			self.sqoopIncrMaxValuePending = int(row.split("-")[3].split(" ")[1])
			self.sqoopIncrMaxValuePending = row.split("last-value ")[1].strip()

		# 19/04/24 07:13:00 INFO tool.ImportTool: No new rows detected since last import.
		if "No new rows detected since last import" in row:
			self.sqoopIncrNoNewRows = True

	def connectToHive(self,):
		logging.debug("Executing import_operations.connectToHive()")

		try:
			self.common_operations.connectToHive()
		except Exception as ex:
			logging.error(ex)
			self.import_config.remove_temporary_files()
			sys.exit(1)

		logging.debug("Executing import_operations.connectToHive() - Finished")

	def createExternalImportTable(self,):
		logging.debug("Executing import_operations.createExternalTable()")

		if self.common_operations.checkHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == True:
			# Table exist, so we need to make sure that it's a managed table and not an external table
			if self.common_operations.isHiveTableExternalParquetFormat(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == False:
				logging.info("Dropping staging table as it's not an external table based on parquet")
				self.common_operations.dropHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
				self.common_operations.reconnectHiveMetaStore()

		if self.common_operations.checkHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == False:
			# Import table does not exist. We just create it in that case
			query  = "create external table `%s`.`%s` ("%(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
			columnsDF = self.import_config.getColumnsFromConfigDatabase() 
			columnsDF = self.updateColumnsForImportTable(columnsDF)

			firstLoop = True
			for index, row in columnsDF.iterrows():
				if firstLoop == False: query += ", "

				# As Parquet import format writes timestamps in the wrong format, we need to force them to string
#				if row['type'] == "timestamp":
#					query += "`%s` string"%(row['name'])
#				else:
				query += "`%s` %s"%(row['name'], row['type'])

				if row['comment'] != None:
					query += " COMMENT \"%s\""%(row['comment'])
				firstLoop = False
			query += ") "

			tableComment = self.import_config.getHiveTableComment()
			if tableComment != None:
				query += "COMMENT \"%s\" "%(tableComment)

			query += "stored as parquet "
#			query += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' "
#			query += "LINES TERMINATED BY '\002' "
			query += "LOCATION '%s%s/' "%(self.import_config.common_config.hdfs_address, self.import_config.sqoop_hdfs_location)
			query += "TBLPROPERTIES('parquet.compress' = 'SNAPPY') "
#			query += "TBLPROPERTIES('serialization.null.format' = '\\\\N') "
#			query += "TBLPROPERTIES('serialization.null.format' = '\003') "

			self.common_operations.executeHiveQuery(query)
			self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createExternalTable() - Finished")

	def createTargetTable(self):
		logging.debug("Executing import_operations.createTargetTable()")
		if self.common_operations.checkHiveTable(self.Hive_DB, self.Hive_Table) == True:
			# Table exist, so we need to make sure that it's a managed table and not an external table
			if self.common_operations.isHiveTableExternal(self.Hive_DB, self.Hive_Table) == True:
				self.common_operations.dropHiveTable(self.Hive_DB, self.Hive_Table)

		# We need to check again as the table might just been droped because it was an external table to begin with
		if self.common_operations.checkHiveTable(self.Hive_DB, self.Hive_Table) == False:
			# Target table does not exist. We just create it in that case
			logging.info("Creating target table %s.%s in Hive"%(self.Hive_DB, self.Hive_Table))

			query  = "create table `%s`.`%s` ("%(self.import_config.Hive_DB, self.import_config.Hive_Table)
			columnsDF = self.import_config.getColumnsFromConfigDatabase() 

			firstLoop = True
			for index, row in columnsDF.iterrows():
				if firstLoop == False: query += ", "
				query += "`%s` %s"%(row['name'], row['type'])
				if row['comment'] != None:
					query += " COMMENT \"%s\""%(row['comment'])
				firstLoop = False

			if self.import_config.datalake_source != None:
				query += ", datalake_source varchar(256)"

			if self.import_config.create_datalake_import_column == True:
				query += ", datalake_import timestamp COMMENT \"Import time from source database\""

			query += ") "

			tableComment = self.import_config.getHiveTableComment()
			if tableComment != None:
				query += "COMMENT \"%s\" "%(tableComment)

			if self.import_config.create_table_with_acid == False:
				query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB') "
			else:
				query += "CLUSTERED BY ("
				firstColumn = True
				for column in self.import_config.getPKcolumns().split(","):
					if firstColumn == False:
						query += ", " 
					query += "`" + column + "`" 
					firstColumn = False
				query += ") into 4 buckets "
				query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true') "

			self.common_operations.executeHiveQuery(query)

			self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createTargetTable() - Finished")
		
	def updateTargetTable(self):
		self.updateHiveTable(self.Hive_DB, self.Hive_Table)

	def updateExternalImportTable(self):
		self.updateHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)

	def updateColumnsForImportTable(self, columnsDF):
		""" Parquet import format from sqoop cant handle all datatypes correctly. So for the column definition, we need to change some. We also replace <SPACE> with underscore in the column name """
		columnsDF["type"].replace(["date"], "string", inplace=True)
		columnsDF["type"].replace(["timestamp"], "string", inplace=True)
		columnsDF["type"].replace(["decimal(.*)"], "string", regex=True, inplace=True)

		# If you change any of the name replace rows, you also need to change the same data in function self.copyHiveTable() and import_definitions.saveColumnData()
		columnsDF["name"].replace([" "], "_", regex=True, inplace=True)
		columnsDF["name"].replace(["%"], "pct", regex=True, inplace=True)
		columnsDF["name"].replace(["\("], "_", regex=True, inplace=True)
		columnsDF["name"].replace(["\)"], "_", regex=True, inplace=True)
		return columnsDF

	def updateHiveTable(self, hiveDB, hiveTable):
		""" Update the target table based on the column information in the configuration database """
		# TODO: If there are less columns in the source table together with a rename of a column, then it wont work. Needs to be handled
		logging.debug("Executing import_operations.updateTargetTable()")
		logging.info("Updating Hive table columns based on source system schema")
		columnsConfig = self.import_config.getColumnsFromConfigDatabase() 
		columnsHive   = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 

		# If we are working on the import table, we need to change some column types to handle Parquet files
		if hiveDB == self.import_config.Hive_Import_DB and hiveTable == self.import_config.Hive_Import_Table:
			columnsConfig = self.updateColumnsForImportTable(columnsConfig)

		# Check for missing columns
		columnsConfigOnlyName = columnsConfig.filter(['name'])
		columnsHiveOnlyName = columnsHive.filter(['name'])
		columnsMergeOnlyName = pd.merge(columnsConfigOnlyName, columnsHiveOnlyName, on=None, how='outer', indicator='Exist')

		columnsConfigCount = len(columnsConfigOnlyName)
		columnsHiveCount   = len(columnsHiveOnlyName)
		columnsMergeLeftOnlyCount  = len(columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'])
		columnsMergeRightOnlyCount = len(columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'right_only'])

		logging.debug("columnsConfigOnlyName")
		logging.debug(columnsConfigOnlyName)
		logging.debug("================================================================")
		logging.debug("columnsHiveOnlyName")
		logging.debug(columnsHiveOnlyName)
		logging.debug("================================================================")
		logging.debug("columnsMergeOnlyName")
		logging.debug(columnsMergeOnlyName)
		logging.debug("")

		if columnsConfigCount == columnsHiveCount and columnsMergeLeftOnlyCount > 0:
			# The number of columns in config and Hive is the same, but there is a difference in name. This is most likely because of a rename of one or more of the columns
			# To handle this, we try a rename. This might fail if the column types are also changed to an incompatable type
			# The logic here is to 
			# 1. get all columns from mergeDF that exists in Left_Only
			# 2. Get the position in configDF with that column name
			# 3. Get the column in the same position from HiveDF
			# 4. Check if that column name exists in the mergeDF with Right_Only. If it does, the column was just renamed
			for index, row in columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iterrows():
				rowInConfig = columnsConfig.loc[columnsConfig['name'] == row['name']].iloc[0]
				indexInConfig = columnsConfig.loc[columnsConfig['name'] == row['name']].index.item()
				rowInHive = columnsHive.iloc[indexInConfig]
				
				if len(columnsMergeOnlyName.loc[(columnsMergeOnlyName['Exist'] == 'right_only') & (columnsMergeOnlyName['name'] == rowInHive["name"])]) > 0: 
					# This is executed if the column is renamed and exists in the same position
					logging.debug("Name in config:  %s"%(rowInConfig["name"]))
					logging.debug("Type in config:  %s"%(rowInConfig["type"]))
					logging.debug("Index in config: %s"%(indexInConfig))
					logging.debug("--------------------")
					logging.debug("Name in Hive: %s"%(rowInHive["name"]))
					logging.debug("Type in Hive: %s"%(rowInHive["type"]))
					logging.debug("======================================")
					logging.debug("")
	
					query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, rowInHive['name'], rowInConfig['name'], rowInConfig['type'])
					self.common_operations.executeHiveQuery(query)

					self.import_config.logColumnRename(rowInConfig['name'], rowInHive["name"], hiveDB=hiveDB, hiveTable=hiveTable)
				
					if rowInConfig["type"] != rowInHive["type"]:
						self.import_config.logColumnTypeChange(rowInConfig['name'], rowInConfig['type'], previous_columnType=rowInHive["type"], hiveDB=hiveDB, hiveTable=hiveTable) 
				else:
					if columnsMergeLeftOnlyCount == 1 and columnsMergeRightOnlyCount == 1:
						# So the columns are not in the same position, but it's only one column that changed. In that case, we just rename that one column
						rowInMergeLeft  = columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iloc[0]
						rowInMergeRight = columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'right_only'].iloc[0]
						rowInConfig = columnsConfig.loc[columnsConfig['name'] == rowInMergeLeft['name']].iloc[0]
						rowInHive = columnsHive.loc[columnsHive['name'] == rowInMergeRight['name']].iloc[0]
						print(rowInConfig["name"])
						print(rowInConfig["type"])
						print("--------------------")
						print(rowInHive["name"])
						print(rowInHive["type"])

						query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, rowInHive['name'], rowInConfig['name'], rowInHive['type'])
						self.common_operations.executeHiveQuery(query)

						self.import_config.logColumnRename(rowInConfig['name'], rowInHive["name"], hiveDB=hiveDB, hiveTable=hiveTable)

			self.common_operations.reconnectHiveMetaStore()
			columnsHive   = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 
			columnsHiveOnlyName = columnsHive.filter(['name'])
			columnsMergeOnlyName = pd.merge(columnsConfigOnlyName, columnsHiveOnlyName, on=None, how='outer', indicator='Exist')

		for index, row in columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iterrows():
			# This will iterate over columns that only exists in the config and not in Hive. We add these to Hive
			fullRow = columnsConfig.loc[columnsConfig['name'] == row['name']].iloc[0]
			query = "alter table `%s`.`%s` add columns (`%s` %s"%(hiveDB, hiveTable, fullRow['name'], fullRow['type'])
			if fullRow['comment'] != None:
				query += " COMMENT \"%s\""%(fullRow['comment'])
			query += ")"

			self.common_operations.executeHiveQuery(query)

			self.import_config.logColumnAdd(fullRow['name'], columnType=fullRow['type'], hiveDB=hiveDB, hiveTable=hiveTable) 

		# Check for changed column types
		self.common_operations.reconnectHiveMetaStore()
		columnsHive = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 

		columnsConfigOnlyNameType = columnsConfig.filter(['name', 'type']).sort_values(by=['name'], ascending=True)
		columnsHiveOnlyNameType = columnsHive.filter(['name', 'type']).sort_values(by=['name'], ascending=True)
		columnsMergeOnlyNameType = pd.merge(columnsConfigOnlyNameType, columnsHiveOnlyNameType, on=None, how='outer', indicator='Exist')

		logging.debug("columnsConfigOnlyNameType")
		logging.debug(columnsConfigOnlyNameType)
		logging.debug("================================================================")
		logging.debug("columnsHiveOnlyNameType")
		logging.debug(columnsHiveOnlyNameType)
		logging.debug("================================================================")
		logging.debug("columnsMergeOnlyNameType")
		logging.debug(columnsMergeOnlyNameType)
		logging.debug("")

		for index, row in columnsMergeOnlyNameType.loc[columnsMergeOnlyNameType['Exist'] == 'left_only'].iterrows():
			# This will iterate over columns that had the type changed from the source
			query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, row['name'], row['name'], row['type'])
			self.common_operations.executeHiveQuery(query)

			# Get the previous column type from the Pandas DF with right_only in Exist column
			previous_columnType = (columnsMergeOnlyNameType.loc[
				(columnsMergeOnlyNameType['name'] == row['name']) &
				(columnsMergeOnlyNameType['Exist'] == 'right_only')]
				).reset_index().at[0, 'type']

			self.import_config.logColumnTypeChange(row['name'], columnType=row['type'], previous_columnType=previous_columnType, hiveDB=hiveDB, hiveTable=hiveTable) 

		# Check for change column comments
		self.common_operations.reconnectHiveMetaStore()
		columnsHive = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 
		columnsHive['comment'].replace('', None, inplace = True)		# Replace blank column comments with None as it would otherwise trigger an alter table on every run
		columnsMerge = pd.merge(columnsConfig, columnsHive, on=None, how='outer', indicator='Exist')
		for index, row in columnsMerge.loc[columnsMerge['Exist'] == 'left_only'].iterrows():
			if row['comment'] == None: row['comment'] = ""
			query = "alter table `%s`.`%s` change column `%s` `%s` %s comment \"%s\""%(hiveDB, hiveTable, row['name'], row['name'], row['type'], row['comment'])

			self.common_operations.executeHiveQuery(query)

		logging.debug("Executing import_operations.updateTargetTable() - Finished")

	def updatePKonTargetTable(self,):
		""" Update the PrimaryKey definition on the Target Hive Table """
		logging.debug("Executing import_operations.updatePKonTargetTable()")
		self.updatePKonTable(self.import_config.Hive_DB, self.import_config.Hive_Table)

		logging.debug("Executing import_operations.updatePKonTargetTable() - Finished")

	def updatePKonTable(self, hiveDB, hiveTable):
		""" Update the PrimaryKey definition on a Hive Table """
		logging.debug("Executing import_operations.updatePKonTable()")
		if self.import_config.getPKcolumns() == None:
			logging.info("No Primary Key informaton exists for this table.")
		else:
			logging.info("Creating Primary Key on table.")
			PKonHiveTable = self.common_operations.getPKfromTable(hiveDB, hiveTable)
			PKinConfigDB = self.import_config.getPKcolumns()
			self.common_operations.reconnectHiveMetaStore()

			if PKonHiveTable != PKinConfigDB:
				# The PK information is not correct. We need to figure out what the problem is and alter the table to fix it
				logging.info("PrimaryKey definition is not correct. If it exists, we will drop it and recreate")
				logging.info("PrimaryKey on Hive table:     %s"%(PKonHiveTable))
				logging.info("PrimaryKey from source table: %s"%(PKinConfigDB))

				currentPKname = self.common_operations.getPKname(hiveDB, hiveTable)
				if currentPKname != None:
					query  = "alter table `%s`.`%s` "%(self.import_config.Hive_DB, self.import_config.Hive_Table)
					query += "drop constraint %s"%(currentPKname)
					self.common_operations.executeHiveQuery(query)

				# There are now no PK on the table. So it's safe to create one
				PKname = "%s__%s__PK"%(self.import_config.Hive_DB, self.import_config.Hive_Table)
				query  = "alter table `%s`.`%s` "%(self.import_config.Hive_DB, self.import_config.Hive_Table)
				query += "add constraint %s primary key ("%(PKname)

				firstColumn = True
				for column in self.import_config.getPKcolumns().split(","):
					if firstColumn == False:
						query += ", " 
					query += "`" + column + "`" 
					firstColumn = False

				query += ") DISABLE NOVALIDATE"
				self.common_operations.executeHiveQuery(query)

				# self.common_operations.executeHiveQuery(query)


			else:
				logging.info("PrimaryKey definition is correct. No change required.")


		logging.debug("Executing import_operations.updatePKonTable() - Finished")

	def updateFKonTargetTable(self,):
		""" Update the ForeignKeys definition on the Target Hive Table """
		logging.debug("Executing import_operations.updatePKonTargetTable()")
		self.updateFKonTable(self.import_config.Hive_DB, self.import_config.Hive_Table)

		logging.debug("Executing import_operations.updatePKonTargetTable() - Finished")

	def updateFKonTable(self, hiveDB, hiveTable):
		""" Update the ForeignKeys definition on a Hive Table """
		logging.debug("Executing import_operations.updateFKonTable()")

		foreignKeysFromConfig = self.import_config.getForeignKeysFromConfig()
		foreignKeysFromHive   = self.common_operations.getForeignKeysFromHive(hiveDB, hiveTable)

		if foreignKeysFromConfig.empty == True and foreignKeysFromHive.empty == True:
			logging.info("No Foreign Keys informaton exists for this table")
			return

		if foreignKeysFromConfig.equals(foreignKeysFromHive) == True:
			logging.info("ForeignKey definitions is correct. No change required.")
		else:
			logging.info("ForeignKey definitions is not correct. Will check what exists and create/drop required foreign keys.")

			foreignKeysDiff = pd.merge(foreignKeysFromConfig, foreignKeysFromHive, on=None, how='outer', indicator='Exist')

			for index, row in foreignKeysDiff.loc[foreignKeysDiff['Exist'] == 'right_only'].iterrows():
				# This will iterate over ForeignKeys that only exists in Hive
				# If it only exists in Hive, we delete it!

				logging.info("Dropping FK in Hive as it doesnt match the FK's in DBImport config database")
				query = "alter table `%s`.`%s` drop constraint %s"%(hiveDB, hiveTable, row['fk_name'])
				self.common_operations.executeHiveQuery(query)
					
							
			for index, row in foreignKeysDiff.loc[foreignKeysDiff['Exist'] == 'left_only'].iterrows():
				# This will iterate over ForeignKeys that only exists in the DBImport configuration database
				# If it doesnt exist in Hive, we create it

				if self.common_operations.checkHiveTable(row['ref_hive_db'], row['ref_hive_table'], ) == True:
					query = "alter table `%s`.`%s` add constraint %s foreign key ("%(hiveDB, hiveTable, row['fk_name'])
					firstLoop = True
					for column in row['source_column_name'].split(','):
						if firstLoop == False: query += ", "
						query += "`%s`"%(column)
						firstLoop = False
	
					query += ") REFERENCES `%s`.`%s` ("%(row['ref_hive_db'], row['ref_hive_table'])
					firstLoop = True
					for column in row['ref_column_name'].split(','):
						if firstLoop == False: query += ", "
						query += "`%s`"%(column)
						firstLoop = False
					query += ") DISABLE NOVALIDATE RELY"
	
					logging.info("Creating FK in Hive as it doesnt exist")
					self.common_operations.executeHiveQuery(query)

		logging.debug("Executing import_operations.updateFKonTable()  - Finished")

	def removeHiveLocks(self,):
		self.common_operations.removeHiveLocksByForce(self.Hive_DB, self.Hive_Table)

	def truncateTargetTable(self,):
		logging.info("Truncating Target table in Hive")
		self.common_operations.truncateHiveTable(self.Hive_DB, self.Hive_Table)

	def updateStatisticsOnTargetTable(self,):
		logging.info("Updating the Hive statistics on the target table")
		self.common_operations.updateHiveTableStatistics(self.Hive_DB, self.Hive_Table)

	def loadDataFromImportToTargetTable(self,):
		logging.info("Loading data from import table to target table")
		importTableExists = self.common_operations.checkHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
		targetTableExists = self.common_operations.checkHiveTable(self.Hive_DB, self.Hive_Table)

		if importTableExists == False:
			logging.error("The import table %s.%s does not exist"%(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table))
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if targetTableExists == False:
			logging.error("The target table %s.%s does not exist"%(self.Hive_DB, self.Hive_Table))
			self.import_config.remove_temporary_files()
			sys.exit(1)

		self.copyHiveTable(	self.import_config.Hive_Import_DB, 
							self.import_config.Hive_Import_Table, 
							self.Hive_DB, 
							self.Hive_Table)

	def copyHiveTable(self, sourceDB, sourceTable, targetDB, targetTable):
		""" Copy one Hive table into another for the columns that have the same name """
		logging.debug("Executing import_definitions.copyHiveTable()")

		# Logic here is to create a new column in both DF and call them sourceCol vs targetCol. These are the original names. Then we replace the values in targetColumn DF column col
		# with the name that the column should be called in the source system. This is needed to handle characters that is not supported in Parquet files, like SPACE
		sourceColumns = self.common_operations.getHiveColumns(sourceDB, sourceTable)
		sourceColumns['sourceCol'] = sourceColumns['col']

		targetColumns = self.common_operations.getHiveColumns(targetDB, targetTable)
		targetColumns['targetCol'] = targetColumns['col']
		# If you change any of the name replace operations, you also need to change the same data in function self.updateColumnsForImportTable() and import_definitions.saveColumnData()
		targetColumns['col'] = targetColumns['col'].str.replace(r' ', '_')
		targetColumns['col'] = targetColumns['col'].str.replace(r'\%', 'pct')
		targetColumns['col'] = targetColumns['col'].str.replace(r'\(', '_')
		targetColumns['col'] = targetColumns['col'].str.replace(r'\)', '_')

		columnMerge = pd.merge(sourceColumns, targetColumns, on=None, how='outer', indicator='Exist')
		logging.debug("\n%s"%(columnMerge))

#		print(columnMerge)
#		self.import_config.remove_temporary_files()
#		sys.exit(1)

		firstLoop = True
		columnDefinitionSource = ""
		columnDefinitionTarget = ""
		for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
			if firstLoop == False: columnDefinitionSource += ", "
			if firstLoop == False: columnDefinitionTarget += ", "
			columnDefinitionSource += "`%s`"%(row['sourceCol'])
			columnDefinitionTarget += "`%s`"%(row['targetCol'])
			firstLoop = False

		query = "insert into `%s`.`%s` ("%(targetDB, targetTable)
		query += columnDefinitionTarget
		if self.import_config.datalake_source != None:
			query += ", datalake_source"
		if self.import_config.create_datalake_import_column == True:
			query += ", datalake_import"

		query += ") select "
		query += columnDefinitionSource
		if self.import_config.datalake_source != None:
			query += ", '%s'"%(self.import_config.datalake_source)
		if self.import_config.create_datalake_import_column == True:
			query += ", '%s'"%(self.import_config.sqoop_last_execution_timestamp)

		query += " from `%s`.`%s` "%(sourceDB, sourceTable)
		if self.import_config.nomerge_ingestion_sql_addition != None:
			query += self.import_config.nomerge_ingestion_sql_addition

		self.common_operations.executeHiveQuery(query)
		logging.debug("Executing import_definitions.copyHiveTable() - Finished")

	def resetIncrMaxValue(self, hiveDB=None, hiveTable=None):
		""" Will read the Max value from the Hive table and save that into the incr_maxvalue column """
		logging.debug("Executing import_operations.resetIncrMaxValue()")

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		if self.import_config.import_is_incremental == True:
			self.sqoopIncrNoNewRows = False
			query = "select max(%s) from `%s`.`%s` "%(self.import_config.sqoop_incr_column, hiveDB, hiveTable)

			self.common_operations.connectToHive(forceSkipTest=True)
			result_df = self.common_operations.executeHiveQuery(query)
			maxValue = result_df.loc[0][0]

			if type(maxValue) == pd._libs.tslibs.timestamps.Timestamp:
		#		maxValue += pd.Timedelta(np.timedelta64(1, 'ms'))
				(dt, micro) = maxValue.strftime('%Y-%m-%d %H:%M:%S.%f').split(".")
				maxValue = "%s.%03d" % (dt, int(micro) / 1000)
			else:
				maxValue = int(maxValue)

			self.import_config.resetSqoopStatistics(maxValue)
			self.import_config.clearStage()
		else:
			logging.error("This is not an incremental import. Nothing to repair.")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		logging.debug("Executing import_operations.resetIncrMaxValue() - Finished")

	def repairAllIncrementalImports(self):
		logging.debug("Executing import_operations.repairAllIncrementalImports()")
		result_df = self.import_config.getAllActiveIncrImports()

		# Only used under debug so I dont clear any real imports that is going on
		tablesRepaired = False
		for index, row in result_df.loc[result_df['hive_db'] == 'user_boszkk'].iterrows():
#		for index, row in result_df.iterrows():
			tablesRepaired = True
			hiveDB = row[0]
			hiveTable = row[1]
			logging.info("Repairing table \u001b[33m%s.%s\u001b[0m"%(hiveDB, hiveTable))
			self.setHiveTable(hiveDB, hiveTable)
			self.resetIncrMaxValue(hiveDB, hiveTable)
			print("")

		if tablesRepaired == False:
			print("\n\u001b[32mNo incremental tables found that could be repaired\u001b[0m\n")

		logging.debug("Executing import_operations.repairAllIncrementalImports() - Finished")
