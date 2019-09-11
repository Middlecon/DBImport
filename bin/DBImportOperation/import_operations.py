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
from subprocess import Popen, PIPE
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from common.Singleton import Singleton
from common.Exceptions import *
from common import constants as constant
from DBImportConfig import import_config
from DBImportOperation import common_operations
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time


class operation(object, metaclass=Singleton):
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

		self.globalHiveConfigurationSet = False

#		try:
			# Initialize the two core classes. import_config will initialize common_config aswell
		self.import_config = import_config.config(Hive_DB, Hive_Table)
		self.common_operations = common_operations.operation(Hive_DB, Hive_Table)
#		except:
#			sys.exit(1)

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
		except invalidConfiguration as errMsg:
			logging.error(errMsg)
			self.import_config.remove_temporary_files()
			sys.exit(1)
		except:
			self.import_config.remove_temporary_files()
			raise
			sys.exit(1)

	def remove_temporary_files(self):
		self.import_config.remove_temporary_files()

	def checkTimeWindow(self):
		self.import_config.checkTimeWindow()
	
	def runStage(self, stage):
		self.import_config.setStage(stage)

		if self.import_config.common_config.getConfigValue(key = "import_stage_disable") == True:
			logging.error("Stage execution disabled from DBImport configuration")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		tempStage = self.import_config.getStage()
		if stage == tempStage:
			return True
		else:
			return False
	
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
	
	def saveStageStatistics(self):
		self.import_config.saveStageStatistics()
	
	def checkHiveDB(self, hiveDB):
		try:
			self.common_operations.checkHiveDB(hiveDB)
		except databaseNotFound as errMsg:
			logging.error(errMsg)
			self.import_config.remove_temporary_files()
			sys.exit(1)
		except:
			self.import_config.remove_temporary_files()
			raise

	def getJDBCTableRowCount(self):
		try:
			self.import_config.getJDBCTableRowCount()
		except:
			logging.exception("Fatal error when reading source table row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getImportViewRowCountAsSource(self, incr=False):
		whereStatement = None
		if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
			# Needed otherwise we will count rows that will be deleted and that will cause the validation to fail
			whereStatement = "datalake_flashback_operation != 'D' or datalake_flashback_operation is null"

		try:
			importViewRowCount = self.common_operations.getHiveTableRowCount(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table, whereStatement = whereStatement)
#			self.import_config.saveHiveTableRowCount(importTableRowCount)
			self.import_config.saveSourceTableRowCount(importViewRowCount, incr=incr)
		except:
			logging.exception("Fatal error when reading Hive view row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getImportTableRowCount(self):
		whereStatement = None
#		if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
#			# Needed otherwise we will count rows that will be deleted and that will cause the validation to fail
#			whereStatement = "datalake_flashback_operation != 'D' or datalake_flashback_operation is null"

		try:
			importTableRowCount = self.common_operations.getHiveTableRowCount(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table, whereStatement = whereStatement)
			self.import_config.saveHiveTableRowCount(importTableRowCount)
		except:
			logging.exception("Fatal error when reading Hive table row count")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def getTargetTableRowCount(self):
		try:
			whereStatement = self.import_config.getIncrWhereStatement(ignoreIfOnlyIncrMax=True)
			if whereStatement == None and self.import_config.import_with_merge == True and self.import_config.soft_delete_during_merge == True:
				whereStatement = "datalake_iud != 'D'"

			if whereStatement == None and self.import_config.importPhase == constant.IMPORT_PHASE_FULL and self.import_config.etlPhase == constant.ETL_PHASE_INSERT:
				whereStatement = "datalake_import == '%s'"%(self.import_config.sqoop_last_execution_timestamp)

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
#			validateResult = self.import_config.validateRowCount(validateSqoop=True, incremental=True) 
			validateResult = self.import_config.validateRowCount(validateSqoop=False, incremental=True) 
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
		except invalidConfiguration as errMsg:
			logging.error(errMsg)
			self.import_config.remove_temporary_files()
			sys.exit(1)
		except:
			logging.exception("Fatal error when reading and/or processing source table schema")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def discoverAndAddTablesFromSource(self, dbalias, hiveDB, schemaFilter=None, tableFilter=None, addSchemaToTable=False, addCustomText=None, addCounterToTable=False, counterStart=None):
		""" This is the main function to search for tables/view on source database and add them to import_tables """
		logging.debug("Executing import_operations.discoverAndAddTablesFromSource()")
		errorDuringAdd = False

		self.import_config.common_config.lookupConnectionAlias(connection_alias=dbalias)
		sourceDF = self.import_config.common_config.getJDBCtablesAndViews(schemaFilter=schemaFilter, tableFilter=tableFilter )

		if len(sourceDF) == 0:
			print("There are no tables in the source database that we dont already have in DBImport")
			self.import_config.remove_temporary_files()
			sys.exit(0)

		importDF = self.import_config.getImportTables(hiveDB = hiveDB)
		importDF = importDF.loc[importDF['dbalias'] == dbalias].filter(['source_schema', 'source_table'])
		importDF.rename(columns={'source_schema':'schema', 'source_table':'table'}, inplace=True)

		mergeDF = pd.merge(sourceDF, importDF, on=None, how='outer', indicator='Exist')
		mergeDF['hiveTable'] = mergeDF['table']
		discoveredTables = len(mergeDF.loc[mergeDF['Exist'] == 'left_only'])

		if addCounterToTable == True or addSchemaToTable == True or addCustomText != None:
			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					mergeDF.loc[index, 'hiveTable'] = "_%s"%(mergeDF.loc[index, 'hiveTable'].strip())

		if addCounterToTable == True:
			if counterStart == None:
				counterStart = "1"
			numberLength=len(counterStart)
			try:
				startValue = int(counterStart)
			except ValueError:
				logging.error("The value specified for --counterStart must be a number")
				self.export_config.remove_temporary_files()
				sys.exit(1)

			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					zeroToAdd = ""
					while len(zeroToAdd) < (numberLength - len(str(startValue))):
						zeroToAdd += "0"

					mergeDF.loc[index, 'hiveTable'] = "%s%s%s"%(zeroToAdd, startValue, mergeDF.loc[index, 'hiveTable'])
					startValue += 1

		if addSchemaToTable == True:
			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					mergeDF.loc[index, 'hiveTable'] = "%s%s"%(mergeDF.loc[index, 'schema'].lower().strip(), mergeDF.loc[index, 'hiveTable'])

		if addCustomText != None:
			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					mergeDF.loc[index, 'hiveTable'] = "%s%s"%(addCustomText.lower().strip(), mergeDF.loc[index, 'hiveTable'])


		if discoveredTables == 0:
			print("There are no tables in the source database that we dont already have in DBImport")
			self.import_config.remove_temporary_files()
			sys.exit(0)

		# At this stage, we have discovered tables in the source system that we dont know about in DBImport
		print("The following tables and/or views have been discovered in the source database and not found in DBImport")
		print("")
		print("%-20s%-40s%-30s%-20s%s"%("Hive DB", "Hive Table", "Connection Alias", "Schema", "Table/View"))
		print("=============================================================================================================================")

		for index, row in mergeDF.loc[mergeDF['Exist'] == 'left_only'].iterrows():
			print("%-20s%-40s%-30s%-20s%s"%(hiveDB, row['hiveTable'].lower(), dbalias, row['schema'], row['table']))

		answer = input("Do you want to add these imports to DBImport? (y/N): ")
		if answer == "y":
			print("")
			for index, row in mergeDF.loc[mergeDF['Exist'] == 'left_only'].iterrows():
				addResult = self.import_config.addImportTable(
					hiveDB=hiveDB, 
					hiveTable=row['hiveTable'].lower(),
					dbalias=dbalias,
					schema=row['schema'].strip(),
					table=row['table'].strip())

				if addResult == False:
					errorDuringAdd = True

			if errorDuringAdd == False:
				print("All tables saved successfully in DBImport")
			else:
				print("")
				print("Not all tables was saved to DBImport. Please check log and output")
		else:
			print("")
			print("Aborting")


		logging.debug("Executing import_operations.discoverAndAddTablesFromSource() - Finished")

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
		sqoopSourceSchema = [] 
		sqoopSourceTable = ""
		sqoopDirectOption = ""
		if self.import_config.common_config.db_mssql == True: 
			sqoopSourceSchema = ["--", "--schema", self.import_config.source_schema]
			sqoopSourceTable = self.import_config.source_table
#			sqoopDirectOption = "--direct"
		if self.import_config.common_config.db_oracle == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema.upper(), self.import_config.source_table.upper())
			sqoopDirectOption = "--direct"
		if self.import_config.common_config.db_postgresql == True: 
#			sqoopSourceSchema = "--  --schema  %s"%(self.import_config.source_schema)
			sqoopSourceSchema = ["--", "--schema", self.import_config.source_schema]
			sqoopSourceTable = self.import_config.source_table
		if self.import_config.common_config.db_progress == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table)
			sqoopDirectOption = "--direct"
		if self.import_config.common_config.db_mysql == True: 
			sqoopSourceTable = self.import_config.source_table
		if self.import_config.common_config.db_db2udb == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table.upper())
			sqoopDirectOption = "--direct"
		if self.import_config.common_config.db_db2as400 == True: 
			sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table.upper())
			sqoopDirectOption = "--direct"

		if self.import_config.sqoop_use_generated_sql == True and self.import_config.sqoop_query == None:
			sqoopQuery = self.import_config.sqlGeneratedSqoopQuery

		if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
			# Add the specific Oracle FlashBack columns that we need to import
			sqoopQuery = re.sub('^select', 'select \"VERSIONS_OPERATION\" as \"datalake_flashback_operation\", \"VERSIONS_STARTSCN\" as \"datalake_flashback_startscn\",', self.import_config.sqlGeneratedSqoopQuery)
#			print(self.import_config.generatedSqoopOptions)
#			self.import_config.remove_temporary_files()
#			sys.exit(1)
			# sqoopQuery = re.sub('^select', 'select \"VERSIONS_OPERATION\" as \"datalake_flashback_operation\",', self.import_config.sqlGeneratedSqoopQuery)

		if self.import_config.sqoop_query != None:
			sqoopQuery = self.import_config.sqoop_query

		# Handle mappers, split-by with custom query
		if sqoopQuery != "":
			if "split-by" not in self.import_config.sqoop_options.lower():
				self.import_config.generateSqoopSplitBy()

		self.import_config.generateSqoopBoundaryQuery()

		# Handle the situation where the source dont have a PK and there is no split-by (force mappers to 1)
		if self.import_config.generatedPKcolumns == None and "split-by" not in self.import_config.sqoop_options.lower():
			logging.warning("There is no PrimaryKey in source system and no--split-by in the sqoop_options columns. This will force the import to use only 1 mapper")
			self.import_config.sqlSessions = 1	
	

		# From here and forward we are building the sqoop command with all options

		sqoopCommand = []
		sqoopCommand.extend(["sqoop", "import", "-D", "mapreduce.job.user.classpath.first=true"])
		sqoopCommand.extend(["-D", "mapreduce.job.queuename=%s"%(configuration.get("Sqoop", "yarnqueue"))])
#		sqoopCommand.extend(["-D", "mapred.child.java.opts=\"-Xmx%sm\""%(12288)])
#		sqoopCommand.extend(["-D", "mapreduce.map.java.opts=\"-Xmx%sm\""%(3072)])
#		sqoopCommand.extend(["-D", "mapreduce.reduce.java.opts=\"-Xmx%sm\""%(6144)])
		sqoopCommand.extend(["-D", "mapreduce.map.memory.mb=%s"%(25000)])
		sqoopCommand.extend(["-D", "mapreduce.reduce.memory.mb=%s"%(50000)])
		sqoopCommand.extend(["-D", "oraoop.disabled=true"]) 
		sqoopCommand.extend(["-D", "org.apache.sqoop.splitter.allow_text_splitter=%s"%(self.import_config.sqoop_allow_text_splitter)])

		if "split-by" not in self.import_config.sqoop_options.lower():
			sqoopCommand.append("--autoreset-to-one-mapper")

		# Progress and DB2 AS400 imports needs to know the class name for the JDBC driver. 
		if self.import_config.common_config.db_progress == True or self.import_config.common_config.db_db2as400 == True:
			sqoopCommand.extend(["--driver", self.import_config.common_config.jdbc_driver])

#		sqoopCommand.extend(["--jar-file", self.import_config.common_config.jdbc_classpath])
#		sqoopCommand.extend(["--class-name", self.import_config.common_config.jdbc_driver])

#		sqoopCommand.extend(["--driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"])
#		sqoopCommand.extend(["--class-name", "com.microsoft.sqlserver.jdbc.SQLServerDriver"])

		sqoopCommand.extend(["--class-name", "dbimport"]) 
		sqoopCommand.append("--fetch-size=10000") 

		sqoopCommand.append("--as-parquetfile") 
		sqoopCommand.append("--compress")
		sqoopCommand.extend(["--compression-codec", "snappy"])

		if PKOnlyImport == True:
			sqoopCommand.extend(["--target-dir", self.import_config.sqoop_hdfs_location_pkonly])
		else:
			sqoopCommand.extend(["--target-dir", self.import_config.sqoop_hdfs_location])

		sqoopCommand.extend(["--outdir", self.import_config.common_config.tempdir])
		sqoopCommand.extend(["--connect", "\"%s\""%(self.import_config.common_config.jdbc_url)])
		sqoopCommand.extend(["--username", self.import_config.common_config.jdbc_username])
		sqoopCommand.extend(["--password-file", "file://%s"%(self.import_config.common_config.jdbc_password_file)])
		if sqoopDirectOption != "":
			sqoopCommand.append(sqoopDirectOption)

		sqoopCommand.extend(["--num-mappers", str(self.import_config.sqlSessions)])

		# If we dont have a SQL query to use for sqoop, then we need to specify the table instead
		if sqoopQuery == "" and self.import_config.importPhase != constant.IMPORT_PHASE_ORACLE_FLASHBACK:
			sqoopCommand.extend(["--table", sqoopSourceTable])

#		if self.import_config.import_is_incremental == True and PKOnlyImport == False:	# Cant do incr for PK only imports. Needs to be full
#			self.sqoopIncrNoNewRows = False
#
#			sqoopCommand += "--incremental  %s  "%(self.import_config.sqoop_incr_mode)
#			sqoopCommand += "--check-column  %s  "%(self.import_config.sqoop_incr_column)
#			if self.import_config.sqoop_incr_lastvalue != None: 
#				sqoopCommand += "--last-value  %s  "%(self.import_config.sqoop_incr_lastvalue)
#		else:
#			sqoopCommand += "--delete-target-dir  " 
		sqoopCommand.append("--delete-target-dir")

		if self.import_config.generatedSqoopOptions != "" and self.import_config.generatedSqoopOptions != None:
			sqoopCommand.extend(shlex.split(self.import_config.generatedSqoopOptions))

		if self.import_config.sqoop_options.strip() != "" and self.import_config.sqoop_options != None:
			if self.import_config.common_config.jdbc_force_column_lowercase == True:
				sqoopCommand.extend(shlex.split(self.import_config.sqoop_options.lower()))
			else:
				sqoopCommand.extend(shlex.split(self.import_config.sqoop_options))
#				sqoopCommand.extend(shlex.split(self.import_config.sqoop_options.lower()))
#			sqoopCommand.extend(shlex.split(self.import_config.sqoop_options))

		if self.import_config.sqoopBoundaryQuery.strip() != "" and self.import_config.sqlSessions > 1:
			sqoopCommand.extend(["--boundary-query", self.import_config.sqoopBoundaryQuery])

		if self.import_config.import_is_incremental == True and PKOnlyImport == False:
			incrWhereStatement = self.import_config.getIncrWhereStatement(whereForSqoop=True).replace('"', '\'')
			if incrWhereStatement == "":
				# DBImport is unable to find the max value for the configured incremental column. Is the table empty?
				rowCount = self.import_config.common_config.getJDBCTableRowCount(self.import_config.source_schema, self.import_config.source_table)
				if rowCount == 0:
					self.sqoopIncrNoNewRows = True
					try:
						logging.warning("There are no rows in the source table. As this is an incremental import, sqoop will not run")
						self.import_config.saveSqoopStatistics(self.sqoopStartUTS, sqoopSize=0, sqoopRows=0, sqoopMappers=0)
					except:
						logging.exception("Fatal error when saving sqoop statistics")
						self.import_config.remove_temporary_files()
						sys.exit(1)
					return
				else:
					logging.error("DBImport is unable to find the max value for the configured incremental column.")
					self.import_config.remove_temporary_files()
					sys.exit(1)

		if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
#			sqoopCommand.extend(["--query", "%s where $CONDITIONS and %s"%(sqoopQuery, incrWhereStatement)])
			sqoopCommand.extend(["--query", "%s %s AND $CONDITIONS "%(sqoopQuery, incrWhereStatement)])
		elif sqoopQuery == "":
			if self.import_config.import_is_incremental == True and PKOnlyImport == False:
				sqoopCommand.extend(["--where", incrWhereStatement])
			else:
				if self.import_config.sqoop_sql_where_addition != None:
					sqoopCommand.extend(["--where", self.import_config.sqoop_sql_where_addition.replace('"', '\'')])
			sqoopCommand.extend(sqoopSourceSchema)
		else:
			if self.import_config.import_is_incremental == True and PKOnlyImport == False:
#			if self.import_config.sqoop_sql_where_addition != None:
				sqoopCommand.extend(["--query", "%s where $CONDITIONS and %s"%(sqoopQuery, incrWhereStatement)])
			else:
				if self.import_config.sqoop_sql_where_addition != None:
					sqoopCommand.extend(["--query", "%s where $CONDITIONS and %s"%(sqoopQuery, self.import_config.sqoop_sql_where_addition.replace('"', '\''))])
				else:
					sqoopCommand.extend(["--query", "%s where $CONDITIONS"%(sqoopQuery)])

#		print(sqoopQuery)

		logging.info("Starting sqoop with the following command: %s"%(sqoopCommand))
		sqoopOutput = ""

		print(" _____________________ ")
		print("|                     |")
		print("| Sqoop Import starts |")
		print("|_____________________|")
		print("")

		# Start Sqoop
		sh_session = subprocess.Popen(sqoopCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

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

				if "java.sql.SQLException: ORA-30052:" in row:
					# java.sql.SQLException: ORA-30052: invalid lower limit snapshot expression 
					logging.error("The Oracle Flashback import has passed the valid timeframe for undo logging in Oracle. To recover from this failure, you need to do a full import.")
					self.remove_temporary_files()
					sys.exit(1)

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
				self.import_config.saveSqoopStatistics(self.sqoopStartUTS, sqoopSize=self.sqoopSize, sqoopRows=self.sqoopRows, sqoopMappers=self.import_config.sqlSessions)
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

	def connectToHive(self, forceSkipTest=False):
		logging.debug("Executing import_operations.connectToHive()")

		try:
			self.common_operations.connectToHive(forceSkipTest=forceSkipTest)
		except Exception as ex:
			logging.error(ex)
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if self.globalHiveConfigurationSet == False:
			self.globalHiveConfigurationSet = True
			if self.import_config.hiveJavaHeap != None:
				query = "set hive.tez.container.size=%s"%(self.import_config.hiveJavaHeap)
				self.common_operations.executeHiveQuery(query)

		# Connect to Hive Metastore Database
#		se.common_operations.connectToMetaStoreDB()


		logging.debug("Executing import_operations.connectToHive() - Finished")

	def createExternalImportView(self):
		logging.debug("Executing import_operations.createExternalImportView()")

		if self.import_config.isExternalViewRequired() == True:
			if self.common_operations.checkHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_View) == False:
				logging.info("Creating External Import View")

				query = "create view `%s`.`%s` as %s "%(
					self.import_config.Hive_Import_DB, 
					self.import_config.Hive_Import_View,
					self.import_config.getSelectForImportView())
#				query = "create view `%s`.`%s` as select "%(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_View)
#				query += "%s "%(self.import_config.getSelectForImportView())
#				if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
#					# Add the specific Oracle FlashBack columns that we need to import
#					query += ", `datalake_flashback_operation`"
#					query += ", `datalake_flashback_startscn`"
#				query += "from `%s`.`%s` "%(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)

				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()


		logging.debug("Executing import_operations.createExternalImportView() - Finished")

	def updateExternalImportView(self):
		logging.debug("Executing import_operations.updateExternalImportView()")

		if self.import_config.isExternalViewRequired() == True:
			columnsConfig = self.import_config.getColumnsFromConfigDatabase(sourceIsParquetFile=True) 

			hiveDB = self.import_config.Hive_Import_DB
			hiveView = self.import_config.Hive_Import_View
			columnsHive = self.common_operations.getHiveColumns(hiveDB, hiveView, includeType=True, excludeDataLakeColumns=True) 
			columnsMerge = pd.merge(columnsConfig, columnsHive, on=None, how='outer', indicator='Exist')
			columnsDiffCount  = len(columnsMerge.loc[columnsMerge['Exist'] != 'both'])

			if columnsDiffCount > 0:
				# There is a diff between the configuration and the view. Lets update it
				logging.info("Updating Import View as columns in Hive is not the same as in the configuration")
				query = "alter view `%s`.`%s` as %s "%(
					self.import_config.Hive_Import_DB, 
					self.import_config.Hive_Import_View,
					self.import_config.getSelectForImportView())

				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.updateExternalImportView() - Finished")

	def createExternalImportTable(self):
		logging.debug("Executing import_operations.createExternalImportTable()")

		if self.common_operations.checkHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == True:
			# Table exist, so we need to make sure that it's a managed table and not an external table
			if self.common_operations.isHiveTableExternalParquetFormat(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == False:
				logging.info("Dropping staging table as it's not an external table based on parquet")
				self.common_operations.dropHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
				self.common_operations.reconnectHiveMetaStore()
#				externalTableDeleted = True

#		if self.common_operations.checkHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == False or externalTableDeleted == True:
		if self.common_operations.checkHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == False:
			# Import table does not exist. We just create it in that case
			logging.info("Creating External Import Table")
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


			if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
				# Add the specific Oracle FlashBack columns that we need to import
				query += ", `datalake_flashback_operation` varchar(2)"
				query += ", `datalake_flashback_startscn` string"

			query += ") "

			tableComment = self.import_config.getHiveTableComment()
			if tableComment != None:
				query += "COMMENT \"%s\" "%(tableComment)

			query += "stored as parquet "
#			query += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' "
#			query += "LINES TERMINATED BY '\002' "
#			query += "LOCATION '%s%s/' "%(self.import_config.common_config.hdfs_address, self.import_config.sqoop_hdfs_location)
			query += "LOCATION '%s%s/' "%(self.common_operations.hdfs_address, self.import_config.sqoop_hdfs_location)
#			query += "LOCATION '%s%s/' "%(self.import_config.common_config.getConfigValue(key = "hdfs_address"), self.import_config.sqoop_hdfs_location)
			query += "TBLPROPERTIES ('parquet.compress' = 'SNAPPY') "
#			query += "TBLPROPERTIES('serialization.null.format' = '\\\\N') "
#			query += "TBLPROPERTIES('serialization.null.format' = '\003') "

			self.common_operations.executeHiveQuery(query)
			self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createExternalImportTable() - Finished")

	def convertHiveTableToACID(self):
		if self.common_operations.isHiveTableTransactional(self.Hive_DB, self.Hive_Table) == False:
			self.common_operations.convertHiveTableToACID(self.Hive_DB, self.Hive_Table, createDeleteColumn=self.import_config.soft_delete_during_merge, createMergeColumns=True)

	def addHiveDBImportColumns(self, mergeOperation):
		""" Will add the required DBImport columns in the Hive table """
		logging.debug("Executing import_operations.createHiveMergeColumns()")

		columns = self.common_operations.getHiveColumns(hiveDB=self.Hive_DB, hiveTable=self.Hive_Table, includeType=False, includeComment=False)

		if columns[columns['name'] == 'datalake_source'].empty == True and self.import_config.datalake_source != None:
			query = "alter table `%s`.`%s` add columns ( datalake_source varchar(256) )"%(self.Hive_DB, self.Hive_Table)
			self.common_operations.executeHiveQuery(query)
			self.common_operations.reconnectHiveMetaStore()


		if mergeOperation == False:
			if self.import_config.create_datalake_import_column == True:
				if columns[columns['name'] == 'datalake_insert'].empty == False and columns[columns['name'] == 'datalake_import'].empty == True:
					query = "alter table `%s`.`%s` change column datalake_insert datalake_import timestamp"%(self.Hive_DB, self.Hive_Table)
					self.common_operations.executeHiveQuery(query)
					self.common_operations.reconnectHiveMetaStore()
				elif columns[columns['name'] == 'datalake_import'].empty == True:
					query = "alter table `%s`.`%s` add columns ( datalake_import timestamp COMMENT \"Import time from source database\")"%(self.Hive_DB, self.Hive_Table)
					self.common_operations.executeHiveQuery(query)
					self.common_operations.reconnectHiveMetaStore()

		else:
			if columns[columns['name'] == 'datalake_import'].empty == False:
				query = "alter table `%s`.`%s` change column datalake_import datalake_insert timestamp"%(self.Hive_DB, self.Hive_Table)
				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

			elif columns[columns['name'] == 'datalake_insert'].empty == True:
				query = "alter table `%s`.`%s` add columns ( datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\")"%(self.Hive_DB, self.Hive_Table)
				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

			if columns[columns['name'] == 'datalake_iud'].empty == True:
				query = "alter table `%s`.`%s` add columns ( datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\")"%(self.Hive_DB, self.Hive_Table)
				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

			if columns[columns['name'] == 'datalake_update'].empty == True:
				query = "alter table `%s`.`%s` add columns ( datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\")"%(self.Hive_DB, self.Hive_Table)
				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

			if columns[columns['name'] == 'datalake_delete'].empty == True and self.import_config.soft_delete_during_merge == True:
				query = "alter table `%s`.`%s` add columns ( datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\")"%(self.Hive_DB, self.Hive_Table)
				self.common_operations.executeHiveQuery(query)
				self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createHiveMergeColumns() - Finished")

	def generateCreateTargetTableSQL(self, hiveDB, hiveTable, acidTable=False, buckets=4, restrictColumns=None):
		""" Will generate the common create table for the target table as a list. """
		logging.debug("Executing import_operations.generateCreateTargetTableSQL()")

		queryList = []
		query  = "create table `%s`.`%s` ("%(hiveDB, hiveTable)
		columnsDF = self.import_config.getColumnsFromConfigDatabase() 

		restrictColumnsList = []
		if restrictColumns != None:
			restrictColumnsList = restrictColumns.split(",")

		firstLoop = True
		for index, row in columnsDF.iterrows():
			if restrictColumnsList != []:
				if row['name'] not in restrictColumnsList:
					continue
			if firstLoop == False: query += ", "
			query += "`%s` %s"%(row['name'], row['type'])
			if row['comment'] != None:
				query += " COMMENT \"%s\""%(row['comment'])
			firstLoop = False
		queryList.append(query)

		query = ") "
		tableComment = self.import_config.getHiveTableComment()
		if tableComment != None:
			query += "COMMENT \"%s\" "%(tableComment)

		if acidTable == False:
			query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB') "
		else:
			# TODO: HDP3 shouldnt run this
			query += "CLUSTERED BY ("
			firstColumn = True

			if self.import_config.getPKcolumns() == None:
				logging.error("There is no Primary Key for this table. Please add one in source system or in 'pk_column_override'")
				self.import_config.remove_temporary_files()
				sys.exit(1)

			for column in self.import_config.getPKcolumns().split(","):
				if firstColumn == False:
					query += ", " 
				query += "`" + column + "`" 
				firstColumn = False
			#TODO: Smarter calculation of the number of buckets
			query += ") into %s buckets "%(buckets)
			query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true') "

		queryList.append(query)

		logging.debug("Executing import_operations.generateCreateTargetTableSQL() - Finished")
		return queryList

	def createDeleteTable(self):
		logging.debug("Executing import_operations.createDeleteTable()")

		Hive_Delete_DB = self.import_config.Hive_Delete_DB
		Hive_Delete_Table = self.import_config.Hive_Delete_Table

		if self.common_operations.checkHiveTable(Hive_Delete_DB, Hive_Delete_Table) == False:
			# Target table does not exist. We just create it in that case
			logging.info("Creating Delete table %s.%s in Hive"%(Hive_Delete_DB, Hive_Delete_Table))
			queryList = self.generateCreateTargetTableSQL( 
				hiveDB = Hive_Delete_DB, 
				hiveTable = Hive_Delete_Table, 
				acidTable = False, 
				restrictColumns = self.import_config.getPKcolumns(PKforMerge=True))

			query = "".join(queryList)

			self.common_operations.executeHiveQuery(query)
			self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createDeleteTable() - Finished")
# self.Hive_HistoryTemp_DB = "etl_import_staging"
# self.Hive_HistoryTemp_Table = self.Hive_DB + "__" + self.Hive_Table + "__temporary"
# self.Hive_Import_PKonly_DB = "etl_import_staging"
# self.Hive_Import_PKonly_Table = self.Hive_DB + "__" + self.Hive_Table + "__pkonly__staging"
# self.Hive_Import_Delete_DB = "etl_import_staging"
# self.Hive_Import_Delete_Table = self.Hive_DB + "__" + self.Hive_Table + "__pkonly__deleted"


	def createHistoryTable(self):
		logging.debug("Executing import_operations.createHistoryTable()")

		Hive_History_DB = self.import_config.Hive_History_DB
		Hive_History_Table = self.import_config.Hive_History_Table

		if self.common_operations.checkHiveTable(Hive_History_DB, Hive_History_Table) == False:
			# Target table does not exist. We just create it in that case
			logging.info("Creating History table %s.%s in Hive"%(Hive_History_DB, Hive_History_Table))
			queryList = self.generateCreateTargetTableSQL( hiveDB=Hive_History_DB, hiveTable=Hive_History_Table, acidTable=False)

			query = queryList[0]
			if self.import_config.datalake_source != None:
				query += ", datalake_source varchar(256)"
			query += ", datalake_iud char(1) COMMENT \"SQL operation of this record was I=Insert, U=Update or D=Delete\""
			query += ", datalake_timestamp timestamp COMMENT \"Timestamp for SQL operation in Datalake\""
			query += queryList[1]

			self.common_operations.executeHiveQuery(query)
			self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createHistoryTable() - Finished")

	def createTargetTable(self):
		logging.debug("Executing import_operations.createTargetTable()")
		if self.common_operations.checkHiveTable(self.Hive_DB, self.Hive_Table) == True:
			# Table exist, so we need to make sure that it's a managed table and not an external table
			if self.common_operations.isHiveTableExternal(self.Hive_DB, self.Hive_Table) == True:
				self.common_operations.dropHiveTable(self.Hive_DB, self.Hive_Table)

		# We need to check again as the table might just been droped because it was an external table to begin with
		if self.common_operations.checkHiveTable(self.Hive_DB, self.Hive_Table) == False:
			# Target table does not exist. We just create it in that case
			logging.info("Creating Target table %s.%s in Hive"%(self.Hive_DB, self.Hive_Table))

			queryList = self.generateCreateTargetTableSQL( 
				hiveDB=self.Hive_DB, 
				hiveTable=self.Hive_Table, 
				acidTable=self.import_config.create_table_with_acid)

			query = queryList[0]

			if self.import_config.datalake_source != None:
				query += ", datalake_source varchar(256)"

			if self.import_config.import_with_merge == False:
				if self.import_config.create_datalake_import_column == True:
					query += ", datalake_import timestamp COMMENT \"Import time from source database\""
			else:
				query += ", datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""
				query += ", datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\""
				query += ", datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\""

				if self.import_config.soft_delete_during_merge == True:
					query += ", datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\""

			query += queryList[1]

			self.common_operations.executeHiveQuery(query)
			self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.createTargetTable() - Finished")
		
	def updateTargetTable(self):
		logging.info("Updating Target table columns based on source system schema")
		self.updateHiveTable(self.Hive_DB, self.Hive_Table)

	def updateHistoryTable(self):
		logging.info("Updating History table columns based on source system schema")
		self.updateHiveTable(self.import_config.Hive_History_DB, self.import_config.Hive_History_Table)

	def updateDeleteTable(self):
		logging.info("Updating Delete table columns based on source system schema")
		self.updateHiveTable(self.import_config.Hive_Delete_DB, self.import_config.Hive_Delete_Table, restrictColumns = self.import_config.getPKcolumns(PKforMerge=True))

	def updateExternalImportTable(self):
		logging.info("Updating Import table columns based on source system schema")
		self.updateHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table, sourceIsParquetFile=True)
	
		tableLocation = self.common_operations.getTableLocation(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
		configuredLocation = "%s%s"%(self.common_operations.hdfs_address, self.import_config.sqoop_hdfs_location)

		if tableLocation != configuredLocation:
			logging.info("The configured location for the external table have changed. Updating the external table")
			logging.debug("tableLocation:      %s"%(tableLocation))
			logging.debug("configuredLocation: %s"%(configuredLocation))

			query = "alter table `%s`.`%s` set location \"%s\""%(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table, self.import_config.sqoop_hdfs_location)
			self.common_operations.executeHiveQuery(query)
		

	def updateColumnsForImportTable(self, columnsDF):
		""" Parquet import format from sqoop cant handle all datatypes correctly. So for the column definition, we need to change some. We also replace <SPACE> with underscore in the column name """
		columnsDF["type"].replace(["date"], "string", inplace=True)
		columnsDF["type"].replace(["timestamp"], "string", inplace=True)
		columnsDF["type"].replace(["decimal(.*)"], "string", regex=True, inplace=True)
		columnsDF["type"].replace(["bigint"], "string", regex=True, inplace=True)

		# If you change any of the name replace rows, you also need to change the same data in function self.copyHiveTable() and import_definitions.saveColumnData()
		columnsDF["name"].replace([" "], "_", regex=True, inplace=True)
		columnsDF["name"].replace(["%"], "pct", regex=True, inplace=True)
		columnsDF["name"].replace(["\("], "_", regex=True, inplace=True)
		columnsDF["name"].replace(["\)"], "_", regex=True, inplace=True)
		columnsDF["name"].replace(["ü"], "u", regex=True, inplace=True)
		columnsDF["name"].replace(["å"], "a", regex=True, inplace=True)
		columnsDF["name"].replace(["ä"], "a", regex=True, inplace=True)
		columnsDF["name"].replace(["ö"], "o", regex=True, inplace=True)

		return columnsDF

#	def addDatalakeImportColumn(self):
#		""" Adding datalake_column to Hive Table if it does not exists """
#		logging.debug("Executing import_operations.addDatalakeImportColumn()")
#		columnsHive   = self.common_operations.getHiveColumns(self.Hive_DB, self.Hive_Table, excludeDataLakeColumns=False) 
##		columnsHive   = self.common_operations.getColumnsFromHiveTable(self.Hive_DB, self.Hive_Table, excludeDataLakeColumns=False) 
#		if len(columnsHive.loc[columnsHive['name'] == 'datalake_import']) == 0:
#			query = "alter table `%s`.`%s` add columns ( datalake_import timestamp COMMENT \"Import time from source database\")"%(self.Hive_DB, self.Hive_Table)
#			self.common_operations.executeHiveQuery(query)
#			self.common_operations.reconnectHiveMetaStore()
#
#		logging.debug("Executing import_operations.addDatalakeImportColumn() - Finished")


	def updateHiveTable(self, hiveDB, hiveTable, restrictColumns=None, sourceIsParquetFile=False):
		""" Update the target table based on the column information in the configuration database """
		# TODO: If there are less columns in the source table together with a rename of a column, then it wont work. Needs to be handled
		logging.debug("Executing import_operations.updateTargetTable()")
		columnsConfig = self.import_config.getColumnsFromConfigDatabase(restrictColumns=restrictColumns, sourceIsParquetFile=sourceIsParquetFile) 
		columnsHive   = self.common_operations.getHiveColumns(hiveDB, hiveTable, includeType=True, excludeDataLakeColumns=True) 

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

					self.import_config.logHiveColumnRename(rowInConfig['name'], rowInHive["name"], hiveDB=hiveDB, hiveTable=hiveTable)
					if rowInConfig["type"] != rowInHive["type"]:
						self.import_config.logHiveColumnTypeChange(rowInConfig['name'], rowInConfig['type'], previous_columnType=rowInHive["type"], hiveDB=hiveDB, hiveTable=hiveTable) 
				else:
					if columnsMergeLeftOnlyCount == 1 and columnsMergeRightOnlyCount == 1:
						# So the columns are not in the same position, but it's only one column that changed. In that case, we just rename that one column
						rowInMergeLeft  = columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iloc[0]
						rowInMergeRight = columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'right_only'].iloc[0]
						rowInConfig = columnsConfig.loc[columnsConfig['name'] == rowInMergeLeft['name']].iloc[0]
						rowInHive = columnsHive.loc[columnsHive['name'] == rowInMergeRight['name']].iloc[0]
						logging.debug(rowInConfig["name"])
						logging.debug(rowInConfig["type"])
						logging.debug("--------------------")
						logging.debug(rowInHive["name"])
						logging.debug(rowInHive["type"])

						query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, rowInHive['name'], rowInConfig['name'], rowInHive['type'])
						self.common_operations.executeHiveQuery(query)

						self.import_config.logHiveColumnRename(rowInConfig['name'], rowInHive["name"], hiveDB=hiveDB, hiveTable=hiveTable)
						if rowInConfig["type"] != rowInHive["type"]:
							self.import_config.logHiveColumnTypeChange(rowInConfig['name'], rowInConfig['type'], previous_columnType=rowInHive["type"], hiveDB=hiveDB, hiveTable=hiveTable) 

			self.common_operations.reconnectHiveMetaStore()
			columnsHive   = self.common_operations.getHiveColumns(hiveDB, hiveTable, includeType=True, includeComment=True, excludeDataLakeColumns=True) 
#			columnsHive   = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 
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

			self.import_config.logHiveColumnAdd(fullRow['name'], columnType=fullRow['type'], hiveDB=hiveDB, hiveTable=hiveTable) 

		# Check for changed column types
		self.common_operations.reconnectHiveMetaStore()
		columnsHive = self.common_operations.getHiveColumns(hiveDB, hiveTable, includeType=True, excludeDataLakeColumns=True) 
#		columnsHive = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 

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

			self.import_config.logHiveColumnTypeChange(row['name'], columnType=row['type'], previous_columnType=previous_columnType, hiveDB=hiveDB, hiveTable=hiveTable) 

		# Check for change column comments
		self.common_operations.reconnectHiveMetaStore()
		columnsHive = self.common_operations.getHiveColumns(hiveDB, hiveTable, includeType=True, includeComment=True, excludeDataLakeColumns=True) 
#		columnsHive = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 
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
				logging.debug("PK in Hive:   %s"%(PKonHiveTable))
				logging.debug("PK in Config: %s"%(PKinConfigDB))
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
				self.common_operations.reconnectHiveMetaStore()

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

		if self.import_config.create_foreign_keys == 0:
			logging.info("No Foreign Keys will be created for this table as it's disabled in the configuration")
			return

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

			self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing import_operations.updateFKonTable()  - Finished")

	def removeHiveLocks(self,):
		if self.import_config.common_config.getConfigValue(key = "hive_remove_locks_by_force") == True:
			self.common_operations.removeHiveLocksByForce(self.Hive_DB, self.Hive_Table)
			self.common_operations.removeHiveLocksByForce(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
			self.common_operations.removeHiveLocksByForce(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_View)
			self.common_operations.removeHiveLocksByForce(self.import_config.Hive_Import_PKonly_DB, self.import_config.Hive_Import_PKonly_Table)
			self.common_operations.removeHiveLocksByForce(self.import_config.Hive_Delete_DB, self.import_config.Hive_Delete_Table)
			self.common_operations.removeHiveLocksByForce(self.import_config.Hive_HistoryTemp_DB, self.import_config.Hive_HistoryTemp_Table)

	def truncateTargetTable(self,):
		logging.info("Truncating Target table in Hive")
		self.common_operations.connectToHive(forceSkipTest=True)
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
		logging.debug("Executing import_operations.copyHiveTable()")

		columnMerge = self.common_operations.getHiveColumnNameDiff(sourceDB=sourceDB, sourceTable=sourceTable, targetDB=targetDB, targetTable=targetTable, sourceIsImportTable=True)

		firstLoop = True
		columnDefinitionSource = ""
		columnDefinitionTarget = ""
		for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
			if firstLoop == False: columnDefinitionSource += ", "
			if firstLoop == False: columnDefinitionTarget += ", "
			columnDefinitionSource += "`%s`"%(row['sourceName'])
			columnDefinitionTarget += "`%s`"%(row['targetName'])
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
		logging.debug("Executing import_operations.copyHiveTable() - Finished")

	def resetIncrMinMaxValues(self, maxValue):
		self.import_config.resetIncrMinMaxValues(maxValue=maxValue)

	def resetIncrMaxValue(self, hiveDB=None, hiveTable=None):
		""" Will read the Max value from the Hive table and save that into the incr_maxvalue column """
		logging.debug("Executing import_operations.resetIncrMaxValue()")

		if hiveDB == None: hiveDB = self.Hive_DB
		if hiveTable == None: hiveTable = self.Hive_Table

		if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
			logging.error("Oracle Flashback imports does not support repairing the table.")
			self.import_config.remove_temporary_files()
			sys.exit(1)

		if self.import_config.import_is_incremental == True:
			self.sqoopIncrNoNewRows = False
			query = "select max(`%s`) from `%s`.`%s` "%(self.import_config.sqoop_incr_column, hiveDB, hiveTable)

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

		tablesRepaired = False
		for index, row in result_df.iterrows():
			tablesRepaired = True
			hiveDB = row[0]
			hiveTable = row[1]
			logging.info("Repairing table \u001b[33m%s.%s\u001b[0m"%(hiveDB, hiveTable))
			self.setHiveTable(hiveDB, hiveTable)
			self.resetIncrMaxValue(hiveDB, hiveTable)
			logging.info("")

		if tablesRepaired == False:
			logging.info("\n\u001b[32mNo incremental tables found that could be repaired\u001b[0m\n")

		logging.debug("Executing import_operations.repairAllIncrementalImports() - Finished")
