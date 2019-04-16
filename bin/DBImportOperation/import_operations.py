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
from datetime import datetime
import pandas as pd
import time


class operation(object):
	def __init__(self, Hive_DB, Hive_Table):
		logging.debug("Executing import_operations.__init__()")
		self.Hive_DB = Hive_DB.lower()	 
		self.Hive_Table = Hive_Table.lower()	 
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
		self.sqoopIncrMaxvaluePending = None

		try:
			# Initialize the two core classes. import_config will initialize common_config aswell
			self.common_operations = common_operations.operation(Hive_DB, Hive_Table)
			self.import_config = import_definitions.config(Hive_DB, Hive_Table)
		except:
			sys.exit(1)

		try:
			self.import_config.getImportConfig()
			self.startDate = self.import_config.startDate
			self.import_config.lookupConnectionAlias()
		except:
			self.import_config.remove_temporary_files()
			sys.exit(1)

		logging.debug("Executing import_operations.__init__() - Finished")

	def remove_temporary_files(self):
		self.import_config.remove_temporary_files()

	def checkTimeWindow(self):
		self.import_config.checkTimeWindow()
	
	def setStage(self, stage):
		self.import_config.setStage(stage)
	
	def getStage(self):
		return self.import_config.getStage()
	
	def clearStage(self):
		self.import_config.clearStage()
	
	def setStageOnlyInMemory(self):
		self.import_config.setStageOnlyInMemory()
	
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

	def getSourceTableSchema(self):
		try:
			self.import_config.getJDBCTableDefinition()
			self.import_config.updateLastUpdateFromSource()
			self.import_config.removeFKforTable()
			self.import_config.saveColumnData()
			self.import_config.setPrimaryKeyColumn()
			self.import_config.saveKeyData()
			self.import_config.saveGeneratedData()
		except:
			logging.exception("Fatal error when reading and/or processing source table schema")
			self.import_config.remove_temporary_files()
			sys.exit(1)

	def runSqoop(self, PKOnlyImport):
		logging.debug("Executing import_operations.runSqoop()")

		self.sqoopStartTimestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
		self.sqoopStartUTS = int(time.time())

		if self.import_config.import_is_incremental == True:
			logging.error("Incremental imports not supported yet")
			self.remove_temporary_files()
			sys.exit(3)
			
		# Fetch the number of mappers that should be used
		self.import_config.calculateJobMappers()


		# Sets the correct sqoop table and schema that will be used if a custom SQL is not used
		sqoopQuery = ""
		sqoopSourceSchema = "" 
#		sqoopSourceTable = self.import_config.source_table
		sqoopSourceTable = ""
		sqoopDirectOption = ""
#		if self.import_config.source_schema != "-": 
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
#			sqoopDirectOption = "--direct  "
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
#			sqoopQuery = self.import_config.sqlGeneratedSqoopQuery.replace("\"", "\\\"")
			sqoopQuery = self.import_config.sqlGeneratedSqoopQuery

		# Handle mappers, split-by with custom query
		if sqoopQuery != "":
			if "split-by" not in self.import_config.sqoop_options.lower():
				logging.warning("There is no --split-by in the sqoop_options columns but the import is executing with a custom query. This is not supported and will force the import to use only 1 mapper")
				self.import_config.sqlSessions = 1	

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
		sqoopCommand += "--delete-target-dir  " 

		if "split-by" not in self.import_config.sqoop_options.lower():
			sqoopCommand += "--autoreset-to-one-mapper  "

		# Progress and DB2 AS400 imports needs to know the class name for the JDBC driver. 
		if self.import_config.common_config.db_progress == True or self.import_config.common_config.db_db2as400 == True:
			sqoopCommand += "--driver  %s  "%(self.import_config.common_config.jdbc_driver)

		sqoopCommand += "--class-name  dbimport  " 

		if self.import_config.sqoop_import_type == "hive":
			if PKOnlyImport == True:
				sqoopCommand += "--hcatalog-database  %s  "%(self.import_config.Hive_Import_PKonly_DB)
				sqoopCommand += "--hcatalog-table  %s  "%(self.import_config.Hive_Import_PKonly_Table)
			else:
				sqoopCommand += "--hcatalog-database  %s  "%(self.import_config.Hive_Import_DB)
				sqoopCommand += "--hcatalog-table  %s  "%(self.import_config.Hive_Import_Table)
			sqoopCommand += "--hive-drop-import-delims  "

		if self.import_config.sqoop_import_type == "hdfs":
#			sqoopCommand += "--as-textfile "
#			sqoopCommand += "--fields-terminated-by \001 "
#			sqoopCommand += "--lines-terminated-by \002 "
#			sqoopCommand += "--null-string \003 "
#			sqoopCommand += "--null-non-string \003 "

			sqoopCommand += "--as-parquetfile  " 
			sqoopCommand += "--compress  "
			sqoopCommand += "--compression-codec  snappy  "

#			sqoopCommand += "--null-string '\\\\N' "
#			sqoopCommand += "--null-non-string '\\\\N' "
#			sqoopCommand += "--as-sequencefile "
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

		if self.import_config.import_is_incremental == True and PKOnlyImport == False:	# Cant do incr for PK only imports. Needs to be full
			sqoopCommand += "--incremental  %s  "%(self.import_config.incr_mode)
			# TODO: This code is not completed. Incre not fully supported

		if self.import_config.generatedSqoopOptions != "" and self.import_config.generatedSqoopOptions != None:
			sqoopCommand += "%s  "%(self.import_config.generatedSqoopOptions.replace(" ", "  "))
		if self.import_config.sqoop_options != "" and self.import_config.sqoop_options != None:
			sqoopCommand += "%s  "%(self.import_config.sqoop_options.replace(" ", "  "))

		if sqoopQuery == "":
			if self.import_config.sqoop_sql_where_addition != None:
				sqoopCommand += "--where  \"%s\""%(self.import_config.sqoop_sql_where_addition.replace('"', '\''))
			sqoopCommand += "%s  "%(sqoopSourceSchema)
		else:
			if self.import_config.sqoop_sql_where_addition != None:
				sqoopCommand += "--query  \"%s where $CONDITION and %s\""%(sqoopQuery, self.import_config.sqoop_sql_where_addition.replace('"', '\''))
			else:
				sqoopCommand += "--query  \"%s where $CONDITIONS\""%(sqoopQuery)

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
				sqoopOutput += row + "\n"
				self.parseSqoopOutput(row)

		# Print what is left in output after sqoop finished
		for row in sh_session.stdout.readlines():
			row = row.decode('utf-8').rstrip()
			if row != "": 
				print(row)
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
		
		if PKOnlyImport == False:
			try:
				self.import_config.saveSqoopStatistics(self.sqoopStartUTS, self.sqoopSize, self.sqoopRows, self.sqoopIncrMaxvaluePending)
			except:
				logging.exception("Fatal error when saving sqoop statistics")
				self.import_config.remove_temporary_files()
				sys.exit(1)

		logging.debug("Executing import_operations.runSqoop() - Finished")


	def parseSqoopOutput(self, row ):
		# 19/03/28 05:15:44 HDFS: Number of bytes written=17
		if "HDFS: Number of bytes written" in row:
			self.sqoopSize = row.split("=")[1]
		
		# 19/03/28 05:15:44 INFO mapreduce.ImportJobBase: Retrieved 2 records.
		if "mapreduce.ImportJobBase: Retrieved" in row:
			self.sqoopRows = row.split(" ")[5]

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
		""" Parquet import format from sqoop cant handle all datatypes correctly. So for the column definition, we need to change some """
		columnsDF["type"].replace(["date"], "string", inplace=True)
		columnsDF["type"].replace(["timestamp"], "string", inplace=True)
		columnsDF["type"].replace(["decimal(.*)"], "string", regex=True, inplace=True)
		return columnsDF

	def updateHiveTable(self, hiveDB, hiveTable):
		""" Update the target table based on the column information in the configuration database """
# TODO: Handle deletes in source systems. As of right now, we only add and change columns. If it is a full import, we should replace all columns if the numbers of columns is equal or smaller compared to source schema
		logging.debug("Executing import_operations.updateTargetTable()")
		logging.info("Updating Hive table columns based on source system schema")
		columnsConfig = self.import_config.getColumnsFromConfigDatabase() 
		columnsHive   = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 

		# If we are working on the import table, we need to change some column types to handle Parquet files
		if hiveDB == self.import_config.Hive_Import_DB and hiveTable == self.import_config.Hive_Import_Table:
			columnsConfig = self.updateColumnsForImportTable(columnsConfig)

		# Check for missing columns
		columnsConfigOnlyName = columnsConfig.filter(['name']).sort_values(by=['name'], ascending=True)
		columnsHiveOnlyName = columnsHive.filter(['name']).sort_values(by=['name'], ascending=True)
		columnsMergeOnlyName = pd.merge(columnsConfigOnlyName, columnsHiveOnlyName, on=None, how='outer', indicator='Exist')

		for index, row in columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iterrows():
			# This will iterate over columns that only exists in the config and not in Hive. We add these to Hive
			fullRow = columnsConfig.loc[columnsConfig['name'] == row['name']].iloc[0]
			query = "alter table `%s`.`%s` add columns (`%s` %s"%(hiveDB, hiveTable, fullRow['name'], fullRow['type'])
			if fullRow['comment'] != None:
				query += " COMMENT \"%s\""%(fullRow['comment'])
			query += ")"

			print(query)
			self.common_operations.executeHiveQuery(query)

		# Check for changed column types
		self.common_operations.reconnectHiveMetaStore()
		columnsHive = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 

		columnsConfigOnlyNameType = columnsConfig.filter(['name', 'type']).sort_values(by=['name'], ascending=True)
		columnsHiveOnlyNameType = columnsHive.filter(['name', 'type']).sort_values(by=['name'], ascending=True)
		columnsMergeOnlyNameType = pd.merge(columnsConfigOnlyNameType, columnsHiveOnlyNameType, on=None, how='outer', indicator='Exist')

		for index, row in columnsMergeOnlyNameType.loc[columnsMergeOnlyNameType['Exist'] == 'left_only'].iterrows():
			# This will iterate over columns that had the type changed from the source
			query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, row['name'], row['name'], row['type'])

			self.common_operations.executeHiveQuery(query)

		# Check for change column comments
		self.common_operations.reconnectHiveMetaStore()
		columnsHive = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 
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
					# TODO: Delete PK
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

		sourceColumns = self.common_operations.getHiveColumns(sourceDB, sourceTable)
		targetColumns = self.common_operations.getHiveColumns(targetDB, targetTable)
		columnMerge = pd.merge(sourceColumns, targetColumns, on=None, how='outer', indicator='Exist')

		firstLoop = True
		columnDefinition = ""
		for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
			if firstLoop == False: columnDefinition += ", "
			columnDefinition += "`%s`"%(row['col'])
			firstLoop = False

		query = "insert into `%s`.`%s` ("%(targetDB, targetTable)
		query += columnDefinition
		if self.import_config.datalake_source != None:
			query += ", datalake_source"
		if self.import_config.create_datalake_import_column == True:
			query += ", datalake_import"

		query += ") select "
		query += columnDefinition
		if self.import_config.datalake_source != None:
			query += ", '%s'"%(self.import_config.datalake_source)
		if self.import_config.create_datalake_import_column == True:
			query += ", '%s'"%(self.import_config.sqoop_last_execution_timestamp)

		query += " from `%s`.`%s` "%(sourceDB, sourceTable)
		if self.import_config.nomerge_ingestion_sql_addition != None:
			query += self.import_config.nomerge_ingestion_sql_addition

		self.common_operations.executeHiveQuery(query)
		logging.debug("Executing import_definitions.copyHiveTable() - Finished")

