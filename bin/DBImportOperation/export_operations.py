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
from common import constants as constant
from common.Exceptions import *
from DBImportConfig import export_config
from DBImportOperation import common_operations
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time


class operation(object, metaclass=Singleton):
	def __init__(self, connectionAlias=None, targetSchema=None, targetTable=None):
		logging.debug("Executing export_operations.__init__()")

		self.connectionAlias = connectionAlias
		self.targetSchema = targetSchema
		self.targetTable = targetTable
		self.hiveDB = None
		self.hiveTable = None
		self.hiveExportTempDB = None
		self.hiveExportTempTable = None
		self.tempTableNeeded = None

		self.sqoopSize = None
		self.sqoopRows = None
		self.sqoopMappers = None

		self.globalHiveConfigurationSet = False

		if connectionAlias == None and targetSchema == None and targetTable == None:
			self.export_config = export_config.config()
			self.common_operations = common_operations.operation()
		else:
			try:
				self.export_config = export_config.config(connectionAlias=connectionAlias, targetSchema=targetSchema, targetTable=targetTable)
				self.common_operations = common_operations.operation()
	
				self.export_config.getExportConfig()
				self.export_config.common_config.lookupConnectionAlias(connection_alias=self.connectionAlias)
	
				self.hiveDB = self.export_config.hiveDB
				self.hiveTable = self.export_config.hiveTable
				self.hiveExportTempDB = self.export_config.hiveExportTempDB
				self.hiveExportTempTable = self.export_config.hiveExportTempTable
	
#				self.export_config.setHiveTable(hiveDB=self.hiveDB, hiveTable=self.hiveTable)
				self.common_operations.setHiveTable(Hive_DB=self.hiveDB, Hive_Table=self.hiveTable)
				self.hiveTableIsTransactional = self.common_operations.isHiveTableTransactional(hiveDB=self.hiveDB, hiveTable=self.hiveTable)
				self.hiveTableIsView = self.common_operations.isHiveTableView(hiveDB=self.hiveDB, hiveTable=self.hiveTable)
	
			except invalidConfiguration as errMsg:
				logging.error(errMsg)
				self.export_config.remove_temporary_files()
				sys.exit(1)
			except:
				self.export_config.remove_temporary_files()
				raise
				sys.exit(1)

		logging.debug("Executing export_operations.__init__() - Finished")

	def runStage(self, stage):
		self.export_config.setStage(stage)

		if self.export_config.common_config.getConfigValue(key = "export_stage_disable") == True:
			logging.error("Stage execution disabled from DBImport configuration")
			self.export_config.remove_temporary_files()
			sys.exit(1)

		tempStage = self.export_config.getStage()
		if stage == tempStage:
			return True
		else:
			return False
	
	def setStage(self, stage, force=False):
		self.export_config.setStage(stage, force=force)
	
	def getStage(self):
		return self.export_config.getStage()
	
	def clearStage(self):
		self.export_config.clearStage()
	
	def saveRetryAttempt(self, stage):
		self.export_config.saveRetryAttempt(stage)
	
	def setStageOnlyInMemory(self):
		self.export_config.setStageOnlyInMemory()

	def convertStageStatisticsToJSON(self):
		self.export_config.convertStageStatisticsToJSON()
	
	def saveStageStatistics(self):
		self.export_config.saveStageStatistics()
	
	def saveIncrPendingValues(self):
		self.export_config.saveIncrPendingValues()

	def resetIncrMinMaxValues(self, maxValue):
		self.export_config.resetIncrMinMaxValues(maxValue=maxValue)

	def resetMaxValueFromTarget(self):
		self.export_config.resetMaxValueFromTarget()

	def getHiveTableSchema(self):
		try:
			self.export_config.updateLastUpdateFromHive()
			self.export_config.saveColumnData(columnsDF = self.common_operations.getHiveColumns(self.hiveDB, self.hiveTable, includeType=True, includeComment=True, includeIdx=True))
		except invalidConfiguration as errMsg:
			logging.error(errMsg)
			self.export_config.remove_temporary_files()
			sys.exit(1)
		except:
			logging.exception("Fatal error when reading and/or processing Hive table schema")
			self.export_config.remove_temporary_files()
			sys.exit(1)

	def dropJDBCTable(self):
		try:
			if self.export_config.truncateTargetTable == True:
				logging.info("Dropping Target Table")
				self.export_config.common_config.dropJDBCTable(schema=self.targetSchema, table=self.targetTable)
		except invalidConfiguration as errMsg:
			logging.error(errMsg)
			self.export_config.remove_temporary_files()
			sys.exit(1)
		except:
			logging.exception("Fatal error when truncating target table")
			self.export_config.remove_temporary_files()
			sys.exit(1)

	def truncateJDBCTable(self):
		try:
			if self.export_config.truncateTargetTable == True:
				logging.info("Truncating Target Table")
				self.export_config.common_config.truncateJDBCTable(schema=self.targetSchema, table=self.targetTable)
		except invalidConfiguration as errMsg:
			logging.error(errMsg)
			self.export_config.remove_temporary_files()
			sys.exit(1)
		except:
			logging.exception("Fatal error when truncating target table")
			self.export_config.remove_temporary_files()
			sys.exit(1)


	def createTargetTable(self):
		try:
			if self.export_config.checkJDBCTable() == False: 
				self.export_config.createTargetTable()
		except SQLerror as errMsg:
			logging.error(errMsg)
			self.export_config.remove_temporary_files()
			sys.exit(1)
		except:
			logging.exception("Fatal error when creating the target table")
			self.export_config.remove_temporary_files()
			sys.exit(1)

	def updateTargetTable(self):
		try:
			self.export_config.updateTargetTable()
		except SQLerror as errMsg:
			if "ORA-22859: invalid modification of columns" in str(errMsg):
				# We get this message from Oracle when we try to change a column type that is not supported
				if self.export_config.exportIsIncremental == False:
					try:
						logging.info("There is a column type change that is not supported by Oracle. But because this export is a full export, we will drop the Target table and recreate it automatically")
						self.dropJDBCTable()
						self.createTargetTable()
						self.export_config.updateTargetTable()
					except invalidConfiguration as errMsg:
						logging.error(errMsg)
						self.export_config.remove_temporary_files()
						sys.exit(1)
					except:
						logging.exception("Fatal error when updating the target table")
						self.export_config.remove_temporary_files()
						sys.exit(1)
				else:
					logging.error("There is a column type change required on the target table but Oracle doesnt support that change. Drop the table or disable the column is the only option. As this is an incremental export, we cant do that automatically as it might result in loss of data")
					self.export_config.remove_temporary_files()
					sys.exit(1)
			else:
				logging.error(errMsg)
				self.export_config.remove_temporary_files()
				sys.exit(1)
		except invalidConfiguration as errMsg:
			logging.error(errMsg)
			self.export_config.remove_temporary_files()
			sys.exit(1)
		except:
			logging.exception("Fatal error when updating the target table")
			self.export_config.remove_temporary_files()
			sys.exit(1)

	def getJDBCTableRowCount(self):
		try:
			self.export_config.getJDBCTableRowCount()
		except:
			logging.exception("Fatal error when reading source table row count")
			self.export_config.remove_temporary_files()
			sys.exit(1)

#	def getImportTableRowCount(self):
#		try:
#			whereStatement = None
#			if self.export_config.exportIsIncremental == True:
#				whereStatement = self.export_config.getIncrWhereStatement(ignoreIfOnlyIncrMax=True)
#
#			exportTableRowCount = self.common_operations.getHiveTableRowCount(self.export_config.Hive_Import_DB, self.export_config.Hive_Import_Table, whereStatement=whereStatement)
#			self.export_config.saveHiveTableRowCount(exportTableRowCount)
#		except:
#			logging.exception("Fatal error when reading Hive table row count")
#			self.export_config.remove_temporary_files()
#			sys.exit(1)

	def discoverAndAddTablesFromHive(self, dbalias, schema, dbFilter=None, tableFilter=None, addDBToTable=False, addCustomText=None, addCounterToTable=False, counterStart=None):
		""" This is the main function to search for tables/view in Hive and add them to export_tables """
		logging.debug("Executing export_operations.discoverAndAddTablesFromHive()")
		errorDuringAdd = False

		sourceDF = self.common_operations.getHiveTables(dbFilter=dbFilter, tableFilter=tableFilter)

		if len(sourceDF) == 0:
			print("There are no tables in the source database that we dont already have in DBImport")
			self.export_config.remove_temporary_files()
			sys.exit(0)

		exportDF = self.export_config.getExportTables(dbalias=dbalias, schema=schema)

		mergeDF = pd.merge(sourceDF, exportDF, on=None, how='outer', indicator='Exist')
		mergeDF['targetTable'] = mergeDF['hiveTable']
		discoveredTables = len(mergeDF.loc[mergeDF['Exist'] == 'left_only'])

		if addCounterToTable == True or addDBToTable == True or addCustomText != None:
			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					mergeDF.loc[index, 'targetTable'] = "_%s"%(mergeDF.loc[index, 'targetTable'])

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

					mergeDF.loc[index, 'targetTable'] = "%s%s%s"%(zeroToAdd, startValue, mergeDF.loc[index, 'targetTable'])
					startValue += 1

		if addDBToTable == True:
			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					mergeDF.loc[index, 'targetTable'] = "%s%s"%(mergeDF.loc[index, 'hiveDB'], mergeDF.loc[index, 'targetTable'])

		if addCustomText != None:
			for index, row in mergeDF.iterrows():
				if mergeDF.loc[index, 'Exist'] == 'left_only':
					mergeDF.loc[index, 'targetTable'] = "%s%s"%(addCustomText.lower().strip(), mergeDF.loc[index, 'targetTable'])

		if discoveredTables == 0:
			print("There are no tables in the source database that we dont already have in DBImport")
			self.export_config.remove_temporary_files()
			sys.exit(0)

		# At this stage, we have discovered tables in the source system that we dont know about in DBImport
		print("The following tables and/or views have been discovered in Hive and not found as export tables in DBImport")
		print("")
		print("%-20s %-40s %-30s %-20s %s"%("Hive DB", "Hive Table", "Connection Alias", "Schema", "Table/View"))
		print("=============================================================================================================================")

		for index, row in mergeDF.loc[mergeDF['Exist'] == 'left_only'].iterrows():
#			if addSchemaToTable == True:
#				hiveTable = "%s_%s"%(row['schema'].lower().strip(), row['table'].lower().strip())
#			else:
#				hiveTable = row['table'].lower()
#			print("%-20s%-40s%-30s%-20s%s"%(hiveDB, hiveTable, dbalias, row['schema'], row['table']))
			print("%-20s %-40s %-30s %-20s %s"%(row['hiveDB'], row['hiveTable'], dbalias, schema, row['targetTable']))

		answer = input("Do you want to add these exports to DBImport? (y/N): ")
		if answer == "y":
			print("")
			for index, row in mergeDF.loc[mergeDF['Exist'] == 'left_only'].iterrows():
#			for index, row in mergeDFLeftOnly.iterrows():
#				if addSchemaToTable == True:
#					hiveTable = "%s_%s"%(row['schema'].lower().strip(), row['table'].lower().strip())
#				else:
#					hiveTable = row['table'].lower()
				addResult = self.export_config.addExportTable(
					dbalias=dbalias,
					schema=schema,
					table=row['targetTable'],
					hiveDB=row['hiveDB'],
					hiveTable=row['hiveTable'])

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

		logging.debug("Executing export_operations.discoverAndAddTablesFromHive() - Finished")


	def clearTableRowCount(self):
		try:
			self.export_config.clearTableRowCount()
		except invalidConfiguration as errMsg:
			logging.error(errMsg)
			self.export_config.remove_temporary_files()
			sys.exit(1)
		except:
			logging.exception("Fatal error when clearing row counts from previous exports")
			self.export_config.remove_temporary_files()
			sys.exit(1)

	def isExportTempTableNeeded(self):
		try:
			self.tempTableNeeded = self.export_config.isExportTempTableNeeded(hiveTableIsTransactional = self.hiveTableIsTransactional, hiveTableIsView = self.hiveTableIsView)
		except invalidConfiguration as errMsg:
			logging.error(errMsg)
			self.export_config.remove_temporary_files()
			sys.exit(1)
		except:
			logging.exception("Fatal error when clearing row counts from previous exports")
			self.export_config.remove_temporary_files()
			sys.exit(1)

		return self.tempTableNeeded

	def getIncrMaxvalueFromHive(self, column=None, hiveDB=None, hiveTable=None):
		logging.debug("Executing export_operations.getIncrMaxvalueFromHive()")

		if hiveDB == None: hiveDB = self.hiveDB
		if hiveTable == None: hiveTable = self.hiveTable
		if column == None: column = self.export_config.incr_column

		query = "select max(`%s`) from `%s`.`%s`"%(column, hiveDB, hiveTable)
		resultDF = self.common_operations.executeHiveQuery(query)
		maxValue = resultDF.loc[0][0]
		logging.debug("Maxvalue: %s"%(maxValue))

		logging.debug("Executing export_operations.getIncrMaxvalueFromHive() - Finished")
		return maxValue

	def updateStatisticsOnExportedTable(self,):
		if self.common_operations.isHiveTableView(hiveDB = self.hiveDB, hiveTable = self.hiveTable) == False:
			logging.info("Updating the Hive statistics on the exported table")
			self.common_operations.updateHiveTableStatistics(self.hiveDB, self.hiveTable)

	def createExportTempTable(self):
		logging.debug("Executing export_operations.createExportTempTable()")

		if self.common_operations.checkHiveTable(self.hiveExportTempDB, self.hiveExportTempTable) == False:
			# Target table does not exist. We just create it in that case
			logging.info("Creating Export Temp table %s.%s in Hive"%(self.hiveExportTempDB, self.hiveExportTempTable))

			columnsDF = self.export_config.getColumnsFromConfigDatabase(excludeColumns=True)
			query  = "create table `%s`.`%s` ("%(self.hiveExportTempDB, self.hiveExportTempTable)

			firstLoop = True
			for index, row in columnsDF.iterrows():
				targetColumnName = row['targetColumnName']

				if targetColumnName != None and targetColumnName.strip() != "":
					columnName = targetColumnName
				else:
					columnName = row['hiveColumnName']

				if firstLoop == False: query += ", "
				query += "`%s` %s"%(columnName, row['columnType'])
				if row['comment'] != None:
					query += " COMMENT \"%s\""%(row['comment'])
				firstLoop = False
			query += ") STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB')"

			self.common_operations.executeHiveQuery(query)
			self.common_operations.reconnectHiveMetaStore()

		logging.debug("Executing export_operations.createExportTempTable() - Finished")

	def connectToHive(self,):
		logging.debug("Executing export_operations.connectToHive()")

		try:
			self.common_operations.connectToHive()
		except Exception as ex:
			logging.error(ex)
			self.export_config.remove_temporary_files()
			sys.exit(1)

		if self.globalHiveConfigurationSet == False:
			self.globalHiveConfigurationSet = True
			if self.export_config.hiveJavaHeap != None:
				query = "set hive.tez.container.size=%s"%(self.export_config.hiveJavaHeap)
				self.common_operations.executeHiveQuery(query)

		logging.debug("Executing export_operations.connectToHive() - Finished")

	def remove_temporary_files(self):
		self.export_config.remove_temporary_files()

	def checkTimeWindow(self):
		self.export_config.checkTimeWindow()
	
	def updateExportTempTable(self):
		""" Update the Export Temp table based on the column information in the configuration database """
		logging.debug("Executing export_operations.updateExportTempTable()")
		hiveDB = self.hiveExportTempDB
		hiveTable = self.hiveExportTempTable

		columnsHive   = self.common_operations.getHiveColumns(hiveDB, hiveTable, includeType=True, includeIdx=False)

		columnsConfig = self.export_config.getColumnsFromConfigDatabase(excludeColumns=True)
		columnsConfig.rename(columns={'hiveColumnName':'name', 'columnType':'type'}, inplace=True) 

		for index, row in columnsConfig.iterrows():
			targetColumnName = row['targetColumnName']
			if targetColumnName != None and targetColumnName.strip() != "":
				columnsConfig.iloc[index]['name'] = targetColumnName

		columnsConfig.drop('targetColumnName', axis=1, inplace=True)

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

					self.export_config.logHiveColumnRename(rowInConfig['name'], rowInHive["name"], hiveDB=hiveDB, hiveTable=hiveTable)
				
					if rowInConfig["type"] != rowInHive["type"]:
						self.export_config.logHiveColumnTypeChange(rowInConfig['name'], rowInConfig['type'], previous_columnType=rowInHive["type"], hiveDB=hiveDB, hiveTable=hiveTable) 
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

						self.export_config.logHiveColumnRename(rowInConfig['name'], rowInHive["name"], hiveDB=hiveDB, hiveTable=hiveTable)

			self.common_operations.reconnectHiveMetaStore()
			columnsHive   = self.common_operations.getHiveColumns(hiveDB, hiveTable, includeType=True, includeIdx=False)
			columnsHiveOnlyName = columnsHive.filter(['name'])
			columnsMergeOnlyName = pd.merge(columnsConfigOnlyName, columnsHiveOnlyName, on=None, how='outer', indicator='Exist')


		for index, row in columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iterrows():
			# This will iterate over columns that only exists in the config and not in Hive. We add these to Hive
			fullRow = columnsConfig.loc[columnsConfig['name'] == row['name']].iloc[0]
			query = "alter table `%s`.`%s` add columns (`%s` %s"%(hiveDB, hiveTable, fullRow['name'], fullRow['type'])
#			if fullRow['comment'] != None:
#				query += " COMMENT \"%s\""%(fullRow['comment'])
			query += ")"

			self.common_operations.executeHiveQuery(query)

			self.export_config.logHiveColumnAdd(fullRow['name'], columnType=fullRow['type'], hiveDB=hiveDB, hiveTable=hiveTable) 


		# Check for changed column types
		self.common_operations.reconnectHiveMetaStore()
		columnsHive = self.common_operations.getHiveColumns(hiveDB, hiveTable, includeType=True, includeIdx=False, includeComment=True)

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

			self.export_config.logHiveColumnTypeChange(row['name'], columnType=row['type'], previous_columnType=previous_columnType, hiveDB=hiveDB, hiveTable=hiveTable) 

		# Check for change column comments
		self.common_operations.reconnectHiveMetaStore()
		columnsHive = self.common_operations.getHiveColumns(hiveDB, hiveTable, includeType=True, includeIdx=False, includeComment=True)
		columnsHive['comment'].replace('', None, inplace = True)		# Replace blank column comments with None as it would otherwise trigger an alter table on every run
		columnsMerge = pd.merge(columnsConfig, columnsHive, on=None, how='outer', indicator='Exist')

		for index, row in columnsMerge.loc[columnsMerge['Exist'] == 'left_only'].iterrows():
			if row['comment'] == None: row['comment'] = ""
			query = "alter table `%s`.`%s` change column `%s` `%s` %s comment \"%s\""%(hiveDB, hiveTable, row['name'], row['name'], row['type'], row['comment'])

			self.common_operations.executeHiveQuery(query)

		logging.debug("Executing export_operations.updateTargetTable() - Finished")

	def truncateExportTempTable(self,):
		logging.info("Truncating Export Temp table in Hive")
		self.common_operations.truncateHiveTable(self.hiveExportTempDB, self.hiveExportTempTable)

	def updateStatisticsOnExportTempTable(self,):
		logging.info("Updating the Hive statistics on the Export Temp table")
		self.common_operations.updateHiveTableStatistics(self.hiveExportTempDB, self.hiveExportTempTable)

	def insertDataIntoExportTempTable(self):
		""" Insert data from the source Hive Table into the Export Temp Table """
		logging.debug("Executing export_operations.insertDataIntoExportTempTable()")

		columnsConfig = self.export_config.getColumnsFromConfigDatabase(excludeColumns=True)

		query  = "insert into `%s`.`%s` ("%(self.hiveExportTempDB, self.hiveExportTempTable)

		firstLoop = True
		for index, row in columnsConfig.iterrows():
			if firstLoop == False: query += ", "
			targetColumnName = row['targetColumnName']
			columnName = row['hiveColumnName']
			if targetColumnName != None and targetColumnName.strip() != "":
				columnName = targetColumnName

			query += "`%s`"%(columnName)
			firstLoop = False

		query += ") select "
		firstLoop = True
		for index, row in columnsConfig.iterrows():
			if firstLoop == False: query += ", "
			query += "`%s`"%(row['hiveColumnName'])
			firstLoop = False

		query += " from `%s`.`%s` "%(self.hiveDB, self.hiveTable)

		if self.export_config.exportIsIncremental == True:
			query += "where "
			query += self.export_config.getIncrWhereStatement()

		self.common_operations.executeHiveQuery(query)
		logging.debug("Executing export_operations.insertDataIntoExportTempTable() - Finished")

	def fetchIncrMinMaxvalue(self):
		logging.debug("Executing export_operations.fetchIncrMinMaxvalue()")

		maxValue = self.getIncrMaxvalueFromHive()
		minValue = self.export_config.incr_maxvalue
		self.export_config.saveIncrMinMaxValue(minValue=minValue, maxValue=maxValue)

		logging.debug("Executing export_operations.fetchIncrMinMaxvalue() - Finished")

	def runSqoop(self):
		logging.debug("Executing export_operations.runSqoop()")

		self.sqoopStartTimestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
		self.sqoopStartUTS = int(time.time())

		forceColumnUppercase = False
		if self.export_config.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
			forceColumnUppercase = True
			targetSchema = self.targetSchema.upper()
			targetTable = self.targetTable.upper()
		else:
			targetSchema = self.targetSchema
			targetTable = self.targetTable

		self.export_config.common_config.getJDBCTableDefinition(source_schema = targetSchema, source_table = targetTable)
#		print(self.export_config.common_config.source_columns_df)
		columnsTarget = self.export_config.common_config.source_columns_df
		columnsTarget.rename(columns={'SOURCE_COLUMN_NAME':'name'}, inplace=True) 
		columnsTarget.drop('IS_NULLABLE', axis=1, inplace=True)
		columnsTarget.drop('SOURCE_COLUMN_COMMENT', axis=1, inplace=True)
		columnsTarget.drop('SOURCE_COLUMN_TYPE', axis=1, inplace=True)
		columnsTarget.drop('TABLE_COMMENT', axis=1, inplace=True)

		if self.isExportTempTableNeeded() == True:
			hiveDB = self.hiveExportTempDB
			hiveTable = self.hiveExportTempTable
		else:
			hiveDB = self.hiveDB
			hiveTable = self.hiveTable

		logging.debug("forceColumnUppercase: %s"%(forceColumnUppercase))

#		columnsHive = self.common_operations.getHiveColumns(hiveDB, hiveTable, includeType=False, includeIdx=False, forceColumnUppercase=forceColumnUppercase)
#		print(columnsHive)
		columnsConfig = self.export_config.getColumnsFromConfigDatabase(excludeColumns=True, forceColumnUppercase=forceColumnUppercase)
		columnsConfig.rename(columns={'hiveColumnName':'name'}, inplace=True) 
		for index, row in columnsConfig.iterrows():
			targetColumnName = row['targetColumnName']
			if targetColumnName != None and targetColumnName.strip() != "":
				columnsConfig.iloc[index]['name'] = targetColumnName
		columnsConfig.drop('targetColumnName', axis=1, inplace=True)
		columnsConfig.drop('columnType', axis=1, inplace=True)
		columnsConfig.drop('comment', axis=1, inplace=True)
#		print(columnsConfig)
#		print(columnsTarget)
#		columnsMerge = pd.merge(columnsHive, columnsTarget, on=None, how='outer', indicator='Exist')
		columnsMerge = pd.merge(columnsConfig, columnsTarget, on=None, how='outer', indicator='Exist')

		columnList = []
		for index, row in columnsMerge.loc[columnsMerge['Exist'] == 'both'].iterrows():
			if forceColumnUppercase == True:
				columnList.append(row['name'].upper())
			else:
				columnList.append(row['name'])

		schemaOptions = ""

		# Fetch the number of mappers that should be used
		self.export_config.calculateJobMappers()

		# Sets the correct sqoop table and schema that will be used if a custom SQL is not used
		sqoopTargetSchema = [] 
		sqoopTargetTable = ""
		sqoopDirectOption = ""
		if self.export_config.common_config.db_mssql == True: 
			sqoopTargetSchema = ["--", "--schema", targetSchema]
			sqoopTargetTable = targetTable
		if self.export_config.common_config.db_oracle == True: 
#			sqoopTargetTable = "\"%s\".\"%s\""%(targetSchema, targetTable)
			sqoopTargetTable = "%s.%s"%(targetSchema, targetTable)
		if self.export_config.common_config.db_mysql == True: 
			sqoopTargetTable = targetTable
		if self.export_config.common_config.db_db2udb == True: 
			sqoopTargetTable = "%s.%s"%(targetSchema, targetTable)


#		if sqoopQuery != "":
#			if "split-by" not in self.import_config.sqoop_options.lower():
#				self.import_config.generateSqoopSplitBy()
#
#		# Handle the situation where the source dont have a PK and there is no split-by (force mappers to 1)
#		if self.import_config.generatedPKcolumns == None and "split-by" not in self.import_config.sqoop_options.lower():
#			logging.warning("There is no PrimaryKey in source system and no--split-by in the sqoop_options columns. This will force the import to use only 1 mapper")
#			self.import_config.sqlSessions = 1	
#	
#
#		# From here and forward we are building the sqoop command with all options

		sqoopCommand = []
		sqoopCommand.extend(["sqoop", "export", "-D", "mapreduce.job.user.classpath.first=true"])
		sqoopCommand.extend(["-D", "mapreduce.job.queuename=%s"%(configuration.get("Sqoop", "yarnqueue"))])
		sqoopCommand.extend(["-D", "oraoop.disabled=true"]) 
		# TODO: Add records to settings
		sqoopCommand.extend(["-D", "sqoop.export.records.per.statement=10000"]) 
		sqoopCommand.extend(["-D", "sqoop.export.records.per.transaction=1"]) 
#		sqoopCommand.extend(["-D", "org.apache.sqoop.splitter.allow_text_splitter=%s"%(self.import_config.sqoop_allow_text_splitter)])
#
#		if "split-by" not in self.import_config.sqoop_options.lower():
#			sqoopCommand.append("--autoreset-to-one-mapper")
#
#		# Progress and DB2 AS400 imports needs to know the class name for the JDBC driver. 
#		if self.import_config.common_config.db_progress == True or self.import_config.common_config.db_db2as400 == True:
#			sqoopCommand.extend(["--driver", self.import_config.common_config.jdbc_driver])

#		sqoopCommand.extend(["--jar-file", "%s"%(self.import_config.common_config.jdbc_driver)])
		sqoopCommand.extend(["--class-name", "dbimport"]) 

		sqoopCommand.extend(["--hcatalog-database", hiveDB])
		sqoopCommand.extend(["--hcatalog-table", hiveTable])

		sqoopCommand.extend(["--outdir", self.export_config.common_config.tempdir])
		sqoopCommand.extend(["--connect", "\"%s\""%(self.export_config.common_config.jdbc_url)])
		sqoopCommand.extend(["--username", self.export_config.common_config.jdbc_username])
		sqoopCommand.extend(["--password-file", "file://%s"%(self.export_config.common_config.jdbc_password_file)])
		if sqoopDirectOption != "":
			sqoopCommand.append(sqoopDirectOption)

		mapColumnJava = self.export_config.getSqoopMapColumnJava()
		if len(mapColumnJava) > 0: 
			sqoopCommand.extend(["--map-column-java", ",".join(mapColumnJava)])

		sqoopCommand.extend(["--num-mappers", str(self.export_config.sqlSessions)])

		# If we dont have a SQL query to use for sqoop, then we need to specify the table instead
		sqoopCommand.extend(["--columns", ",".join(columnList)])
		sqoopCommand.extend(["--table", sqoopTargetTable])

		sqoopCommand.extend(sqoopTargetSchema)

		logging.info("Starting sqoop with the following command: %s"%(sqoopCommand))
		sqoopOutput = ""

		print(" _____________________ ")
		print("|                     |")
		print("| Sqoop Export starts |")
		print("|_____________________|")
		print("")

		# Start Sqoop
#		sh_session = subprocess.Popen(sqoopCommandList, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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

		if sh_session.returncode != 0:
			sqoopOutput += " ERROR "

		print(" ________________________ ")
		print("|                        |")
		print("| Sqoop Export completed |")
		print("|________________________|")
		print("")

		# At this stage, the entire sqoop job is run and we fetched all data from the output. Lets parse and store it
		if self.sqoopSize == None: self.sqoopSize = 0
		if self.sqoopRows == None: self.sqoopRows = 0
		if self.sqoopMappers == None: self.sqoopMappers = 0
		
		# Check for errors in output
		sqoopWarning = False
		if " ERROR " in sqoopOutput:
			for row in sqoopOutput.split("\n"):
		
				if "Not authorized to access topics: " in row:
					# This is not really an error, but we cant verify the table.
					logging.warning("Problem sending data to Atlas")
					sqoopWarning = True
		
				if "ERROR hook.AtlasHook" in row:
					# This is not really an error, but we cant send to Atlas
					logging.warning("Problem sending data to Atlas")
					sqoopWarning = True

				if sqoopWarning == False:
					# We detected the string ERROR in the output but we havent found a vaild reason for it in the IF statements above.
					# We need to mark the sqoop command as error and exit the program
					logging.error("Unknown error in sqoop export. Please check the output for errors and try again")
					self.remove_temporary_files()
					sys.exit(1)

		# No errors detected in the output and we are ready to store the result
		logging.info("Sqoop executed successfully")			
		
		try:
			self.export_config.saveSqoopStatistics(self.sqoopStartUTS, sqoopSize=self.sqoopSize, sqoopRows=self.sqoopRows, sqoopMappers=self.sqoopMappers)
		except:
			logging.exception("Fatal error when saving sqoop statistics")
			self.export_config.remove_temporary_files()
			sys.exit(1)

		logging.debug("Executing export_operations.runSqoop() - Finished")

	def parseSqoopOutput(self, row ):
		# HDFS: Number of bytes written=17
		if "HDFS: Number of bytes read" in row:
			self.sqoopSize = int(row.split("=")[1])
		
		# 19/05/09 11:15:59 INFO mapreduce.ExportJobBase: Exported 8 records.
		if "mapreduce.ExportJobBase: Exported" in row:
			self.sqoopRows = int(row.split(" ")[5])

		# Launched map tasks=1
		if "Launched map tasks" in row:
			self.sqoopMappers = int(row.split("=")[1])


	def getHiveTableRowCount(self):
		try:
			whereStatement = None

#			if self.isExportTempTableNeeded() == True:
#				hiveDB = self.hiveExportTempDB
#				hiveTable = self.hiveExportTempTable
#			else:
			hiveDB = self.hiveDB
			hiveTable = self.hiveTable

			if self.export_config.incr_validation_method == "incr":
				whereStatement = self.export_config.getIncrWhereStatement(excludeMinValue=False)
			else:
				# Needed to make sure that we are not validating against new rows that might have been saved in Hive
				if self.export_config.exportIsIncremental == True:
					whereStatement = self.export_config.getIncrWhereStatement(excludeMinValue=True)

			hiveTableRowCount = self.common_operations.getHiveTableRowCount(hiveDB, hiveTable, whereStatement=whereStatement)
			self.export_config.saveHiveTableRowCount(hiveTableRowCount)
		except:
			logging.exception("Fatal error when reading Hive table row count")
			self.export_config.remove_temporary_files()
			sys.exit(1)

	def isThereIncrDataToExport(self):
		try:
			return self.export_config.isThereIncrDataToExport()
		except:
			logging.exception("Fatal error when reading Hive table row count")
			self.export_config.remove_temporary_files()
			sys.exit(1)


	def getTargetTableRowCount(self):
		try:
#			whereStatement = self.import_config.getIncrWhereStatement(ignoreIfOnlyIncrMax=True)
#			if whereStatement == None and self.import_config.import_with_merge == True and self.import_config.soft_delete_during_merge == True:
#				whereStatement = "datalake_iud != 'D'"
#			targetTableRowCount = self.export_config.getJDBCTableRowCount()
#			self.export_config.saveTargetTableRowCount(targetTableRowCount)
			whereStatement = None
			if self.export_config.incr_validation_method == "incr":
				whereStatement = self.export_config.getIncrWhereStatement(whereForTarget=True, excludeMinValue=False)
			self.export_config.getJDBCTableRowCount(whereStatement=whereStatement)
		except:
			logging.exception("Fatal error when reading Hive table row count")
			self.export_config.remove_temporary_files()
			sys.exit(1)

	def validateRowCount(self):
		try:
			validateResult = self.export_config.validateRowCount() 
		except:
			logging.exception("Fatal error when validating exported rows")
			self.export_config.remove_temporary_files()
			sys.exit(1)

		if validateResult == False:
			self.export_config.remove_temporary_files()
			sys.exit(1)

# =====================================================================================================================================
# DELETE ALL AFTER THIS
# =====================================================================================================================================


#	def setHiveTable(self, Hive_DB, Hive_Table):
#		""" Sets the parameters to work against a new Hive database and table """
#		self.Hive_DB = Hive_DB.lower()
#		self.Hive_Table = Hive_Table.lower()
#
#		self.common_operations.setHiveTable(self.Hive_DB, self.Hive_Table)
#		self.import_config.setHiveTable(self.Hive_DB, self.Hive_Table)
#
#		try:
#			self.import_config.getImportConfig()
#			self.startDate = self.import_config.startDate
#			self.import_config.lookupConnectionAlias()
#		except invalidConfiguration as errMsg:
#			logging.error(errMsg)
#			self.import_config.remove_temporary_files()
#			sys.exit(1)
#		except:
#			self.import_config.remove_temporary_files()
#			raise
#			sys.exit(1)
#
#	
#	def checkHiveDB(self, hiveDB):
#		try:
#			self.common_operations.checkHiveDB(hiveDB)
#		except databaseNotFound as errMsg:
#			logging.error(errMsg)
#			self.import_config.remove_temporary_files()
#			sys.exit(1)
#		except:
#			self.import_config.remove_temporary_files()
#			raise
#
#	def validateSqoopRowCount(self):
#		try:
#			validateResult = self.import_config.validateRowCount(validateSqoop=True) 
#		except:
#			logging.exception("Fatal error when validating imported rows by sqoop")
#			self.import_config.remove_temporary_files()
#			sys.exit(1)
#
#		if validateResult == False:
#			self.import_config.remove_temporary_files()
#			sys.exit(1)
#
#	def validateIncrRowCount(self):
#		try:
#			validateResult = self.import_config.validateRowCount(validateSqoop=True, incremental=True) 
#		except:
#			logging.exception("Fatal error when validating imported incremental rows")
#			self.import_config.remove_temporary_files()
#			sys.exit(1)
#
#		if validateResult == False:
#			self.import_config.remove_temporary_files()
#			sys.exit(1)
#
#	def createExternalImportTable(self,):
#		logging.debug("Executing import_operations.createExternalTable()")
#
#		if self.common_operations.checkHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == True:
#			# Table exist, so we need to make sure that it's a managed table and not an external table
#			if self.common_operations.isHiveTableExternalParquetFormat(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == False:
#				logging.info("Dropping staging table as it's not an external table based on parquet")
#				self.common_operations.dropHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
#				self.common_operations.reconnectHiveMetaStore()
#
#		if self.common_operations.checkHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table) == False:
#			# Import table does not exist. We just create it in that case
#			query  = "create external table `%s`.`%s` ("%(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
#			columnsDF = self.import_config.getColumnsFromConfigDatabase() 
#			columnsDF = self.updateColumnsForImportTable(columnsDF)
#
#			firstLoop = True
#			for index, row in columnsDF.iterrows():
#				if firstLoop == False: query += ", "
#
#				# As Parquet import format writes timestamps in the wrong format, we need to force them to string
##				if row['type'] == "timestamp":
##					query += "`%s` string"%(row['name'])
##				else:
#				query += "`%s` %s"%(row['name'], row['type'])
#
#				if row['comment'] != None:
#					query += " COMMENT \"%s\""%(row['comment'])
#				firstLoop = False
#			query += ") "
#
#			tableComment = self.import_config.getHiveTableComment()
#			if tableComment != None:
#				query += "COMMENT \"%s\" "%(tableComment)
##
#			query += "stored as parquet "
##			query += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' "
##			query += "LINES TERMINATED BY '\002' "
#			query += "LOCATION '%s%s/' "%(self.import_config.common_config.hdfs_address, self.import_config.sqoop_hdfs_location)
#			query += "TBLPROPERTIES ('parquet.compress' = 'SNAPPY') "
###			query += "TBLPROPERTIES('serialization.null.format' = '\\\\N') "
##			query += "TBLPROPERTIES('serialization.null.format' = '\003') "
#
#			self.common_operations.executeHiveQuery(query)
##			self.common_operations.reconnectHiveMetaStore()
#
#		logging.debug("Executing import_operations.createExternalTable() - Finished")
#
#	def convertHiveTableToACID(self):
#		if self.common_operations.isHiveTableTransactional(self.Hive_DB, self.Hive_Table) == False:
#			self.common_operations.convertHiveTableToACID(self.Hive_DB, self.Hive_Table, createDeleteColumn=self.import_config.soft_delete_during_merge, createMergeColumns=True)
#
#	def addHiveMergeColumns(self):
#		""" Will add the required columns for merge operations in the Hive table """
#		logging.debug("Executing import_operations.createHiveMergeColumns()")
#
#		columns = self.common_operations.getHiveColumns(hiveDB=self.Hive_DB, hiveTable=self.Hive_Table, includeType=False, includeComment=False)
#		if columns[columns['name'] == 'datalake_import'].empty == False:
#			query = "alter table `%s`.`%s` change column datalake_import datalake_insert timestamp"%(self.Hive_DB, self.Hive_Table)
#			self.common_operations.executeHiveQuery(query)
#		elif columns[columns['name'] == 'datalake_insert'].empty == True:
#			query = "alter table `%s`.`%s` add columns ( datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\")"%(self.Hive_DB, self.Hive_Table)
#			self.common_operations.executeHiveQuery(query)
#
#		if columns[columns['name'] == 'datalake_iud'].empty == True:
#			query = "alter table `%s`.`%s` add columns ( datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\")"%(self.Hive_DB, self.Hive_Table)
#			self.common_operations.executeHiveQuery(query)
#
#		if columns[columns['name'] == 'datalake_update'].empty == True:
#			query = "alter table `%s`.`%s` add columns ( datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\")"%(self.Hive_DB, self.Hive_Table)
#			self.common_operations.executeHiveQuery(query)
#
#		if columns[columns['name'] == 'datalake_delete'].empty == True and self.import_config.soft_delete_during_merge == True:
#			query = "alter table `%s`.`%s` add columns ( datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\")"%(self.Hive_DB, self.Hive_Table)
#			self.common_operations.executeHiveQuery(query)
#
#		logging.debug("Executing import_operations.createHiveMergeColumns() - Finished")
#
#	def generateCreateTargetTableSQL(self, hiveDB, hiveTable, acidTable=False, buckets=4, restrictColumns=None):
#		""" Will generate the common create table for the target table as a list. """
#		logging.debug("Executing import_operations.generateCreateTargetTableSQL()")
#
#		queryList = []
#		query  = "create table `%s`.`%s` ("%(hiveDB, hiveTable)
#		columnsDF = self.import_config.getColumnsFromConfigDatabase() 
#
#		restrictColumnsList = []
#		if restrictColumns != None:
#			restrictColumnsList = restrictColumns.split(",")
#
#		firstLoop = True
#		for index, row in columnsDF.iterrows():
#			if restrictColumnsList != []:
#				if row['name'] not in restrictColumnsList:
#					continue
#			if firstLoop == False: query += ", "
#			query += "`%s` %s"%(row['name'], row['type'])
#			if row['comment'] != None:
#				query += " COMMENT \"%s\""%(row['comment'])
#			firstLoop = False
#		queryList.append(query)
#
#		query = ") "
#		tableComment = self.import_config.getHiveTableComment()
#		if tableComment != None:
#			query += "COMMENT \"%s\" "%(tableComment)
#
#		if acidTable == False:
#			query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB') "
#		else:
#			# TODO: HDP3 shouldnt run this
#			query += "CLUSTERED BY ("
#			firstColumn = True
#			for column in self.import_config.getPKcolumns().split(","):
#				if firstColumn == False:
#					query += ", " 
#				query += "`" + column + "`" 
#				firstColumn = False
#			#TODO: Smarter calculation of the number of buckets
#			query += ") into %s buckets "%(buckets)
#			query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true') "
#
#		queryList.append(query)
#
#		logging.debug("Executing import_operations.generateCreateTargetTableSQL() - Finished")
#		return queryList
#
#	def createDeleteTable(self):
#		logging.debug("Executing import_operations.createDeleteTable()")
#
#		Hive_Delete_DB = self.import_config.Hive_Delete_DB
#		Hive_Delete_Table = self.import_config.Hive_Delete_Table
#
#		if self.common_operations.checkHiveTable(Hive_Delete_DB, Hive_Delete_Table) == False:
#			# Target table does not exist. We just create it in that case
#			logging.info("Creating Delete table %s.%s in Hive"%(Hive_Delete_DB, Hive_Delete_Table))
#			queryList = self.generateCreateTargetTableSQL( 
#				hiveDB = Hive_Delete_DB, 
#				hiveTable = Hive_Delete_Table, 
#				acidTable = False, 
#				restrictColumns = self.import_config.getPKcolumns(PKforMerge=True))
#
#			query = "".join(queryList)
#
#			self.common_operations.executeHiveQuery(query)
#			self.common_operations.reconnectHiveMetaStore()
#
#		logging.debug("Executing import_operations.createDeleteTable() - Finished")
## self.Hive_HistoryTemp_DB = "etl_import_staging"
## self.Hive_HistoryTemp_Table = self.Hive_DB + "__" + self.Hive_Table + "__temporary"
## self.Hive_Import_PKonly_DB = "etl_import_staging"
## self.Hive_Import_PKonly_Table = self.Hive_DB + "__" + self.Hive_Table + "__pkonly__staging"
## self.Hive_Import_Delete_DB = "etl_import_staging"
## self.Hive_Import_Delete_Table = self.Hive_DB + "__" + self.Hive_Table + "__pkonly__deleted"
#
#
#	def createHistoryTable(self):
#		logging.debug("Executing import_operations.createHistoryTable()")
#
#		Hive_History_DB = self.import_config.Hive_History_DB
#		Hive_History_Table = self.import_config.Hive_History_Table
#
#		if self.common_operations.checkHiveTable(Hive_History_DB, Hive_History_Table) == False:
#			# Target table does not exist. We just create it in that case
#			logging.info("Creating History table %s.%s in Hive"%(Hive_History_DB, Hive_History_Table))
#			queryList = self.generateCreateTargetTableSQL( hiveDB=Hive_History_DB, hiveTable=Hive_History_Table, acidTable=False)
#
#			query = queryList[0]
#			if self.import_config.datalake_source != None:
#				query += ", datalake_source varchar(256)"
#			query += ", datalake_iud char(1) COMMENT \"SQL operation of this record was I=Insert, U=Update or D=Delete\""
#			query += ", datalake_timestamp timestamp COMMENT \"Timestamp for SQL operation in Datalake\""
#			query += queryList[1]
#
#			self.common_operations.executeHiveQuery(query)
#			self.common_operations.reconnectHiveMetaStore()
#
#		logging.debug("Executing import_operations.createHistoryTable() - Finished")
#
#	def createTargetTableOLD(self):
#		logging.debug("Executing import_operations.createTargetTable()")
#		if self.common_operations.checkHiveTable(self.Hive_DB, self.Hive_Table) == True:
#			# Table exist, so we need to make sure that it's a managed table and not an external table
#			if self.common_operations.isHiveTableExternal(self.Hive_DB, self.Hive_Table) == True:
#				self.common_operations.dropHiveTable(self.Hive_DB, self.Hive_Table)
#
#		# We need to check again as the table might just been droped because it was an external table to begin with
#		if self.common_operations.checkHiveTable(self.Hive_DB, self.Hive_Table) == False:
#			# Target table does not exist. We just create it in that case
#			logging.info("Creating Target table %s.%s in Hive"%(self.Hive_DB, self.Hive_Table))
#
#			queryList = self.generateCreateTargetTableSQL( 
#				hiveDB=self.Hive_DB, 
#				hiveTable=self.Hive_Table, 
#				acidTable=self.import_config.create_table_with_acid)
#
#			query = queryList[0]
##			query += ", datalake_iud char(1) COMMENT \"SQL operation of this record was I=Insert, U=Update or D=Delete\""
#			query += ", datalake_timestamp timestamp COMMENT \"Timestamp for SQL operation in Datalake\""
##			query += queryList[1]
#
##			query  = "create table `%s`.`%s` ("%(self.import_config.Hive_DB, self.import_config.Hive_Table)
##			columnsDF = self.import_config.getColumnsFromConfigDatabase() 
#
##			firstLoop = True
##			for index, row in columnsDF.iterrows():
##				if firstLoop == False: query += ", "
##				query += "`%s` %s"%(row['name'], row['type'])
##				if row['comment'] != None:
##					query += " COMMENT \"%s\""%(row['comment'])
##				firstLoop = False
#
#			if self.import_config.datalake_source != None:
#				query += ", datalake_source varchar(256)"
#
##			self.import_config.import_with_merge = False
##			self.import_config.create_table_with_acid = False
#
#			if self.import_config.import_with_merge == False:
#				if self.import_config.create_datalake_import_column == True:
#					query += ", datalake_import timestamp COMMENT \"Import time from source database\""
#			else:
#				query += ", datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""
#				query += ", datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\""
#				query += ", datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\""
#
#				if self.import_config.soft_delete_during_merge == True:
#					query += ", datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\""
#
##			query += ") "
##
##			tableComment = self.import_config.getHiveTableComment()
##			if tableComment != None:
##				query += "COMMENT \"%s\" "%(tableComment)
##
##			if self.import_config.create_table_with_acid == False:
##				query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB') "
##			else:
##				# TODO: HDP3 shouldnt run this
##				query += "CLUSTERED BY ("
##				firstColumn = True
##				for column in self.import_config.getPKcolumns().split(","):
##					if firstColumn == False:
##						query += ", " 
##					query += "`" + column + "`" 
##					firstColumn = False
##				#TODO: Smarter calculation of the number of buckets
##				query += ") into 1 buckets "
##				query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true') "
#			query += queryList[1]
#
#			self.common_operations.executeHiveQuery(query)
#			self.common_operations.reconnectHiveMetaStore()
#
#		logging.debug("Executing import_operations.createTargetTable() - Finished")
#		
##	def updateTargetTable(self):
##		logging.info("Updating Target table columns based on source system schema")
##		self.updateHiveTable(self.Hive_DB, self.Hive_Table)
#
#	def updateHistoryTable(self):
#		logging.info("Updating History table columns based on source system schema")
#		self.updateHiveTable(self.import_config.Hive_History_DB, self.import_config.Hive_History_Table)
#
#	def updateDeleteTable(self):
#		logging.info("Updating Delete table columns based on source system schema")
#		self.updateHiveTable(self.import_config.Hive_Delete_DB, self.import_config.Hive_Delete_Table, restrictColumns = self.import_config.getPKcolumns(PKforMerge=True))
#
#	def updateExternalImportTable(self):
#		logging.info("Updating Import table columns based on source system schema")
#		self.updateHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
#
#	def updateColumnsForImportTable(self, columnsDF):
#		""" Parquet import format from sqoop cant handle all datatypes correctly. So for the column definition, we need to change some. We also replace <SPACE> with underscore in the column name """
#		columnsDF["type"].replace(["date"], "string", inplace=True)
#		columnsDF["type"].replace(["timestamp"], "string", inplace=True)
#		columnsDF["type"].replace(["decimal(.*)"], "string", regex=True, inplace=True)
#		columnsDF["type"].replace(["bigint"], "string", regex=True, inplace=True)
#
#		# If you change any of the name replace rows, you also need to change the same data in function self.copyHiveTable() and import_definitions.saveColumnData()
#		columnsDF["name"].replace([" "], "_", regex=True, inplace=True)
#		columnsDF["name"].replace(["%"], "pct", regex=True, inplace=True)
#		columnsDF["name"].replace(["\("], "_", regex=True, inplace=True)
#		columnsDF["name"].replace(["\)"], "_", regex=True, inplace=True)
#		columnsDF["name"].replace(["ü"], "u", regex=True, inplace=True)
#		columnsDF["name"].replace(["å"], "a", regex=True, inplace=True)
#		columnsDF["name"].replace(["ä"], "a", regex=True, inplace=True)
#		columnsDF["name"].replace(["ö"], "o", regex=True, inplace=True)
#
#		return columnsDF
#
#	def updatePKonTargetTable(self,):
#		""" Update the PrimaryKey definition on the Target Hive Table """
#		logging.debug("Executing import_operations.updatePKonTargetTable()")
#		self.updatePKonTable(self.import_config.Hive_DB, self.import_config.Hive_Table)
#
#		logging.debug("Executing import_operations.updatePKonTargetTable() - Finished")
#
#	def updatePKonTable(self, hiveDB, hiveTable):
#		""" Update the PrimaryKey definition on a Hive Table """
#		logging.debug("Executing import_operations.updatePKonTable()")
#		if self.import_config.getPKcolumns() == None:
#			logging.info("No Primary Key informaton exists for this table.")
#		else:
#			logging.info("Creating Primary Key on table.")
#			PKonHiveTable = self.common_operations.getPKfromTable(hiveDB, hiveTable)
#			PKinConfigDB = self.import_config.getPKcolumns()
#			self.common_operations.reconnectHiveMetaStore()
#
#			if PKonHiveTable != PKinConfigDB:
#				# The PK information is not correct. We need to figure out what the problem is and alter the table to fix it
#				logging.debug("PK in Hive:   %s"%(PKonHiveTable))
#				logging.debug("PK in Config: %s"%(PKinConfigDB))
#				logging.info("PrimaryKey definition is not correct. If it exists, we will drop it and recreate")
#				logging.info("PrimaryKey on Hive table:     %s"%(PKonHiveTable))
#				logging.info("PrimaryKey from source table: %s"%(PKinConfigDB))
#
#				currentPKname = self.common_operations.getPKname(hiveDB, hiveTable)
#				if currentPKname != None:
#					query  = "alter table `%s`.`%s` "%(self.import_config.Hive_DB, self.import_config.Hive_Table)
#					query += "drop constraint %s"%(currentPKname)
#					self.common_operations.executeHiveQuery(query)
#
#				# There are now no PK on the table. So it's safe to create one
#				PKname = "%s__%s__PK"%(self.import_config.Hive_DB, self.import_config.Hive_Table)
#				query  = "alter table `%s`.`%s` "%(self.import_config.Hive_DB, self.import_config.Hive_Table)
#				query += "add constraint %s primary key ("%(PKname)
#
#				firstColumn = True
#				for column in self.import_config.getPKcolumns().split(","):
#					if firstColumn == False:
#						query += ", " 
#					query += "`" + column + "`" 
#					firstColumn = False
#
#				query += ") DISABLE NOVALIDATE"
#				self.common_operations.executeHiveQuery(query)
#				self.common_operations.reconnectHiveMetaStore()
#
#			else:
#				logging.info("PrimaryKey definition is correct. No change required.")
#
#
#		logging.debug("Executing import_operations.updatePKonTable() - Finished")
#
#	def updateFKonTargetTable(self,):
#		""" Update the ForeignKeys definition on the Target Hive Table """
#		logging.debug("Executing import_operations.updatePKonTargetTable()")
#		self.updateFKonTable(self.import_config.Hive_DB, self.import_config.Hive_Table)
#
#		logging.debug("Executing import_operations.updatePKonTargetTable() - Finished")
#
#	def updateFKonTable(self, hiveDB, hiveTable):
#		""" Update the ForeignKeys definition on a Hive Table """
#		logging.debug("Executing import_operations.updateFKonTable()")
#
#		foreignKeysFromConfig = self.import_config.getForeignKeysFromConfig()
#		foreignKeysFromHive   = self.common_operations.getForeignKeysFromHive(hiveDB, hiveTable)
#
#		if foreignKeysFromConfig.empty == True and foreignKeysFromHive.empty == True:
#			logging.info("No Foreign Keys informaton exists for this table")
#			return
#
#		if foreignKeysFromConfig.equals(foreignKeysFromHive) == True:
#			logging.info("ForeignKey definitions is correct. No change required.")
#		else:
#			logging.info("ForeignKey definitions is not correct. Will check what exists and create/drop required foreign keys.")
#
#			foreignKeysDiff = pd.merge(foreignKeysFromConfig, foreignKeysFromHive, on=None, how='outer', indicator='Exist')
#
#			for index, row in foreignKeysDiff.loc[foreignKeysDiff['Exist'] == 'right_only'].iterrows():
#				# This will iterate over ForeignKeys that only exists in Hive
#				# If it only exists in Hive, we delete it!
#
#				logging.info("Dropping FK in Hive as it doesnt match the FK's in DBImport config database")
#				query = "alter table `%s`.`%s` drop constraint %s"%(hiveDB, hiveTable, row['fk_name'])
#				self.common_operations.executeHiveQuery(query)
#					
#							
#			for index, row in foreignKeysDiff.loc[foreignKeysDiff['Exist'] == 'left_only'].iterrows():
#				# This will iterate over ForeignKeys that only exists in the DBImport configuration database
#				# If it doesnt exist in Hive, we create it
#
#				if self.common_operations.checkHiveTable(row['ref_hive_db'], row['ref_hive_table'], ) == True:
#					query = "alter table `%s`.`%s` add constraint %s foreign key ("%(hiveDB, hiveTable, row['fk_name'])
#					firstLoop = True
#					for column in row['source_column_name'].split(','):
#						if firstLoop == False: query += ", "
#						query += "`%s`"%(column)
#						firstLoop = False
#	
#					query += ") REFERENCES `%s`.`%s` ("%(row['ref_hive_db'], row['ref_hive_table'])
#					firstLoop = True
#					for column in row['ref_column_name'].split(','):
#						if firstLoop == False: query += ", "
#						query += "`%s`"%(column)
#						firstLoop = False
#					query += ") DISABLE NOVALIDATE RELY"
#	
#					logging.info("Creating FK in Hive as it doesnt exist")
#					self.common_operations.executeHiveQuery(query)
#
#			self.common_operations.reconnectHiveMetaStore()
#
##		logging.debug("Executing import_operations.updateFKonTable()  - Finished")
#
##	def removeHiveLocks(self,):
##		self.common_operations.removeHiveLocksByForce(self.Hive_DB, self.Hive_Table)
#
#	def loadDataFromImportToTargetTable(self,):
#		logging.info("Loading data from import table to target table")
#		importTableExists = self.common_operations.checkHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
#		targetTableExists = self.common_operations.checkHiveTable(self.Hive_DB, self.Hive_Table)
#
#		if importTableExists == False:
#			logging.error("The import table %s.%s does not exist"%(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table))
#			self.import_config.remove_temporary_files()
#			sys.exit(1)
#
#		if targetTableExists == False:
#			logging.error("The target table %s.%s does not exist"%(self.Hive_DB, self.Hive_Table))
#			self.import_config.remove_temporary_files()
#			sys.exit(1)
#
#		self.copyHiveTable(	self.import_config.Hive_Import_DB, 
#							self.import_config.Hive_Import_Table, 
#							self.Hive_DB, 
#							self.Hive_Table)
#
#	def resetIncrMaxValue_OLD(self, hiveDB=None, hiveTable=None):
#		""" Will read the Max value from the Hive table and save that into the incr_maxvalue column """
#		logging.debug("Executing import_operations.resetIncrMaxValue()")
#
#		if hiveDB == None: hiveDB = self.Hive_DB
#		if hiveTable == None: hiveTable = self.Hive_Table
#
#		if self.import_config.import_is_incremental == True:
#			self.sqoopIncrNoNewRows = False
#			query = "select max(%s) from `%s`.`%s` "%(self.import_config.sqoop_incr_column, hiveDB, hiveTable)
#
#			self.common_operations.connectToHive(forceSkipTest=True)
#			result_df = self.common_operations.executeHiveQuery(query)
#			maxValue = result_df.loc[0][0]
#
#			if type(maxValue) == pd._libs.tslibs.timestamps.Timestamp:
#		#		maxValue += pd.Timedelta(np.timedelta64(1, 'ms'))
#				(dt, micro) = maxValue.strftime('%Y-%m-%d %H:%M:%S.%f').split(".")
#				maxValue = "%s.%03d" % (dt, int(micro) / 1000)
#			else:
#				maxValue = int(maxValue)
#
#			self.import_config.resetSqoopStatistics(maxValue)
#			self.import_config.clearStage()
#		else:
#			logging.error("This is not an incremental import. Nothing to repair.")
#			self.import_config.remove_temporary_files()
#			sys.exit(1)
#
#		logging.debug("Executing import_operations.resetIncrMaxValue() - Finished")
#
#	def repairAllIncrementalImports(self):
#		logging.debug("Executing import_operations.repairAllIncrementalImports()")
#		result_df = self.import_config.getAllActiveIncrImports()
#
#		# Only used under debug so I dont clear any real imports that is going on
#		tablesRepaired = False
#		for index, row in result_df.loc[result_df['hive_db'] == 'user_boszkk'].iterrows():
##		for index, row in result_df.iterrows():
#			tablesRepaired = True
#			hiveDB = row[0]
#			hiveTable = row[1]
#			logging.info("Repairing table \u001b[33m%s.%s\u001b[0m"%(hiveDB, hiveTable))
#			self.setHiveTable(hiveDB, hiveTable)
##			self.resetIncrMaxValue(hiveDB, hiveTable)
#			logging.info("")
#
#		if tablesRepaired == False:
#			logging.info("\n\u001b[32mNo incremental tables found that could be repaired\u001b[0m\n")
#
####		logging.debug("Executing import_operations.repairAllIncrementalImports() - Finished")
