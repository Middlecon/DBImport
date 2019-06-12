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
from DBImportConfig import import_config
from DBImportOperation import common_operations
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time

class operation(object, metaclass=Singleton):
	def __init__(self, Hive_DB=None, Hive_Table=None):
		logging.debug("Executing etl_operations.__init__()")
		self.Hive_DB = None
		self.Hive_Table = None
		self.mysql_conn = None
		self.mysql_cursor = None
		self.startDate = None

		self.common_operations = common_operations.operation(Hive_DB, Hive_Table)
		self.import_config = import_config.config(Hive_DB, Hive_Table)

		if Hive_DB != None and Hive_Table != None:
			self.setHiveTable(Hive_DB, Hive_Table)
		else:
			# If the class already is initialized, we just pull the parameters and set them here
			self.Hive_DB = self.common_operations.Hive_DB
			self.Hive_Table = self.common_operations.Hive_Table
			self.startDate = self.import_config.startDate

		logging.debug("Executing etl_operations.__init__() - Finished")

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

	def connectToHive(self,):
		logging.debug("Executing etl_operations.connectToHive()")

		try:
			self.common_operations.connectToHive()
		except Exception as ex:
			logging.error(ex)
			self.import_config.remove_temporary_files()
			sys.exit(1)

		logging.debug("Executing etl_operations.connectToHive() - Finished")

	def mergeHiveTables(self, sourceDB, sourceTable, targetDB, targetTable, historyDB = None, historyTable=None, targetDeleteDB = None, targetDeleteTable=None, createHistoryAudit=False, sourceIsIncremental=False, sourceIsImportTable=False, softDelete=False, mergeTime=datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), datalakeSource=None, PKColumns=None, hiveMergeJavaHeap=None, oracleFlashbackSource=False, deleteNotUpdatedRows=False):
		""" Merge source table into Target table. Also populate a History Audit table if selected """
		logging.debug("Executing etl_operations.mergeHiveTables()")

		targetColumns = self.common_operations.getHiveColumns(hiveDB=targetDB, hiveTable=targetTable, includeType=False, includeComment=False)
		columnMerge = self.common_operations.getHiveColumnNameDiff(sourceDB=sourceDB, sourceTable=sourceTable, targetDB=targetDB, targetTable=targetTable, sourceIsImportTable=True)
		if PKColumns == None:
			PKColumns = self.common_operations.getPKfromTable(hiveDB=targetDB, hiveTable=targetTable, quotedColumns=False)

		datalakeIUDExists = False
		datalakeInsertExists = False
		datalakeUpdateExists = False
		datalakeDeleteExists = False
		datalakeSourceExists = False

		for index, row in targetColumns.iterrows():
			if row['name'] == "datalake_iud":    datalakeIUDExists = True 
			if row['name'] == "datalake_insert": datalakeInsertExists = True 
			if row['name'] == "datalake_update": datalakeUpdateExists = True 
			if row['name'] == "datalake_delete": datalakeDeleteExists = True 
			if row['name'] == "datalake_source": datalakeSourceExists = True 

		if hiveMergeJavaHeap != None:
			query = "set hive.tez.container.size=%s"%(hiveMergeJavaHeap)
			self.common_operations.executeHiveQuery(query)


		query  = "merge into `%s`.`%s` as T \n"%(targetDB, targetTable)
		query += "using `%s`.`%s` as S \n"%(sourceDB, sourceTable)
		query += "on \n"

		for i, targetColumn in enumerate(PKColumns.split(",")):
			sourceColumn = columnMerge.loc[columnMerge['Exist'] == 'both'].loc[columnMerge['targetName'] == targetColumn].iloc[0]['sourceName']
			if sourceColumn == None:
				logging.error("ERROR: Problem determine column name in source table for primary key column '%s'"%(targetColumn))
				self.import_config.remove_temporary_files()
				sys.exit(1)

			if i == 0: 
				query += "   T.`%s` = S.`%s` "%(targetColumn, sourceColumn)
			else:
				query += "and\n   T.`%s` = S.`%s` "%(targetColumn, sourceColumn)
		query += "\n"

		query += "when matched "
		if sourceIsIncremental == False:
			# If the source is not incremental, it means that we need to check all the values in 
			# all columns as we dont know if the row have changed or not
			query += "and (\n"
			firstIteration = True
			for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
				foundPKcolumn = False
				for column in PKColumns.split(","):
					if row['targetName'] == column:
						foundPKcolumn = True
				if foundPKcolumn == False:
					if firstIteration == True:
						query += "   "
						firstIteration = False
					else:
						query += "   or "
					query += "T.`%s` != S.`%s` "%(row['targetName'], row['sourceName'])
					query += "or ( T.`%s` is null and S.`%s` is not null ) "%(row['targetName'], row['sourceName'])
					query += "or ( T.`%s` is not null and S.`%s` is null ) "%(row['targetName'], row['sourceName'])
					query += "\n"

			if softDelete == True and datalakeIUDExists == True:
				# If a row is deleted and then inserted again with the same values in all fields, this will still trigger an update
				query += "   or T.datalake_iud = 'D' \n"

			query += ") \n"

		if oracleFlashbackSource == True:
			query += "and ( S.datalake_flashback_operation is null or S.datalake_flashback_operation != 'D' ) \n"

		query += "then update set "
		firstIteration = True
		nonPKcolumnFound = False
		for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
			foundPKcolumn = False
			for column in PKColumns.split(","):
				if row['targetName'] == column:
					foundPKcolumn = True
			if foundPKcolumn == False:
				if firstIteration == True:
					firstIteration = False
					query += " \n"
				else:
					query += ", \n"
				query += "   `%s` = S.`%s`"%(row['targetName'], row['sourceName'])
				nonPKcolumnFound = True

		if nonPKcolumnFound == False:
			# This will happen if there are only columns that is part of the PK in the table. Impossible to merge it with full history
			logging.error("This table only have columns that is part of the PrimaryKey. Merge operations cant be used")
			self.import_config.remove_temporary_files()
			sys.exit(1)


		if datalakeIUDExists == True:    query += ", \n   `datalake_iud` = 'U'"
		if datalakeUpdateExists == True: query += ", \n   `datalake_update` = '%s'"%(mergeTime)
		if datalakeSourceExists == True and datalakeSource != None: query += ", \n   `datalake_source` = '%s'"%(datalakeSource)
		query += " \n"

		if oracleFlashbackSource == True:
			query += "when matched and S.datalake_flashback_operation = 'D' then delete \n"

		query += "when not matched "

		if oracleFlashbackSource == True:
			query += "and ( S.datalake_flashback_operation is null or S.datalake_flashback_operation != 'D' ) \n"

		query += "then insert values ( "
		firstIteration = True
		for index, row in targetColumns.iterrows():
			ColumnName = row['name']
			sourceColumnName = columnMerge.loc[columnMerge['targetName'] == ColumnName]['sourceName'].fillna('').iloc[0]
			if firstIteration == True:
				firstIteration = False
				query += " \n"
			else:
				query += ", \n"
			if sourceColumnName != "":
				query += "   S.`%s`"%(sourceColumnName)
			elif ColumnName == "datalake_iud": 
				query += "   'I'"
			elif ColumnName == "datalake_insert": 
				query += "   '%s'"%(mergeTime)
			elif ColumnName == "datalake_update": 
				query += "   '%s'"%(mergeTime)
			elif ColumnName == "datalake_source": 
				query += "   '%s'"%(datalakeSource)
			else:
				query += "   NULL"

		query += " \n) \n"

#		print("==============================================================")
#		print(query)
#		self.import_config.remove_temporary_files()
#		sys.exit(1)
##		query = query.replace('\n', '')
		self.common_operations.executeHiveQuery(query)

		if deleteNotUpdatedRows == True:
			# This is used by Oracle Flashback imports when doing a reinitialization of the data and we need to 
			# remove the rows that was not updated
			query = "delete from `%s`.`%s` where datalake_update != '%s' "%(targetDB, targetTable, mergeTime)
			self.common_operations.executeHiveQuery(query)


		# If a row was previously deleted and now inserted again and we are using Soft Delete, 
		# then the information in the datalake_iud, datalake_insert and datalake_delete is wrong. 

		if softDelete == True:
			query  = "update `%s`.`%s` set "%(targetDB, targetTable)
			query += "   datalake_iud = 'I', "
			query += "   datalake_insert = datalake_update, "
			query += "   datalake_delete = null "
			query += "where " 
			query += "   datalake_iud = 'U' and "
			query += "   datalake_delete is not null"

#			print("==============================================================")
#			print(query)
#			query = query.replace('\n', '')
			self.common_operations.executeHiveQuery(query)

		# Statement to select all rows that was changed in the Target table and insert them to the History table

		if createHistoryAudit == True and historyDB != None and historyTable != None:
			query  = "insert into table `%s`.`%s` \n"%(historyDB, historyTable) 
			query += "( "
			firstIteration = True
			for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
				if firstIteration == True:
					firstIteration = False
					query += " \n"
				else:
					query += ", \n"
				query += "   `%s`"%(row['targetName'])
			if datalakeSourceExists == True:
				query += ",\n   `datalake_source`"
			query += ",\n   `datalake_iud`"
			query += ",\n   `datalake_timestamp`"
			query += "\n) \n"

			query += "select "
			firstIteration = True
			for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
				if firstIteration == True:
					firstIteration = False
					query += " \n"
				else:
					query += ", \n"
				query += "   `%s`"%(row['targetName'])
			if datalakeSourceExists == True:
				query += ",\n   '%s'"%(datalakeSource)
			query += ",\n   `datalake_iud`"
			query += ",\n   `datalake_update`"
			query += "\nfrom `%s`.`%s` \n"%(targetDB, targetTable)
			query += "where datalake_update = '%s'"%(mergeTime)

#			print("==============================================================")
#			print(query)
#			query = query.replace('\n', '')
			self.common_operations.executeHiveQuery(query)

#		if sourceIsIncremental == False and createHistoryAudit == True and historyDB != None and historyTable != None and historyDB != None and historyTable != None and targetDeleteDB != None and targetDeleteTable != None:
		if sourceIsIncremental == False and targetDeleteDB != None and targetDeleteTable != None:
			# Start with truncating the History Delete table as we need to rebuild this one from scratch to determine what rows are deleted
			query  = "truncate table `%s`.`%s`"%(targetDeleteDB, targetDeleteTable)
			self.common_operations.executeHiveQuery(query)

			# Insert all rows (PK columns only) that exists in the Target Table but dont exists in the Import table (the ones that was deleted)
			query  = "insert into table `%s`.`%s` \n(`"%(targetDeleteDB, targetDeleteTable)
			query += "`, `".join(PKColumns.split(","))
			query += "`) \nselect T.`"
			query += "`, T.`".join(PKColumns.split(","))
			query += "` \nfrom `%s`.`%s` as T \n"%(targetDB, targetTable)
			query += "left outer join `%s`.`%s` as S \n"%(sourceDB, sourceTable)
			query += "on \n"
			for i, targetColumn in enumerate(PKColumns.split(",")):
				sourceColumn = columnMerge.loc[columnMerge['Exist'] == 'both'].loc[columnMerge['targetName'] == targetColumn].iloc[0]['sourceName']

				if i == 0: 
					query += "   T.`%s` = S.`%s` "%(targetColumn, sourceColumn)
				else:
					query += "and\n   T.`%s` = S.`%s` "%(targetColumn, sourceColumn)

			query += "\nwhere \n"

			for i, targetColumn in enumerate(PKColumns.split(",")):
				sourceColumn = columnMerge.loc[columnMerge['Exist'] == 'both'].loc[columnMerge['targetName'] == targetColumn].iloc[0]['sourceName']
				if i == 0: 
					query += "   S.`%s` is null "%(sourceColumn)
				else:
					query += "and\n   S.`%s` is null "%(sourceColumn)

#			print("==============================================================")
#			print(query)
#			query = query.replace('\n', '')
			self.common_operations.executeHiveQuery(query)

			# Insert the deleted rows into the History table. Without this, it's impossible to see what values the column had before the delete
		if sourceIsIncremental == False and createHistoryAudit == True and historyDB != None and historyTable != None and historyDB != None and historyTable != None and targetDeleteDB != None and targetDeleteTable != None:
			query  = "insert into table `%s`.`%s` \n"%(historyDB, historyTable) 
			query += "( "
			firstIteration = True
			for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
				if firstIteration == True:
					firstIteration = False
					query += " \n"
				else:
					query += ", \n"
				query += "   `%s`"%(row['targetName'])
			if datalakeSourceExists == True:
				query += ",\n   `datalake_source`"
			query += ",\n   `datalake_iud`"
			query += ",\n   `datalake_timestamp`"
			query += "\n) \n"

			query += "select "
			firstIteration = True
			for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
				if firstIteration == True:
					firstIteration = False
					query += " \n"
				else:
					query += ", \n"
				query += "   T.`%s`"%(row['targetName'])
			if datalakeSourceExists == True:
				query += ",\n   '%s' as `datalake_source`"%(datalakeSource)
			query += ",\n   'D' as `datalake_iud`"
			query += ",\n   timestamp('%s') as `datalake_timestamp`"%(mergeTime)
			query += "\nfrom `%s`.`%s` as D \n"%(targetDeleteDB, targetDeleteTable)
			query += "left join `%s`.`%s` as T \n"%(targetDB, targetTable)
			query += "on \n"
			for i, column in enumerate(PKColumns.split(",")):
				if i == 0: 
					query += "   T.`%s` = D.`%s` "%(column, column)
				else:
					query += "and\n   T.`%s` = D.`%s` "%(column, column)

#			print("==============================================================")
#			print(query)
#			query = query.replace('\n', '')
			self.common_operations.executeHiveQuery(query)

		if sourceIsIncremental == False and targetDeleteDB != None and targetDeleteTable != None:
			# Use the merge command to delete found rows between the Delete Table and the History Table
			query  = "merge into `%s`.`%s` as T \n"%(targetDB, targetTable)
			query += "using `%s`.`%s` as D \n"%(targetDeleteDB, targetDeleteTable)
			query += "on \n"
	
			for i, column in enumerate(PKColumns.split(",")):
				if i == 0: 
					query += "   T.`%s` = D.`%s` "%(column, column)
				else:
					query += "and\n   T.`%s` = D.`%s` "%(column, column)
			if softDelete == True:
				query += "and\n   T.`datalake_delete` != 'D' "
			query += "\n"

			if softDelete == False:
				query += "when matched then delete \n"
			else:
				query += "when matched then update set \n" 
				query += "datalake_iud  = 'D', \n" 
				query += "datalake_update = timestamp('%s'), \n"%(mergeTime)
				query += "datalake_delete = timestamp('%s') "%(mergeTime)

#			print("==============================================================")
#			print(query)
#			query = query.replace('\n', '')
			self.common_operations.executeHiveQuery(query)

#		query  = "alter table `%s`.`%s` compact 'major'"%(targetDB, targetTable)
#		self.common_operations.executeHiveQuery(query)

		logging.debug("Executing etl_operations.mergeHiveTables() - Finished")













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
#
#			query += "stored as parquet "
##			query += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' "
##			query += "LINES TERMINATED BY '\002' "
#			query += "LOCATION '%s%s/' "%(self.import_config.common_config.hdfs_address, self.import_config.sqoop_hdfs_location)
#			query += "TBLPROPERTIES('parquet.compress' = 'SNAPPY') "
##			query += "TBLPROPERTIES('serialization.null.format' = '\\\\N') "
##			query += "TBLPROPERTIES('serialization.null.format' = '\003') "
#
#			self.common_operations.executeHiveQuery(query)
#			self.common_operations.reconnectHiveMetaStore()
#
#		logging.debug("Executing import_operations.createExternalTable() - Finished")
#
#	def createTargetTable(self):
#		logging.debug("Executing import_operations.createTargetTable()")
#		if self.common_operations.checkHiveTable(self.Hive_DB, self.Hive_Table) == True:
#			# Table exist, so we need to make sure that it's a managed table and not an external table
#			if self.common_operations.isHiveTableExternal(self.Hive_DB, self.Hive_Table) == True:
#				self.common_operations.dropHiveTable(self.Hive_DB, self.Hive_Table)
#
#		# We need to check again as the table might just been droped because it was an external table to begin with
#		if self.common_operations.checkHiveTable(self.Hive_DB, self.Hive_Table) == False:
#			# Target table does not exist. We just create it in that case
#			logging.info("Creating target table %s.%s in Hive"%(self.Hive_DB, self.Hive_Table))
#
#			query  = "create table `%s`.`%s` ("%(self.import_config.Hive_DB, self.import_config.Hive_Table)
#			columnsDF = self.import_config.getColumnsFromConfigDatabase() 
#
#			firstLoop = True
#			for index, row in columnsDF.iterrows():
#				if firstLoop == False: query += ", "
#				query += "`%s` %s"%(row['name'], row['type'])
#				if row['comment'] != None:
#					query += " COMMENT \"%s\""%(row['comment'])
#				firstLoop = False
#
#			if self.import_config.datalake_source != None:
#				query += ", datalake_source varchar(256)"
#
#			if self.import_config.create_datalake_import_column == True:
#				query += ", datalake_import timestamp COMMENT \"Import time from source database\""
#
#			query += ") "
#
#			tableComment = self.import_config.getHiveTableComment()
#			if tableComment != None:
#				query += "COMMENT \"%s\" "%(tableComment)
#
#			if self.import_config.create_table_with_acid == False:
#				query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB') "
#			else:
#				query += "CLUSTERED BY ("
#				firstColumn = True
#				for column in self.import_config.getPKcolumns().split(","):
#					if firstColumn == False:
#						query += ", " 
#					query += "`" + column + "`" 
#					firstColumn = False
#				query += ") into 4 buckets "
#				query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true') "
#
#			self.common_operations.executeHiveQuery(query)
#
#			self.common_operations.reconnectHiveMetaStore()
#
#		logging.debug("Executing import_operations.createTargetTable() - Finished")
#		
#	def updateTargetTable(self):
#		self.updateHiveTable(self.Hive_DB, self.Hive_Table)
#
#	def updateExternalImportTable(self):
#		self.updateHiveTable(self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table)
#
#	def updateColumnsForImportTable(self, columnsDF):
#		""" Parquet import format from sqoop cant handle all datatypes correctly. So for the column definition, we need to change some. We also replace <SPACE> with underscore in the column name """
#		columnsDF["type"].replace(["date"], "string", inplace=True)
#		columnsDF["type"].replace(["timestamp"], "string", inplace=True)
#		columnsDF["type"].replace(["decimal(.*)"], "string", regex=True, inplace=True)
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
#	def updateHiveTable(self, hiveDB, hiveTable):
#		""" Update the target table based on the column information in the configuration database """
#		# TODO: If there are less columns in the source table together with a rename of a column, then it wont work. Needs to be handled
#		logging.debug("Executing import_operations.updateTargetTable()")
#		logging.info("Updating Hive table columns based on source system schema")
#		columnsConfig = self.import_config.getColumnsFromConfigDatabase() 
#		columnsHive   = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 
#
#		# If we are working on the import table, we need to change some column types to handle Parquet files
#		if hiveDB == self.import_config.Hive_Import_DB and hiveTable == self.import_config.Hive_Import_Table:
#			columnsConfig = self.updateColumnsForImportTable(columnsConfig)
#
#		# Check for missing columns
#		columnsConfigOnlyName = columnsConfig.filter(['name'])
#		columnsHiveOnlyName = columnsHive.filter(['name'])
#		columnsMergeOnlyName = pd.merge(columnsConfigOnlyName, columnsHiveOnlyName, on=None, how='outer', indicator='Exist')
#
#		columnsConfigCount = len(columnsConfigOnlyName)
#		columnsHiveCount   = len(columnsHiveOnlyName)
#		columnsMergeLeftOnlyCount  = len(columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'])
#		columnsMergeRightOnlyCount = len(columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'right_only'])
#
#		logging.debug("columnsConfigOnlyName")
#		logging.debug(columnsConfigOnlyName)
#		logging.debug("================================================================")
#		logging.debug("columnsHiveOnlyName")
#		logging.debug(columnsHiveOnlyName)
#		logging.debug("================================================================")
#		logging.debug("columnsMergeOnlyName")
#		logging.debug(columnsMergeOnlyName)
#		logging.debug("")
#
#		if columnsConfigCount == columnsHiveCount and columnsMergeLeftOnlyCount > 0:
#			# The number of columns in config and Hive is the same, but there is a difference in name. This is most likely because of a rename of one or more of the columns
#			# To handle this, we try a rename. This might fail if the column types are also changed to an incompatable type
#			# The logic here is to 
#			# 1. get all columns from mergeDF that exists in Left_Only
#			# 2. Get the position in configDF with that column name
#			# 3. Get the column in the same position from HiveDF
#			# 4. Check if that column name exists in the mergeDF with Right_Only. If it does, the column was just renamed
#			for index, row in columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iterrows():
#				rowInConfig = columnsConfig.loc[columnsConfig['name'] == row['name']].iloc[0]
#				indexInConfig = columnsConfig.loc[columnsConfig['name'] == row['name']].index.item()
#				rowInHive = columnsHive.iloc[indexInConfig]
#				
#				if len(columnsMergeOnlyName.loc[(columnsMergeOnlyName['Exist'] == 'right_only') & (columnsMergeOnlyName['name'] == rowInHive["name"])]) > 0: 
#					# This is executed if the column is renamed and exists in the same position
#					logging.debug("Name in config:  %s"%(rowInConfig["name"]))
#					logging.debug("Type in config:  %s"%(rowInConfig["type"]))
#					logging.debug("Index in config: %s"%(indexInConfig))
#					logging.debug("--------------------")
#					logging.debug("Name in Hive: %s"%(rowInHive["name"]))
#					logging.debug("Type in Hive: %s"%(rowInHive["type"]))
#					logging.debug("======================================")
#					logging.debug("")
##	
#					query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, rowInHive['name'], rowInConfig['name'], rowInConfig['type'])
#					self.common_operations.executeHiveQuery(query)
#
#					self.import_config.logHiveColumnRename(rowInConfig['name'], rowInHive["name"], hiveDB=hiveDB, hiveTable=hiveTable)
#				
#					if rowInConfig["type"] != rowInHive["type"]:
#						self.import_config.logHiveColumnTypeChange(rowInConfig['name'], rowInConfig['type'], previous_columnType=rowInHive["type"], hiveDB=hiveDB, hiveTable=hiveTable) 
#				else:
#					if columnsMergeLeftOnlyCount == 1 and columnsMergeRightOnlyCount == 1:
#						# So the columns are not in the same position, but it's only one column that changed. In that case, we just rename that one column
#						rowInMergeLeft  = columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iloc[0]
#						rowInMergeRight = columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'right_only'].iloc[0]
#						rowInConfig = columnsConfig.loc[columnsConfig['name'] == rowInMergeLeft['name']].iloc[0]
#						rowInHive = columnsHive.loc[columnsHive['name'] == rowInMergeRight['name']].iloc[0]
#						print(rowInConfig["name"])
#						print(rowInConfig["type"])
#						print("--------------------")
#						print(rowInHive["name"])
#						print(rowInHive["type"])
#
#						query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, rowInHive['name'], rowInConfig['name'], rowInHive['type'])
#						self.common_operations.executeHiveQuery(query)
#
#						self.import_config.logHiveColumnRename(rowInConfig['name'], rowInHive["name"], hiveDB=hiveDB, hiveTable=hiveTable)
#
#			self.common_operations.reconnectHiveMetaStore()
#			columnsHive   = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 
#			columnsHiveOnlyName = columnsHive.filter(['name'])
#			columnsMergeOnlyName = pd.merge(columnsConfigOnlyName, columnsHiveOnlyName, on=None, how='outer', indicator='Exist')
#
#		for index, row in columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iterrows():
#			# This will iterate over columns that only exists in the config and not in Hive. We add these to Hive
#			fullRow = columnsConfig.loc[columnsConfig['name'] == row['name']].iloc[0]
#			query = "alter table `%s`.`%s` add columns (`%s` %s"%(hiveDB, hiveTable, fullRow['name'], fullRow['type'])
#			if fullRow['comment'] != None:
#				query += " COMMENT \"%s\""%(fullRow['comment'])
#			query += ")"
#
#			self.common_operations.executeHiveQuery(query)
#
#			self.import_config.logHiveColumnAdd(fullRow['name'], columnType=fullRow['type'], hiveDB=hiveDB, hiveTable=hiveTable) 
#
#		# Check for changed column types
#		self.common_operations.reconnectHiveMetaStore()
#		columnsHive = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 
#
#		columnsConfigOnlyNameType = columnsConfig.filter(['name', 'type']).sort_values(by=['name'], ascending=True)
#		columnsHiveOnlyNameType = columnsHive.filter(['name', 'type']).sort_values(by=['name'], ascending=True)
#		columnsMergeOnlyNameType = pd.merge(columnsConfigOnlyNameType, columnsHiveOnlyNameType, on=None, how='outer', indicator='Exist')
#
#		logging.debug("columnsConfigOnlyNameType")
#		logging.debug(columnsConfigOnlyNameType)
#		logging.debug("================================================================")
#		logging.debug("columnsHiveOnlyNameType")
#		logging.debug(columnsHiveOnlyNameType)
#		logging.debug("================================================================")
#		logging.debug("columnsMergeOnlyNameType")
#		logging.debug(columnsMergeOnlyNameType)
#		logging.debug("")
#
#		for index, row in columnsMergeOnlyNameType.loc[columnsMergeOnlyNameType['Exist'] == 'left_only'].iterrows():
#			# This will iterate over columns that had the type changed from the source
#			query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, row['name'], row['name'], row['type'])
#			self.common_operations.executeHiveQuery(query)
#
#			# Get the previous column type from the Pandas DF with right_only in Exist column
#			previous_columnType = (columnsMergeOnlyNameType.loc[
#				(columnsMergeOnlyNameType['name'] == row['name']) &
#				(columnsMergeOnlyNameType['Exist'] == 'right_only')]
##				).reset_index().at[0, 'type']
##
#			self.import_config.logHiveColumnTypeChange(row['name'], columnType=row['type'], previous_columnType=previous_columnType, hiveDB=hiveDB, hiveTable=hiveTable) 
#
#		# Check for change column comments
#		self.common_operations.reconnectHiveMetaStore()
#		columnsHive = self.common_operations.getColumnsFromHiveTable(hiveDB, hiveTable, excludeDataLakeColumns=True) 
#		columnsHive['comment'].replace('', None, inplace = True)		# Replace blank column comments with None as it would otherwise trigger an alter table on every run
#		columnsMerge = pd.merge(columnsConfig, columnsHive, on=None, how='outer', indicator='Exist')
#		for index, row in columnsMerge.loc[columnsMerge['Exist'] == 'left_only'].iterrows():
#			if row['comment'] == None: row['comment'] = ""
#			query = "alter table `%s`.`%s` change column `%s` `%s` %s comment \"%s\""%(hiveDB, hiveTable, row['name'], row['name'], row['type'], row['comment'])
#
#			self.common_operations.executeHiveQuery(query)
#
#		logging.debug("Executing import_operations.updateTargetTable() - Finished")
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
#
#				# self.common_operations.executeHiveQuery(query)
#
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
##					query += ") DISABLE NOVALIDATE RELY"
#	
#					logging.info("Creating FK in Hive as it doesnt exist")
#					self.common_operations.executeHiveQuery(query)
#
#		logging.debug("Executing import_operations.updateFKonTable()  - Finished")
#
#	def removeHiveLocks(self,):
#		self.common_operations.removeHiveLocksByForce(self.Hive_DB, self.Hive_Table)
#
#	def truncateTargetTable(self,):
#		logging.info("Truncating Target table in Hive")
#		self.common_operations.truncateHiveTable(self.Hive_DB, self.Hive_Table)
#
#	def updateStatisticsOnTargetTable(self,):
#		logging.info("Updating the Hive statistics on the target table")
#		self.common_operations.updateHiveTableStatistics(self.Hive_DB, self.Hive_Table)
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
#	def copyHiveTable(self, sourceDB, sourceTable, targetDB, targetTable):
#		""" Copy one Hive table into another for the columns that have the same name """
#		logging.debug("Executing import_definitions.copyHiveTable()")
#
#		# Logic here is to create a new column in both DF and call them sourceCol vs targetCol. These are the original names. Then we replace the values in targetColumn DF column col
#		# with the name that the column should be called in the source system. This is needed to handle characters that is not supported in Parquet files, like SPACE
#		sourceColumns = self.common_operations.getHiveColumns(sourceDB, sourceTable)
#		sourceColumns['sourceCol'] = sourceColumns['col']
#
#		targetColumns = self.common_operations.getHiveColumns(targetDB, targetTable)
#		targetColumns['targetCol'] = targetColumns['col']
#		# If you change any of the name replace operations, you also need to change the same data in function self.updateColumnsForImportTable() and import_definitions.saveColumnData()
#		targetColumns['col'] = targetColumns['col'].str.replace(r' ', '_')
#		targetColumns['col'] = targetColumns['col'].str.replace(r'\%', 'pct')
#		targetColumns['col'] = targetColumns['col'].str.replace(r'\(', '_')
#		targetColumns['col'] = targetColumns['col'].str.replace(r'\)', '_')
#		targetColumns['col'] = targetColumns['col'].str.replace(r'ü', 'u')
#		targetColumns['col'] = targetColumns['col'].str.replace(r'å', 'a')
#		targetColumns['col'] = targetColumns['col'].str.replace(r'ä', 'a')
#		targetColumns['col'] = targetColumns['col'].str.replace(r'ö', 'o')
#
#		columnMerge = pd.merge(sourceColumns, targetColumns, on=None, how='outer', indicator='Exist')
#		logging.debug("\n%s"%(columnMerge))
#
##		print(columnMerge)
##		self.import_config.remove_temporary_files()
#		sys.exit(1)
#
#		firstLoop = True
#		columnDefinitionSource = ""
#		columnDefinitionTarget = ""
#		for index, row in columnMerge.loc[columnMerge['Exist'] == 'both'].iterrows():
#			if firstLoop == False: columnDefinitionSource += ", "
#			if firstLoop == False: columnDefinitionTarget += ", "
#			columnDefinitionSource += "`%s`"%(row['sourceCol'])
#			columnDefinitionTarget += "`%s`"%(row['targetCol'])
#			firstLoop = False
#
#		query = "insert into `%s`.`%s` ("%(targetDB, targetTable)
#		query += columnDefinitionTarget
#		if self.import_config.datalake_source != None:
#			query += ", datalake_source"
#		if self.import_config.create_datalake_import_column == True:
#			query += ", datalake_import"
#
#		query += ") select "
#		query += columnDefinitionSource
#		if self.import_config.datalake_source != None:
#			query += ", '%s'"%(self.import_config.datalake_source)
#		if self.import_config.create_datalake_import_column == True:
#			query += ", '%s'"%(self.import_config.sqoop_last_execution_timestamp)
#
#		query += " from `%s`.`%s` "%(sourceDB, sourceTable)
#		if self.import_config.nomerge_ingestion_sql_addition != None:
#			query += self.import_config.nomerge_ingestion_sql_addition
#
#		self.common_operations.executeHiveQuery(query)
#		logging.debug("Executing import_definitions.copyHiveTable() - Finished")
#
#	def resetIncrMaxValue(self, hiveDB=None, hiveTable=None):
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
##		else:
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
#			self.resetIncrMaxValue(hiveDB, hiveTable)
#			print("")
#
#		if tablesRepaired == False:
#			print("\n\u001b[32mNo incremental tables found that could be repaired\u001b[0m\n")
#
####		logging.debug("Executing import_operations.repairAllIncrementalImports() - Finished")
