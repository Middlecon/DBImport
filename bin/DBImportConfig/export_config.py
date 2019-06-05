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
import json
import math
from ConfigReader import configuration
import mysql.connector
from common.Singleton import Singleton 
from common import constants as constant
from common.Exceptions import *
from DBImportConfig import common_config
from DBImportConfig import rest 
from DBImportConfig import export_stage
from mysql.connector import errorcode
from datetime import datetime
import pandas as pd

class config(object, metaclass=Singleton):
	def __init__(self, connectionAlias=None, targetSchema=None, targetTable=None):
		logging.debug("Executing export_config.__init__()")
		self.mysql_conn = None
		self.mysql_cursor01 = None
		self.startDate = None
		self.common_config = None

		self.connectionAlias = connectionAlias
		self.targetSchema = targetSchema
		self.targetTable = targetTable

		self.common_config = common_config.config()
#		self.rest = rest.restInterface()

		self.startDate = self.common_config.startDate
		self.mysql_conn = self.common_config.mysql_conn
		self.mysql_cursor01 = self.mysql_conn.cursor(buffered=True)
		self.mysql_cursor02 = self.mysql_conn.cursor(buffered=True)

		self.tableID = None
		self.exportType = None
		self.hiveDB = None
		self.hiveTable = None
		self.exportPhase = None
		self.exportPhaseDescription = None
		self.datalakeSourceConnection = None
		self.exportIsIncremental = None
		self.validateExport = None

		self.tempTableNeeded = None

		self.sqoop_mapcolumnjava = None
		self.sqoop_last_mappers = None
		self.sqoop_last_size = None
		self.sqoop_last_rows = None
		self.incr_column = None
		self.incr_maxvalue = None
		self.incr_validation_method = None
		self.truncateTargetTable = None
		self.sqlSessions = None
		self.sqoopMappers = None
		self.generatedSqoopOptions = None

		# Initialize the stage class that will handle all stage operations for us
		self.stage = export_stage.stage(self.mysql_conn, connectionAlias=connectionAlias, targetSchema=targetSchema, targetTable=targetTable)
		
		logging.debug("Executing export_config.__init__() - Finished")

#	def setHiveTable(self, hiveDB, hiveTable):
#		""" Sets the parameters to work against a new Hive database and table """
#		self.hiveDB = hiveDB.lower()
#		self.hiveTable = hiveTable.lower()
#
#		self.common_config.setHiveTable(hiveDB, hiveTable)
#		self.stage.setHiveTable(hiveDB, hiveTable)

	def logHiveColumnAdd(self, column, columnType, description=None, hiveDB=None, hiveTable=None):
		self.common_config.logHiveColumnAdd(column=column, columnType=columnType, description=description, hiveDB=hiveDB, hiveTable=hiveTable) 

	def logHiveColumnTypeChange(self, column, columnType, previous_columnType=None, description=None, hiveDB=None, hiveTable=None):
		self.common_config.logHiveColumnTypeChange(column, columnType, previous_columnType=previous_columnType, description=description, hiveDB=hiveDB, hiveTable=hiveTable)

	def logHiveColumnRename(self, columnName, previous_columnName, description=None, hiveDB=None, hiveTable=None):
		self.common_config.logHiveColumnRename(columnName, previous_columnName, description=description, hiveDB=hiveDB, hiveTable=hiveTable)

	def logTargetColumnAdd(self, column, columnType, description=None, dbAlias=None, database=None, schema=None, table=None):
		self.common_config.logJDBCColumnAdd(column=column, columnType=columnType, description=description, dbAlias=dbAlias, database=database, schema=schema, table=table)

	def logTargetColumnTypeChange(self, column, columnType, previous_columnType=None, description=None, dbAlias=None, database=None, schema=None, table=None):
		self.common_config.logJDBCColumnTypeChange(column, columnType, previous_columnType=previous_columnType, description=description, dbAlias=dbAlias, database=database, schema=schema, table=table)

	def logTargetColumnRename(self, columnName, previous_columnName, description=None, dbAlias=None, database=None, schema=None, table=None):
		self.common_config.logJDBCColumnRename(columnName, previous_columnName, description=description, dbAlias=dbAlias, database=database, schema=schema, table=table)

	def remove_temporary_files(self):
		self.common_config.remove_temporary_files()

	def setStage(self, stage, force=False):
		self.stage.setStage(stage, force=force)

	def getStage(self):
		return self.stage.getStage()

	def clearStage(self):
		self.stage.clearStage()

	def saveRetryAttempt(self, stage):
		self.stage.saveRetryAttempt(stage)

	def setStageOnlyInMemory(self):
		self.stage.setStageOnlyInMemory()

	def saveStageStatistics(self):
		self.stage.saveStageStatistics(
			dbalias = self.connectionAlias,
			target_database=self.common_config.jdbc_database,
			target_schema = self.targetSchema,
			target_table = self.targetTable,
			hive_db = self.hiveDB,
			hive_table = self.hiveTable,
			export_phase = self.exportPhase,
			incremental = self.exportIsIncremental,
			size=self.sqoop_last_size,
			rows=self.sqoop_last_rows,
			sessions=self.sqoop_last_mappers
		)
#
#	def convertStageStatisticsToJSON(self):
#		self.stage.convertStageStatisticsToJSON(
#			hive_db=self.Hive_DB, 
#			hive_table=self.Hive_Table, 
#			import_phase=self.importPhase, 
#			copy_phase=self.copyPhase, 
#			etl_phase=self.etlPhase, 
#			incremental=self.import_is_incremental,
#			source_database=self.common_config.jdbc_database,
#			source_schema=self.source_schema,
#			source_table=self.source_table,
#			sqoop_size=self.sqoop_last_size,
#			sqoop_rows=self.sqoop_last_rows,
#			sqoop_mappers=self.sqoop_last_mappers
#		)
#
#	def lookupConnectionAlias(self):
#		self.common_config.lookupConnectionAlias(self.connection_alias)
#
	def checkTimeWindow(self):
		self.common_config.checkTimeWindow(self.connectionAlias)

#	def getJDBCTableDefinition(self):
#		self.common_config.getJDBCTableDefinition(self.source_schema, self.source_table)
	def checkJDBCTable(self):
		return self.common_config.checkJDBCTable(schema=self.targetSchema, table=self.targetTable)

	def getExportConfig(self):
		logging.debug("Executing export_config.getExportConfig()")
	
		query  = "select "
		query += "    table_id, "
		query += "    export_type, "
		query += "    hive_db, "
		query += "    hive_table, "
		query += "    validate_export, "
		query += "    sqoop_last_size, "
		query += "    sqoop_last_rows, "
		query += "    sqoop_last_mappers, "
		query += "    incr_column, "
		query += "    incr_maxvalue, "
		query += "    truncate_target, "
		query += "    incr_validation_method "
		query += "from export_tables "
		query += "where "
		query += "    dbalias = %s " 
		query += "    and target_schema = %s "
		query += "    and target_table = %s "
	
		self.mysql_cursor01.execute(query, (self.connectionAlias, self.targetSchema, self.targetTable))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		if self.mysql_cursor01.rowcount != 1:
			raise invalidConfiguration("Error: The specified Hive Database and Table can't be found in the configuration database. Please check that you specified the correct database and table and that the configuration exists in the configuration database")

		row = self.mysql_cursor01.fetchone()

		self.tableID = row[0]
		self.exportType = row[1]
		self.hiveDB = row[2]
		self.hiveTable = row[3]
		self.validateExport = row[4]
		self.sqoop_last_size = row[5]
		self.sqoop_last_rows = row[6]
		self.sqoop_last_mappers = row[7]
		self.incr_column = row[8]
		self.incr_maxvalue = row[9]
		truncate_target_table = row[10]
		self.incr_validation_method = row[11]

		if self.validateExport == 0:
			self.validateExport = False
		else:
			self.validateExport = True

		if truncate_target_table == 1:
			self.truncateTargetTable = True
		else:
			self.truncateTargetTable = False

		if self.incr_validation_method != "full" and self.incr_validation_method != "incr":
			raise invalidConfiguration("Only the values 'full' or 'incr' is valid for column incr_validation_method in export_tables.")

		if self.exportType == "full":
			self.exportPhase             = constant.EXPORT_PHASE_FULL
			self.exportPhaseDescription  = "Full Export"

		if self.exportType == "incr":
			self.exportPhase             = constant.EXPORT_PHASE_INCR
			self.exportPhaseDescription  = "Incremental Export"

		if self.exportPhase == None:
			raise invalidConfiguration("Import type '%s' is not a valid type. Please check configuration"%(self.import_type))

		if self.exportPhase == constant.EXPORT_PHASE_INCR:
			self.exportIsIncremental = True
			if self.incr_column == None:
				raise invalidConfiguration("Incremental import requires a column configured in the 'incr_column' column")
		else:
			self.exportIsIncremental = False

		# Fetch data from jdbc_connection table
		query = "select datalake_source from jdbc_connections where dbalias = %s "
		self.mysql_cursor01.execute(query, (self.connectionAlias, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		if self.mysql_cursor01.rowcount != 1:
			raise invalidConfiguration("There is no JDBC Connection defined with the alias '%s'"%(self.connectionAlias))

		row = self.mysql_cursor01.fetchone()

		self.datalakeSourceConnection = row[0]

#		# Set the name of the history tables, temporary tables and such
#		self.hiveExportTempDB = "etl_export_staging"
		self.hiveExportTempDB = self.common_config.getConfigValue(key = "export_staging_database")
		if self.targetSchema == "-":
			self.hiveExportTempTable = self.connectionAlias.replace('-', '_') + "__" + self.targetTable + "__exporttemptable"
		else:
			self.hiveExportTempTable = self.connectionAlias.replace('-', '_') + "__" + self.targetSchema + "__" + self.targetTable + "__exporttemptable"
		self.hiveExportTempTable = self.hiveExportTempTable.lower()
#		self.Hive_History_DB = self.Hive_DB
#		self.Hive_History_Table = self.Hive_Table + "_history"
#		self.Hive_HistoryTemp_DB = "etl_import_staging"
#		self.Hive_HistoryTemp_Table = self.Hive_DB + "__" + self.Hive_Table + "__temporary"
#		self.Hive_Import_PKonly_DB = "etl_import_staging"
#		self.Hive_Import_PKonly_Table = self.Hive_DB + "__" + self.Hive_Table + "__pkonly__staging"
#		self.Hive_Delete_DB = "etl_import_staging"
#		self.Hive_Delete_Table = self.Hive_DB + "__" + self.Hive_Table + "__pkonly__deleted"

		logging.debug("Settings from export_config.getExportConfig()")
		logging.debug("    tableID = %s"%(self.tableID))
		logging.debug("    exportType = %s"%(self.exportType))
		logging.debug("    hiveDB = %s"%(self.hiveDB))
		logging.debug("    hiveTable = %s"%(self.hiveTable))
		logging.debug("    exportIsIncremental = %s"%(self.exportIsIncremental))
		logging.debug("    datalakeSourceConnection = %s"%(self.datalakeSourceConnection))
		logging.debug("    exportPhaseDescription = %s"%(self.exportPhaseDescription))
		logging.debug("    exportPhase = %s"%(self.exportPhase))
		logging.debug("    hiveExportTempDB = %s"%(self.hiveExportTempDB))
		logging.debug("    hiveExportTempTable = %s"%(self.hiveExportTempTable))
		logging.debug("Executing export_config.getExportConfig() - Finished")

	def updateLastUpdateFromHive(self):
		# This function will update the import_tables.last_update_from_source to the startDate from the common class. This will later
		# be used to determine if all the columns exists in the source system as we set the same datatime on all columns
		# that we read from the source system
		logging.debug("")
		logging.debug("Executing export_config.updateLastUpdateFromHive()")

        # Update the import_tables.last_update_from_source with the current date
		query = "update export_tables set last_update_from_hive = %s where table_id = %s "
		self.mysql_cursor01.execute(query, (self.startDate, self.tableID))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		self.mysql_conn.commit()

		logging.debug("Executing export_config.updateLastUpdateFromHive() - Finished")

	def saveColumnData(self, columnsDF=None):
		logging.debug("Executing export_config.saveColumnData()")
		logging.info("Saving column data to MySQL table - export_columns")
	
		for index, row in columnsDF.iterrows():
			columnName = row['name']
			columnType = row['type']
			columnComment = row['comment']
			columnOrder = row['idx']

			query = "select column_id from export_columns where table_id = %s and column_name = %s "
			self.mysql_cursor01.execute(query, (self.tableID, columnName))
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

			row = self.mysql_cursor01.fetchone()
			if row != None:
				columnID = row[0]
			else:
				columnID = None

			if columnID == None:
				query = ("insert into export_columns "
						"( "
						"    table_id, "
						"    column_name, "
						"    column_type, "
						"    column_order, "
						"    hive_db, "
						"    hive_table, "
						"    last_update_from_hive, "
						"    comment "
						") values ( %s, %s, %s, %s, %s, %s, %s, %s )")
	
				self.mysql_cursor01.execute(query, (self.tableID, columnName, columnType, columnOrder, self.hiveDB, self.hiveTable, self.startDate, columnComment))
			else:
				query = ("update export_columns set "
						"    column_type = %s, "
						"    column_order = %s, "
						"    hive_db = %s, "
						"    hive_table = %s, "
						"    last_update_from_hive = %s, "
						"    comment = %s "
						"where column_id = %s ")

				self.mysql_cursor01.execute(query, (columnType, columnOrder, self.hiveDB, self.hiveTable, self.startDate, columnComment, columnID))

			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
			self.mysql_conn.commit()

		
		logging.debug("Executing export_config.saveColumnData() - Finished")

	def clearTableRowCount(self):
		logging.debug("Executing export_config.clearTableRowCount()")
		logging.info("Clearing rowcounts from previous exports")

		query = ("update export_tables set hive_rowcount = NULL, target_rowcount = NULL where table_id = %s")
		self.mysql_cursor01.execute(query, (self.tableID, ))
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		logging.debug("Executing export_config.clearTableRowCount() - Finished")

	def isExportTempTableNeeded(self, hiveTableIsTransactional, hiveTableIsView):
		""" This function return True or False if a temptable is needed to export from """
		logging.debug("Executing export_config.isExportTempTableNeeded()")
		
		self.tempTableNeeded = False
		
		if hiveTableIsTransactional == True or hiveTableIsView == True or self.exportIsIncremental == True:
			self.tempTableNeeded = True

		query  = "select count(column_id) from export_columns "
		query += "where table_id = %s and target_column_name is not null"
		self.mysql_cursor01.execute(query, (self.tableID, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		row = self.mysql_cursor01.fetchone()
		if row[0] > 0:
			self.tempTableNeeded = True

		logging.debug("tempTableNeeded: %s"%(self.tempTableNeeded))
		logging.debug("Executing export_config.isExportTempTableNeeded() - Finished")
		return self.tempTableNeeded

	def resetMaxValueFromTarget(self):
		logging.debug("Executing export_config.resetMaxValueFromTarget()")

		columQuotation = self.common_config.getQuoteAroundColumn()
		column = self.incr_column
		if self.common_config.getJDBCUpperCase() == True:
			column = column.upper()
		query  = "select max(%s%s%s) from "%(columQuotation, column, columQuotation)
		query += self.common_config.getJDBCsqlFromTable(schema=self.targetSchema, table=self.targetTable)
		resultDF = self.common_config.executeJDBCquery(query)
		maxValue = maxValue = resultDF.loc[0][0]
		self.resetIncrMinMaxValues(maxValue=maxValue)

		logging.debug("Executing export_config.resetMaxValueFromTarget() - Finished")


	def getColumnsFromConfigDatabase(self, excludeColumns=False, forceColumnUppercase=False):
		""" Reads the columns from the configuration database and returns the information in a Pandas DF with the columns name, type and comment """
		logging.debug("Executing export_config.getColumnsFromConfigDatabase()")
		hiveColumnDefinition = ""

		query  = "select "
		if forceColumnUppercase == False:
			query += "	c.column_name as hiveColumnName, "
			query += "	c.target_column_name as targetColumnName, "
		else:
			query += "	upper(c.column_name) as hiveColumnName, "
			query += "	upper(c.target_column_name) as targetColumnName, "
#		query += "	c.column_type as columnType, " 
		query += "	if (c.target_column_type is not null and trim(c.target_column_type) != '', c.target_column_type, c.column_type) as columnType, "
		query += " 	c.comment as comment "
		query += "from export_tables t " 
		query += "join export_columns c on t.table_id = c.table_id "
		query += "where t.table_id = %s and t.last_update_from_hive = c.last_update_from_hive "
		if excludeColumns == True:
			query += "   and c.include_in_export = 1 "
		query += "order by c.column_order"

		self.mysql_cursor01.execute(query, (self.tableID, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		result_df = pd.DataFrame(self.mysql_cursor01.fetchall())

		# Set the correct column namnes in the DataFrame
		result_df_columns = []
		for columns in self.mysql_cursor01.description:
			result_df_columns.append(columns[0])    # Name of the column is in the first position
		result_df.columns = result_df_columns

		logging.debug("Executing export_config.getColumnsFromConfigDatabase() - Finished")
		return result_df

	def convertColumnTypeForTargetTable(self, columnName, columnType):
		""" Converts the column type to a type that is supported by the Target database type """
		logging.debug("Executing export_config.convertColumnTypeForTargetTable()")

		mapColumnJava = None
		sourceDatabaseType = ""

		sourceDB = self.hiveDB
		sourceTable = self.hiveTable
		historyTableExport = False

		if sourceTable.endswith('_history'):
			# First check to see that we dont have a import table that actually is called _history in the end
			query = "select count(hive_table) from import_tables where hive_db = %s and hive_table = %s"
			self.mysql_cursor01.execute(query, (sourceDB, sourceTable))
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
			row = self.mysql_cursor01.fetchone()
			if row[0] == 0:
				# No import table exists with this name. Lets try without the _history part
				sourceTable = re.sub('_history$', '', sourceTable)
				self.mysql_cursor01.execute(query, (sourceDB, sourceTable))
				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
				row = self.mysql_cursor01.fetchone()
				if row[0] == 1:
					# A table without _history exists. So lets use that to determine if we need force_string and such
					historyTableExport = True
				else:
					sourceTable = self.hiveTable

		if columnType in ("string", "binary"):
			# If the columnType is "string", it might be because we are exporting a table that was imported with force_string = 1.

			query  = "select "
			query += "  jc.force_string, "
			query += "  t.force_string, "
			query += "  c.force_string, "
			query += "  c.source_column_type, "
			query += "  c.source_database_type "
			query += "from import_tables t "
			query += "left join jdbc_connections jc "
			query += "  on t.dbalias = jc.dbalias "
			query += "left join import_columns c "
			query += "  on c.hive_db = t.hive_db "
			query += "  and c.hive_table = t.hive_table "
			query += "  and c.column_name = %s "
			query += "where "
			query += "  t.hive_db = %s "
			query += "  and t.hive_table = %s "

			self.mysql_cursor01.execute(query, (columnName.lower(), sourceDB, sourceTable))
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

			if columnType == "binary":
				if self.mysql_cursor01.rowcount == 1:
					row = self.mysql_cursor01.fetchone()
					sourceColumnType = row[3]
					sourceDatabaseType = row[4]
					columnType = sourceColumnType

			if columnType == "string":
				forceString = False
				if self.mysql_cursor01.rowcount == 1:
					row = self.mysql_cursor01.fetchone()
					force_string_connection = row[0]
					force_string_table = row[1]
					force_string_column = row[2]
					sourceColumnType = row[3]
					sourceDatabaseType = row[4]

					if force_string_column == 0:
						forceString = False
					elif force_string_column == 1:
						forceString = True
					elif force_string_table == 0:
						forceString = False
					elif force_string_table == 1:
						forceString = True
					elif force_string_connection == 0:
						forceString = False
					elif force_string_connection == 1:
						forceString = True
					else:
						forceString = True

				if forceString == True:
					columnType = sourceColumnType

		if self.common_config.db_mssql == True:
			columnType = re.sub('^timestamp', 'datetime', columnType)
			columnType = re.sub('^double$', 'real', columnType)
			columnType = re.sub('^string$', 'varchar(max)', columnType)
			columnType = re.sub('^boolean$', 'tinyint', columnType)

			if sourceDatabaseType == constant.ORACLE:
				columnType = re.sub('^varchar2\(', 'varchar(', columnType)

		if self.common_config.db_oracle == True:
			columnType = re.sub('^varchar\(-1\)$', 'string', columnType)
			columnType = re.sub('^nvarchar\(-1\)$', 'string', columnType)
			columnType = re.sub('^tinyint$', 'number(3)', columnType)
			columnType = re.sub('^smallint$', 'number(5)', columnType)
			columnType = re.sub('^decimal\(', 'number(', columnType)
			columnType = re.sub('^bigint$', 'number(19)', columnType)
			columnType = re.sub('^double$', 'binary_double', columnType)
			columnType = re.sub('^boolean$', 'number(1)', columnType)
			columnType = re.sub('^uniqueidentifier$', 'varchar2(36)', columnType)
			columnType = re.sub('^nvarchar\(', 'varchar2(', columnType)
			columnType = re.sub('^varchar\(', 'varchar2(', columnType)
			columnType = re.sub('^timestamp$', 'date', columnType)
			columnType = re.sub('^text$', 'string', columnType)
			columnType = re.sub('^int$', 'number', columnType)
			
			# Oracle only supports varchar up to 4000. After that it have to be a CLOB. This will rewrite the soure tabletype
			# to string and will later be converted to clob.
			if columnType.startswith("varchar2("): 
				columnSize = int(columnType.split("(")[1].split(")")[0])
				if columnSize > 4000:
					columnType="string"
#				else:
#					# If it's under 4000 chars, then we create it as a varchar, but with varchar(10 char) instead of varchar(10)
#					columnType = re.sub('\)$', ' char)', columnType)

			if columnType == "string":
				raise invalidConfiguration("Error: Exports of column '%s' with type 'string' to Oracle is not supported. Please set another columntype in 'target_column_type' or ignore this column in the export."%(columnName))
#				columnType = "clob"
#				mapColumnJava = "String"

		if self.common_config.db_mysql == True:
			columnType = re.sub('^string$', 'text', columnType)

		if self.common_config.db_db2udb == True:
			if columnType == "string":
				columnType = "clob"
				mapColumnJava = "String"
			columnType = re.sub('^timestamp$', 'timestmp', columnType)

		logging.debug("Executing export_config.convertColumnTypeForTargetTable() - Finished")
		return columnType, mapColumnJava


	def updateTargetTable(self):
		""" Updates the target table with new column definitions """
		logging.debug("Executing export_config.updateTargetTable()")
		logging.info("Updating the Target Table")

		columnsSource = self.getColumnsFromConfigDatabase(excludeColumns=True)
		forceUppercase = False

		targetSchema = self.targetSchema
		targetTable = self.targetTable

		if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
			forceUppercase = True
			targetSchema = self.targetSchema.upper()
			targetTable = self.targetTable.upper()

		self.sqoop_mapcolumnjava=[]
		firstLoop = True
		alterTableExecuted = False

		for index, row in columnsSource.iterrows():
			columnType = row['columnType']
			columnComment = row['comment']
			targetColumnName = row['targetColumnName']
			hiveColumnName = row['hiveColumnName']
			if targetColumnName != None and targetColumnName.strip() != "":
				columnName = targetColumnName
			else:
				columnName = hiveColumnName

			if forceUppercase == True:
				columnName = columnName.upper()

			columnType, mapcolumnjava = self.convertColumnTypeForTargetTable(hiveColumnName, columnType)
			if mapcolumnjava != None:
				self.sqoop_mapcolumnjava.append(columnName + "=" + mapcolumnjava)

			if forceUppercase == True:
				columnType = columnType.upper()

			columnsSource.iloc[index, columnsSource.columns.get_loc('columnType')] = columnType 
			columnsSource.iloc[index, columnsSource.columns.get_loc('targetColumnName')] = columnName 

		columnsSource.drop('hiveColumnName', axis=1, inplace=True)
		columnsSource.rename(columns={'targetColumnName':'name', 'columnType':'type'}, inplace=True)

#		print(columnsSource)
#		self.common_config.remove_temporary_files()
#		sys.exit(1)

		self.common_config.getJDBCTableDefinition(source_schema = targetSchema, source_table = targetTable, printInfo=False)
		columnsTarget = self.common_config.source_columns_df
		columnsTarget.rename(columns={
			'SOURCE_COLUMN_NAME':'name', 
			'SOURCE_COLUMN_COMMENT':'comment', 
			'SOURCE_COLUMN_TYPE':'type'}, 
			inplace=True)
		columnsTarget.drop('IS_NULLABLE', axis=1, inplace=True)
		columnsTarget.drop('TABLE_COMMENT', axis=1, inplace=True)

		# Check for missing columns
		columnsSourceOnlyName = columnsSource.filter(['name'])
		columnsTargetOnlyName = columnsTarget.filter(['name'])
		columnsMergeOnlyName = pd.merge(columnsSourceOnlyName, columnsTargetOnlyName, on=None, how='outer', indicator='Exist')

		columnsSourceCount = len(columnsSourceOnlyName)
		columnsTargetCount   = len(columnsTargetOnlyName)
		columnsMergeLeftOnlyCount  = len(columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'])
		columnsMergeRightOnlyCount = len(columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'right_only'])

		logging.debug("columnsSourceOnlyName")
		logging.debug(columnsSourceOnlyName)
		logging.debug("================================================================")
		logging.debug("columnsTargetOnlyName")
		logging.debug(columnsTargetOnlyName)
		logging.debug("================================================================")
		logging.debug("columnsMergeOnlyName")
		logging.debug(columnsMergeOnlyName)
		logging.debug("")

		if columnsSourceCount == columnsTargetCount and columnsMergeLeftOnlyCount > 0:
			# The number of columns in config and Hive is the same, but there is a difference in name. 
			# This is most likely because of a rename of one or more of the columns
			# To handle this, we try a rename. This might fail if the column types are also changed to an incompatable type
			# The logic here is to
			# 1. get all columns from mergeDF that exists in Left_Only
			# 2. Get the position in configDF with that column name
			# 3. Get the column in the same position from HiveDF
			# 4. Check if that column name exists in the mergeDF with Right_Only. If it does, the column was just renamed
			for index, row in columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iterrows():
				rowInSource = columnsSource.loc[columnsSource['name'] == row['name']].iloc[0]
				indexInSource = columnsSource.loc[columnsSource['name'] == row['name']].index.item()
				rowInTarget = columnsTarget.iloc[indexInSource]

				if len(columnsMergeOnlyName.loc[(columnsMergeOnlyName['Exist'] == 'right_only') & (columnsMergeOnlyName['name'] == rowInTarget["name"])]) > 0:
					# This is executed if the column is renamed and exists in the same position
					logging.debug("Name in Source:  %s"%(rowInSource["name"]))
					logging.debug("Type in Source:  %s"%(rowInSource["type"]))
					logging.debug("Index in Source: %s"%(indexInSource))
					logging.debug("--------------------")
					logging.debug("Name in Target: %s"%(rowInTarget["name"]))
					logging.debug("Type in Target: %s"%(rowInTarget["type"]))
					logging.debug("======================================")
					logging.debug("")

					if self.common_config.jdbc_servertype == constant.MSSQL:
						query = "EXEC sp_rename '%s.%s.%s', '%s', 'COLUMN'"%(targetSchema, targetTable, rowInTarget["name"], rowInSource["name"])

					if self.common_config.jdbc_servertype == constant.ORACLE:
						query = "ALTER TABLE \"%s\".\"%s\" RENAME COLUMN \"%s\" TO \"%s\""%(targetSchema, targetTable, rowInTarget["name"], rowInSource["name"])

					if self.common_config.jdbc_servertype == constant.DB2_UDB:
						query = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s"%(targetSchema, targetTable, rowInTarget["name"], rowInSource["name"])

					if self.common_config.jdbc_servertype == constant.MYSQL:
						query = "ALTER TABLE %s CHANGE COLUMN %s %s %s"%(targetTable, rowInTarget["name"], rowInSource["name"], rowInSource["type"])
					self.common_config.executeJDBCquery(query)
					alterTableExecuted = True

					logging.info("Column %s renamed to %s in Target Table"%(rowInTarget["name"], rowInSource["name"]))
					self.logTargetColumnRename(rowInSource['name'], rowInTarget["name"], dbAlias=self.connectionAlias, database=self.common_config.jdbc_database, schema=targetSchema, table=targetTable)

					if rowInSource["type"] != rowInTarget["type"]:
						logging.info("Column %s changed type from %s to %s"%(rowInSource["name"], rowInTarget["type"], rowInSource["type"]))
						self.logTargetColumnTypeChange(rowInSource['name'], rowInSource['type'], previous_columnType=rowInTarget["type"], dbAlias=self.connectionAlias, database=self.common_config.jdbc_database, schema=targetSchema, table=targetTable)
				else:
					if columnsMergeLeftOnlyCount == 1 and columnsMergeRightOnlyCount == 1:
						# So the columns are not in the same position, but it's only one column that changed. In that case, we just rename that one column
						rowInMergeLeft  = columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iloc[0]
						rowInMergeRight = columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'right_only'].iloc[0]
						rowInSource = columnsSource.loc[columnsSource['name'] == rowInMergeLeft['name']].iloc[0]
						rowInTarget = columnsTarget.loc[columnsTarget['name'] == rowInMergeRight['name']].iloc[0]
						logging.debug(rowInSource["name"])
						logging.debug(rowInSource["type"])
						logging.debug("--------------------")
						logging.debug(rowInTarget["name"])
						logging.debug(rowInTarget["type"])

						if self.common_config.jdbc_servertype == constant.MSSQL:
							query = "EXEC sp_rename '%s.%s.%s', '%s', 'COLUMN'"%(targetSchema, targetTable, rowInTarget["name"], rowInSource["name"])

						if self.common_config.jdbc_servertype == constant.ORACLE:
							query = "ALTER TABLE \"%s\".\"%s\" RENAME COLUMN \"%s\" TO \"%s\""%(targetSchema, targetTable, rowInTarget["name"], rowInSource["name"])

						if self.common_config.jdbc_servertype == constant.DB2_UDB:
							query = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s"%(targetSchema, targetTable, rowInTarget["name"], rowInSource["name"])

						if self.common_config.jdbc_servertype == constant.MYSQL:
							query = "ALTER TABLE %s CHANGE COLUMN %s %s %s NULL"%(targetTable, rowInTarget["name"], rowInSource["name"], rowInSource["type"])
						self.common_config.executeJDBCquery(query)
						alterTableExecuted = True

						logging.info("Column %s renamed to %s in Target Table"%(rowInTarget["name"], rowInSource["name"]))
						self.logTargetColumnRename(rowInSource['name'], rowInTarget["name"], dbAlias=self.connectionAlias, database=self.common_config.jdbc_database, schema=targetSchema, table=targetTable)

						if rowInSource["type"] != rowInTarget["type"] and self.common_config.jdbc_servertype in (constant.MYSQL):
							logging.info("Column %s changed type from %s to %s"%(rowInSource["name"], rowInTarget["type"], rowInSource["type"]))
							self.logTargetColumnTypeChange(rowInSource['name'], rowInSource['type'], previous_columnType=rowInTarget["type"], dbAlias=self.connectionAlias, database=self.common_config.jdbc_database, schema=targetSchema, table=targetTable)

			# Update the DF because the target table was changed
			self.common_config.getJDBCTableDefinition(source_schema = targetSchema, source_table = targetTable, printInfo=False)
			columnsTarget = self.common_config.source_columns_df
			columnsTarget.rename(columns={
				'SOURCE_COLUMN_NAME':'name', 
				'SOURCE_COLUMN_COMMENT':'comment', 
				'SOURCE_COLUMN_TYPE':'type'}, 
				inplace=True)
			columnsTarget.drop('IS_NULLABLE', axis=1, inplace=True)
			columnsTarget.drop('TABLE_COMMENT', axis=1, inplace=True)
			columnsTargetOnlyName = columnsTarget.filter(['name'])
			columnsMergeOnlyName = pd.merge(columnsSourceOnlyName, columnsTargetOnlyName, on=None, how='outer', indicator='Exist')

		for index, row in columnsMergeOnlyName.loc[columnsMergeOnlyName['Exist'] == 'left_only'].iterrows():
			# This will iterate over columns that only exists in the config and not in Hive. We add these to Hive
			fullRow = columnsSource.loc[columnsSource['name'] == row['name']].iloc[0]

			if self.common_config.jdbc_servertype == constant.MSSQL:
				query = "ALTER TABLE %s.%s ADD %s %s NULL"%(targetSchema, targetTable, fullRow["name"], fullRow["type"])

			if self.common_config.jdbc_servertype == constant.ORACLE:
				query = "ALTER TABLE \"%s\".\"%s\" ADD \"%s\" %s"%(targetSchema, targetTable, fullRow["name"], fullRow["type"])

			if self.common_config.jdbc_servertype == constant.DB2_UDB:
				query = "ALTER TABLE %s.%s ADD COLUMN %s %s"%(targetSchema, targetTable, fullRow["name"], fullRow["type"])

			if self.common_config.jdbc_servertype == constant.MYSQL:
				query = "ALTER TABLE %s ADD COLUMN %s %s NULL"%(targetTable, fullRow["name"], fullRow["type"])

			self.common_config.executeJDBCquery(query)
			alterTableExecuted = True

			logging.info("Column %s (%s) added to Target Table"%(fullRow["name"], fullRow["type"]))
			self.logTargetColumnAdd(fullRow['name'], columnType=fullRow["type"], dbAlias=self.connectionAlias, database=self.common_config.jdbc_database, schema=targetSchema, table=targetTable)

		# Update the DF because the target table was changed
		self.common_config.getJDBCTableDefinition(source_schema = targetSchema, source_table = targetTable, printInfo=False)
		columnsTarget = self.common_config.source_columns_df
		columnsTarget.rename(columns={
			'SOURCE_COLUMN_NAME':'name', 
			'SOURCE_COLUMN_COMMENT':'comment', 
			'SOURCE_COLUMN_TYPE':'type'}, 
			inplace=True)
		columnsTarget.drop('IS_NULLABLE', axis=1, inplace=True)
		columnsTarget.drop('TABLE_COMMENT', axis=1, inplace=True)

		# Check for changed column types
		columnsSourceOnlyNameType = columnsSource.filter(['name', 'type']).sort_values(by=['name'], ascending=True)
		columnsTargetOnlyNameType = columnsTarget.filter(['name', 'type']).sort_values(by=['name'], ascending=True)
		columnsMergeOnlyNameType = pd.merge(columnsSourceOnlyNameType, columnsTargetOnlyNameType, on=None, how='outer', indicator='Exist')

		logging.debug("columnsSourceOnlyNameType")
		logging.debug(columnsSourceOnlyNameType)
		logging.debug("================================================================")
		logging.debug("columnsTargetOnlyNameType")
		logging.debug(columnsTargetOnlyNameType)
		logging.debug("================================================================")
		logging.debug("columnsMergeOnlyNameType")
		logging.debug(columnsMergeOnlyNameType)
		logging.debug("")

		for index, row in columnsMergeOnlyNameType.loc[columnsMergeOnlyNameType['Exist'] == 'left_only'].iterrows():
			# This will iterate over columns that had the type changed from the source
			if self.common_config.jdbc_servertype == constant.MSSQL:
				query = "ALTER TABLE %s.%s ALTER COLUMN %s %s NULL"%(targetSchema, targetTable, row["name"], row["type"])

			if self.common_config.jdbc_servertype == constant.ORACLE:
				query = "ALTER TABLE \"%s\".\"%s\" MODIFY (\"%s\" %s)"%(targetSchema, targetTable, row["name"], row["type"])

			if self.common_config.jdbc_servertype == constant.DB2_UDB:
				query = "ALTER TABLE %s.%s ALTER COLUMN %s SET DATA TYPE %s"%(targetSchema, targetTable, row["name"], row["type"])

			if self.common_config.jdbc_servertype == constant.MYSQL:
				query = "ALTER TABLE %s CHANGE COLUMN %s %s %s NULL"%(targetTable, row["name"], row["name"], row["type"])

			self.common_config.executeJDBCquery(query)
			alterTableExecuted = True

#			query = "alter table `%s`.`%s` change column `%s` `%s` %s"%(hiveDB, hiveTable, row['name'], row['name'], row['type'])

			# Get the previous column type from the Pandas DF with right_only in Exist column
			previous_columnType = (columnsMergeOnlyNameType.loc[
				(columnsMergeOnlyNameType['name'] == row['name']) &
				(columnsMergeOnlyNameType['Exist'] == 'right_only')]
				).reset_index().at[0, 'type']

			logging.info("Column %s changed type from %s to %s"%(row["name"], previous_columnType, row["type"]))
			self.logTargetColumnTypeChange(row['name'], columnType=row['type'], previous_columnType=previous_columnType, dbAlias=self.connectionAlias, database=self.common_config.jdbc_database, schema=targetSchema, table=targetTable)

		# Check for comment changes
		# Update the DF because the target table was changed
		self.common_config.getJDBCTableDefinition(source_schema = targetSchema, source_table = targetTable, printInfo=False)
		columnsTarget = self.common_config.source_columns_df
		columnsTarget.rename(columns={
			'SOURCE_COLUMN_NAME':'name', 
			'SOURCE_COLUMN_COMMENT':'comment', 
			'SOURCE_COLUMN_TYPE':'type'}, 
			inplace=True)
		columnsTarget.drop('IS_NULLABLE', axis=1, inplace=True)
		columnsTarget.drop('TABLE_COMMENT', axis=1, inplace=True)
		columnsTarget['comment'].replace('', None, inplace = True)        # Replace blank column comments with None as it would otherwise trigger an alter table on every run

		if alterTableExecuted == True and self.common_config.jdbc_servertype == constant.DB2_UDB:
			# DB2 require a reorg if the table was changed
			logging.info("Running a reorg on the target table")
			query = "call SYSPROC.ADMIN_CMD('REORG TABLE %s.%s')"%(targetSchema, targetTable)
			self.common_config.executeJDBCquery(query)

		columnsMerge = pd.merge(columnsSource, columnsTarget, on=None, how='outer', indicator='Exist')
		for index, row in columnsMerge.loc[columnsMerge['Exist'] == 'left_only'].iterrows():
			if row['comment'] == None: row['comment'] = ""

			if self.common_config.jdbc_servertype == constant.MSSQL:
				query = "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'%s', @level1type=N'TABLE',@level1name=N'%s', @level2type=N'COLUMN',@level2name=N'%s';"%(row["comment"], targetSchema, targetTable, row["name"])

			if self.common_config.jdbc_servertype == constant.ORACLE:
				query = "comment on column \"%s\".\"%s\".\"%s\" is '%s'"%(targetSchema, targetTable, row["name"], row["comment"])

			if self.common_config.jdbc_servertype == constant.DB2_UDB:
				query = "COMMENT ON COLUMN %s.%s.%s IS '%s'"%(targetSchema, targetTable, row["name"], row["comment"])

			if self.common_config.jdbc_servertype == constant.MYSQL:
				query = "ALTER TABLE %s CHANGE COLUMN %s %s %s NULL COMMENT '%s'"%(targetTable, row["name"], row["name"], row["type"], row['comment'])

			self.common_config.executeJDBCquery(query)
			alterTableExecuted = True



		logging.debug("Executing export_config.updateTargetTable() - Finished")

	def getSqoopMapColumnJava(self):
		""" Returns the generated mapColumnJava settngs for all columns """
		logging.debug("Executing export_config.getSqoopMapColumnJava()")

		columnsDF = self.getColumnsFromConfigDatabase(excludeColumns=True)
		self.sqoop_mapcolumnjava=[]
		forceColumnUppercase = False

		if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
			forceColumnUppercase = True

		for index, row in columnsDF.iterrows():
			columnType = row['columnType']
			targetColumnName = row['targetColumnName']
			hiveColumnName = row['hiveColumnName']
			if targetColumnName != None and targetColumnName.strip() != "":
				columnName = targetColumnName
			else:
				columnName = hiveColumnName

			if forceColumnUppercase == True:
				columnName = columnName.upper()

			columnType, mapColumnJava = self.convertColumnTypeForTargetTable(hiveColumnName, columnType)
			if mapColumnJava != None:
				self.sqoop_mapcolumnjava.append(columnName + "=" + mapColumnJava)

		logging.debug("Executing export_config.getSqoopMapColumnJava() - Finished")
		return self.sqoop_mapcolumnjava

	def createTargetTable(self):
		""" Creates the target table on the JDBC connection. This is the table we will export to """
		logging.debug("Executing export_config.createTargetTable()")
		logging.info("Creating the Target Table")
	
		columnsDF = self.getColumnsFromConfigDatabase(excludeColumns=True)
		forceColumnUppercase = False
		columnQuote = ""

		if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
			targetSchema = self.targetSchema.upper()
			targetTable = self.targetTable.upper()
			forceColumnUppercase = True
			columnQuote = "\""
			query  = "create table \"%s\".\"%s\" ("%(targetSchema, targetTable) 
		else:
			targetSchema = self.targetSchema
			targetTable = self.targetTable
			if targetSchema == "-":
				query  = "create table %s ("%(targetTable) 
			else:
				query  = "create table %s.%s ("%(targetSchema, targetTable) 

		self.sqoop_mapcolumnjava=[]
		firstLoop = True

		for index, row in columnsDF.iterrows():
			columnType = row['columnType']
			columnComment = row['comment']
			targetColumnName = row['targetColumnName']
			hiveColumnName = row['hiveColumnName']
			if targetColumnName != None and targetColumnName.strip() != "":
				columnName = targetColumnName
			else:
				columnName = hiveColumnName

			if forceColumnUppercase == True:
				columnName = columnName.upper()

			if firstLoop == False: 
				query += ", "
			else:
				firstLoop = False

			columnType, mapColumnJava = self.convertColumnTypeForTargetTable(hiveColumnName, columnType)
			if mapColumnJava != None:
				self.sqoop_mapcolumnjava.append(columnName + "=" + mapColumnJava)

			query += "%s%s%s %s NULL "%(columnQuote, columnName, columnQuote, columnType)
			if self.common_config.jdbc_servertype not in (constant.ORACLE, constant.MSSQL):
				if columnComment != None and columnComment.strip() != "":
					query += "comment \"%s\""%(columnComment)
		query += ")"
				
		self.common_config.executeJDBCquery(query)

		logging.debug("Executing export_config.createTargetTable() - Finished")

	def saveSqoopStatistics(self, sqoopStartUTS, sqoopSize=None, sqoopRows=None, sqoopIncrMaxvaluePending=None, sqoopMappers=None):
		logging.debug("Executing export_config.saveSqoopStatistics()")
		logging.info("Saving sqoop statistics")

		self.sqoopStartUTS = sqoopStartUTS
		self.sqoop_last_execution_timestamp = datetime.utcfromtimestamp(sqoopStartUTS).strftime('%Y-%m-%d %H:%M:%S.000')

		queryParam = []
		query =  "update export_tables set "
		query += "	sqoop_last_execution = %s "
		queryParam.append(sqoopStartUTS)

		if sqoopSize != None:
			query += "  ,sqoop_last_size = %s "
			self.sqoop_last_size = sqoopSize
			queryParam.append(sqoopSize)

		if sqoopRows != None:
			query += "  ,sqoop_last_rows = %s "
			self.sqoop_last_rows = sqoopRows
			queryParam.append(sqoopRows)

		if sqoopIncrMaxvaluePending != None:
			query += "  ,incr_maxvalue_pending = %s "
			self.sqoopIncrMaxvaluePending = sqoopIncrMaxvaluePending
			queryParam.append(sqoopIncrMaxvaluePending)

		if sqoopMappers != None:
			query += "  ,sqoop_last_mappers = %s "
			self.sqoop_last_mappers = sqoopMappers
			queryParam.append(sqoopMappers)

#		if self.validate_source == "sqoop":
#			logging.info("Saving the imported row count as the number of rows in the source system.")
#			if self.import_is_incremental == True:
#				query += "  ,source_rowcount = NULL "
#				query += "  ,source_rowcount_incr = %s "
#			else:
#				query += "  ,source_rowcount = %s "
#				query += "  ,source_rowcount_incr = NULL "
#			queryParam.append(sqoopRows)

		query += "where table_id = %s "
		queryParam.append(self.tableID)

		self.mysql_cursor01.execute(query, queryParam)
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		self.mysql_conn.commit()

		logging.debug("Executing export_config.saveSqoopStatistics() - Finished")

	def saveHiveTableRowCount(self, rowCount):
		logging.debug("Executing export_config.saveHiveTableRowCount()")
		logging.info("Saving the number of rows in the Hive Table to the configuration database")

		# Save the value to the database
		query = ("update export_tables set hive_rowcount = %s where table_id = %s")
		self.mysql_cursor01.execute(query, (rowCount, self.tableID))
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		logging.debug("Executing export_config.saveHiveTableRowCount() - Finished")

#	def saveTargetTableRowCount(self, rowCount):
#		logging.debug("Executing export_config.saveTargetTableRowCount()")
#		logging.info("Saving the number of rows in the Target Table to the configuration database")
#
#		# Save the value to the database
#		query = ("update export_tables set target_rowcount = %s where table_id = %s")
#		self.mysql_cursor01.execute(query, (rowCount, self.tableID))
#		self.mysql_conn.commit()
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#
#		logging.debug("Executing export_config.saveTargetTableRowCount() - Finished")

	def getJDBCTableRowCount(self, whereStatement=None):
		logging.debug("Executing export_config.getJDBCTableRowCount()")

#		if self.validate_source == "sqoop":
#			logging.debug("Executing export_config.getJDBCTableRowCount() - Finished (because self.validate_source == sqoop)")
#			return

		logging.info("Reading and saving number of rows in target table. This will later be used for validating the export")

		JDBCRowsFull = None

#		if self.export_is_incremental == True:
#			whereStatement = self.getIncrWhereStatement(forceIncr = False, whereForSourceTable=True)
#			logging.debug("Where statement for forceIncr = False: %s"%(whereStatement))
#			JDBCRowsFull = self.common_config.getJDBCTableRowCount(self.source_schema, self.source_table, whereStatement)
#			logging.debug("Got %s rows from getJDBCTableRowCount()"%(JDBCRowsFull))
##
#			whereStatement = self.getIncrWhereStatement(forceIncr = True, whereForSourceTable=True)
#			logging.debug("Where statement for forceIncr = True: %s"%(whereStatement))
#			JDBCRowsIncr = self.common_config.getJDBCTableRowCount(self.source_schema, self.source_table, whereStatement)
#			logging.debug("Got %s rows from getJDBCTableRowCount()"%(JDBCRowsIncr))
#
#		else:
#			if self.sqoop_sql_where_addition != None:
#				whereStatement = " where %s"%(self.sqoop_sql_where_addition)
#			else:
#				whereStatement = ""
		JDBCRowsFull = self.common_config.getJDBCTableRowCount(self.targetSchema, self.targetTable, whereStatement)

		# Save the value to the database
		query = "update export_tables set "

#		if JDBCRowsIncr != None: 
#			query += "source_rowcount_incr = %s "%(JDBCRowsIncr)
#			logging.debug("Source table contains %s rows for the incremental part"%(JDBCRowsIncr))
#
#		if JDBCRowsFull != None: 
#			if  JDBCRowsIncr != None: query += ", "
#			query += "source_rowcount = %s "%(JDBCRowsFull)
#			logging.debug("Source table contains %s rows"%(JDBCRowsFull))
		query += "target_rowcount = %s "%(JDBCRowsFull)
		query += "where table_id = %s"%(self.tableID)

		self.mysql_cursor01.execute(query)
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		logging.debug("Executing export_config.getJDBCTableRowCount() - Finished")

	def resetIncrMinMaxValues(self, maxValue):
		logging.debug("Executing export_config.resetIncrMinMaxValues()")
		if maxValue != None:
			logging.info("Reseting incremental values for sqoop exports. New max value is: %s"%(maxValue))
		else:
			logging.info("Reseting incremental values for sqoop exports.")

		query =  "update export_tables set "
		query += "	incr_minvalue = NULL, "
		query += "	incr_maxvalue = %s, "
		query += "	incr_minvalue_pending = NULL, "
		query += "	incr_maxvalue_pending = NULL "
		query += "where table_id = %s "

		self.mysql_cursor01.execute(query, (maxValue, self.tableID))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		self.mysql_conn.commit()

		logging.debug("Executing export_config.resetIncrMinMaxValues() - Finished")

	def validateRowCount(self, validateSqoop=False, incremental=False):
		""" Validates the rows based on values stored in export_tables -> hive_rowcount and target_rowcount. Returns True or False """
		# incremental=True is used for normal validations but only on incremented rows, regardless if we are going to validate full or incr.

		logging.debug("Executing export_config.validateRowCount()")
		returnValue = None

		if self.validateExport == True:
			# Reading the saved number from the configurationdatabase
#			if validateSqoop == False:
			if 1 == 1:
				if self.exportIsIncremental == False or (self.exportIsIncremental == True and self.incr_validation_method == "full"):
					# Standard full validation
					query  = "select hive_rowcount, target_rowcount from export_tables where table_id = %s "
					validateTextTarget = "Target table"
					validateTextSource = "Hive table"
					validateText = validateTextTarget
#				elif self.sqoop_incr_validation_method == "full" and incremental == False:
				else:
					query  = "select hive_rowcount, target_rowcount from export_tables where table_id = %s "
					validateTextTarget = "Target table (incr)"
					validateTextSource = "Hive table (incr)"
					validateText = "Target table"
#				else:
#					raise undevelopedFeature("Only full validation is supported")
#					# We are not validating the sqoop export, but the validation is an incremental export and
#					# we are going to validate only the incremental part of the export (what sqoop exported)
#					query  = "select source_rowcount_incr, hive_rowcount from export_tables where table_id = %s "
#					validateTextTarget = "Hive table (incr)"
#					validateTextSource = "Source table (incr)"
#					validateText = "Hive table"
			else:
				raise undevelopedFeature("Unsupported validation")
#				if self.export_is_incremental == False:
#					# Sqoop validation for full exports
#					query  = "select source_rowcount from export_tables where table_id = %s "
#					validateTextTarget = "Sqoop export"
#					validateTextSource = "Source table"
#					validateText = validateTextTarget
#				else:
#					# Sqoop validation for incremental exports
#					query  = "select source_rowcount_incr from export_tables where table_id = %s "
#					validateTextTarget = "Sqoop export (incr)"
#					validateTextSource = "Source table (incr)"
#					validateText = "Sqoop export"
			self.mysql_cursor01.execute(query, (self.tableID, ))
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

			row = self.mysql_cursor01.fetchone()
			source_rowcount = row[0]
#			if validateSqoop == False:
			target_rowcount = row[1]
#			else:
#				target_rowcount = self.sqoop_last_rows
			diffAllowed = 0
			logging.debug("source_rowcount: %s"%(source_rowcount))
			logging.debug("target_rowcount: %s"%(target_rowcount))

#			if source_rowcount > 0:
#				if self.validate_diff_allowed != None:
#					diffAllowed = int(self.validate_diff_allowed)
#				else:
#					diffAllowed = int(source_rowcount*(50/(100*math.sqrt(source_rowcount))))
#
#			upperValidationLimit = source_rowcount + diffAllowed
#			lowerValidationLimit = source_rowcount - diffAllowed

#			logging.debug("upperValidationLimit: %s"%(upperValidationLimit))
#			logging.debug("lowerValidationLimit: %s"%(lowerValidationLimit))
#			logging.debug("diffAllowed: %s"%(diffAllowed))


#			if target_rowcount > upperValidationLimit or target_rowcount < lowerValidationLimit:
			if source_rowcount != target_rowcount:
				logging.error("Validation failed! The %s exceedes the allowed limit compared to the %s table"%(validateTextSource,validateTextTarget ))
				if target_rowcount > source_rowcount:
					logging.info("Diff between tables: %s"%(target_rowcount - source_rowcount))
				else:
					logging.info("Diff between tables: %s"%(source_rowcount - target_rowcount))
				logging.info("%s rowcount: %s"%(validateTextSource, source_rowcount))
				logging.info("%s rowcount: %s"%(validateTextTarget, target_rowcount))
#				logging.info("Validation diff allowed: %s"%(diffAllowed))
#				logging.info("Upper limit: %s"%(upperValidationLimit))
#				logging.info("Lower limit: %s"%(lowerValidationLimit))
				returnValue = False 
			else:
				logging.info("%s validation successful!"%(validateText))
				logging.info("%s rowcount: %s"%(validateTextSource, source_rowcount))
				logging.info("%s rowcount: %s"%(validateTextTarget, target_rowcount))
				logging.info("")
				returnValue = True 

		else:
			returnValue = True

		logging.debug("Executing export_config.validateRowCount() - Finished")
		return returnValue
		
	def calculateJobMappers(self):
		""" Based on previous sqoop size, the number of mappers for sqoop will be calculated. Formula is sqoop_size / 128MB. """

		logging.debug("Executing export_config.calculateJobMappers()")
		logging.info("Calculating the number of SQL sessions in the source system that the export will use")

		sqlSessionsMin = 1
		sqlSessionsMax = None

		# TODO: Add max_export_sessions to jdbc_connections
#		query = "select max_export_sessions from jdbc_connections where dbalias = %s "
#		self.mysql_cursor01.execute(query, (self.connection_alias, ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#
#		row = self.mysql_cursor01.fetchone()
		row = None

		if row != None and row[0] > 0: 
			sqlSessionsMax = row[0]
			logging.info("\u001B[33mThis connection is limited to a maximum of %s parallel sessions during export\u001B[0m"%(sqlSessionsMax))

		if row != None and row[0] == 0: 
			logging.warning("Unsupported value of 0 in column 'max_export_sessions' in table 'jdbc_connections'")

		# Fetch the configured max and default value from configuration file
		sqlSessionsMaxFromConfig = int(configuration.get("Export", "max_sql_sessions"))
		sqlSessionsDefault = int(configuration.get("Export", "default_sql_sessions"))

		if sqlSessionsMax == None: sqlSessionsMax = sqlSessionsMaxFromConfig 
		if sqlSessionsMaxFromConfig < sqlSessionsMax: sqlSessionsMax = sqlSessionsMaxFromConfig
		if sqlSessionsDefault > sqlSessionsMax: sqlSessionsDefault = sqlSessionsMax

		# Execute SQL query that calculates the value
		query = ("select sqoop_last_size, " 
				"cast(sqoop_last_size / (1024*1024*128) as unsigned) as calculated_mappers, "
				"mappers "
				"from export_tables where "
				"table_id = %s")
		self.mysql_cursor01.execute(query, (self.tableID, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		if self.mysql_cursor01.rowcount != 1:
			logging.error("Error: Number of rows returned from query on 'export_table' was not one.")
			logging.error("Rows returned: %d" % (self.mysql_cursor01.rowcount) )
			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor01.statement) )
			raise Exception

		row = self.mysql_cursor01.fetchone()
		sqlSessionsLast = row[0]
		self.sqlSessions = row[1]
		self.sqoopMappers = row[2]

		if self.sqoopMappers > 0: 
			logging.info("Setting the number of mappers to a fixed value")
			self.sqlSessions = self.sqoopMappers

		if self.sqlSessions == None:
			logging.info("Cant find the previous number of SQL sessions used. Will default to %s"%(sqlSessionsDefault))
			self.sqlSessions = sqlSessionsDefault
		else:
			if self.sqlSessions < sqlSessionsMin:
				self.sqlSessions = sqlSessionsMin
			elif self.sqlSessions > sqlSessionsMax:
				self.sqlSessions = sqlSessionsMax

			logging.info("The export will use %s parallell SQL sessions in the source system."%(self.sqlSessions)) 

		logging.debug("Executing export_config.calculateJobMappers() - Finished")


	def saveIncrMinMaxValue(self, minValue, maxValue):
		""" Save the min value in the pendings column """
		logging.debug("Executing export_config.saveIncrMinValue()")

		query  = "update export_tables set " 
		query += "	incr_minvalue_pending = %s, "
		query += "	incr_maxvalue_pending = %s "
		query += "where table_id = %s"

		self.mysql_cursor01.execute(query, (minValue, maxValue, self.tableID, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		self.mysql_conn.commit()

		logging.debug("Executing export_config.saveIncrMinValue() - Finished")


	def saveIncrPendingValues(self):
		""" After the incremental export is completed, we save the pending values so the next export starts from the correct spot"""
		logging.debug("Executing export_config.savePendingIncrValues()")

		query  = "update export_tables set " 
		query += "	incr_minvalue = incr_minvalue_pending, "
		query += "	incr_maxvalue = incr_maxvalue_pending "
		query += "where table_id = %s"

		self.mysql_cursor01.execute(query, (self.tableID, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		self.mysql_conn.commit()

		query  = "update export_tables set " 
		query += "	incr_minvalue_pending = NULL, "
		query += "	incr_maxvalue_pending = NULL "
		query += "where table_id = %s"

		self.mysql_cursor01.execute(query, (self.tableID, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		self.mysql_conn.commit()

		logging.debug("Executing export_config.savePendingIncrValues() - Finished")

	def isThereIncrDataToExport(self):
		""" Returns False if minvalue_pending = maxvalue_pending. That means that there is no new data in Hive to export """
		logging.debug("Executing export_config.isThereIncrDataToExport()")

		dataToExport = True

		query = "select incr_minvalue_pending, incr_maxvalue_pending from export_tables where table_id = %s "
		self.mysql_cursor01.execute(query, (self.tableID, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		row = self.mysql_cursor01.fetchone()
		minValue = row[0]
		maxValue = row[1]
	
		if minValue == maxValue:
			dataToExport = False

		logging.debug("dataToExport: %s"%(dataToExport))
		logging.debug("Executing export_config.isThereIncrDataToExport() - Finished")
		return dataToExport

	def getIncrWhereStatement(self, excludeMinValue=False, whereForTarget=False):
		""" Returns the where statement that is needed to only work on the rows that was loaded incr """
		logging.debug("Executing export_config.getIncrWhereStatement()")

		columnName = self.incr_column
		columnQuotation = ""
		whereStatement = ""
	
		query = "select column_type from export_columns where table_id = %s and column_name = %s "
		self.mysql_cursor01.execute(query, (self.tableID, columnName))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		row = self.mysql_cursor01.fetchone()
		columnType = row[0]

		# COALESCE is needed if you are going to call a function that counts source target rows outside the normal import
		query  = "select "
		query += "   COALESCE(incr_minvalue_pending, incr_minvalue), "
		query += "   COALESCE(incr_maxvalue_pending, incr_maxvalue) "
		query += "from export_tables where table_id = %s "
		self.mysql_cursor01.execute(query, (self.tableID, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		row = self.mysql_cursor01.fetchone()
		minValue = row[0]
		maxValue = row[1]

		# Regardless of how we translate the minValue in the next couple of rows, if it's NONE here, it will never be included in the where
		if minValue == None: excludeMinValue = True

		if whereForTarget == True and self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
			columnName = columnName.upper()

		if whereForTarget == True and columnType == "timestamp" and self.common_config.jdbc_servertype == constant.MSSQL:
			minValue += "CONVERT(datetime, '%s', 121) "%(minValue)
			maxValue += "CONVERT(datetime, '%s', 121) "%(maxValue)
		elif whereForTarget == True and columnType == "timestamp" and self.common_config.jdbc_servertype == constant.MYSQL:
			minValue += "STR_TO_DATE('%s', '%Y-%m-%d %H:%i:%s')"%(minValue)
			maxValue += "STR_TO_DATE('%s', '%Y-%m-%d %H:%i:%s')"%(maxValue)
		elif whereForTarget == True and columnType == "timestamp" and self.common_config.jdbc_servertype == constant.ORACLE:
			minValue = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')"%(minValue)
			maxValue = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')"%(maxValue)
		elif columnType not in ("int", "integer", "bigint", "tinyint", "smallint", "decimal", "double", "float", "boolean"):
			minValue = "\"%s\""%(minValue)
			maxValue = "\"%s\""%(maxValue)

		if whereForTarget == True and self.common_config.jdbc_servertype == constant.ORACLE:
			columnQuotation = "\""

		if whereForTarget == False:
			columnQuotation = "`"

		if minValue != None and excludeMinValue == False:
			whereStatement += "%s%s%s > %s "%(columnQuotation, columnName, columnQuotation, minValue)
			whereStatement += "and "
		whereStatement += "%s%s%s <= %s "%(columnQuotation, columnName, columnQuotation, maxValue)

		logging.debug("Executing export_config.getIncrWhereStatement() - Finished")
		return whereStatement


	def getExportTables(self, dbalias, schema):
		""" Return all tables that are exported to a specific connection and schema """
		logging.debug("Executing export_config.getExportTables()")

		query = "select hive_db as hiveDB, hive_table as hiveTable from export_tables where dbalias = %s and target_schema = %s "
		self.mysql_cursor01.execute(query, (dbalias, schema))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		result_df = pd.DataFrame(self.mysql_cursor01.fetchall())
		if len(result_df) == 0:
			print("Error: No data returned when searching for tables that are exported to dbalias '%s' and schema '%s'"%(dbalias, schema))
			self.remove_temporary_files()
			sys.exit(1)

		# Set the correct column namnes in the DataFrame
		result_df_columns = []
		for columns in self.mysql_cursor01.description:
			result_df_columns.append(columns[0])    # Name of the column is in the first position
		result_df.columns = result_df_columns

		logging.debug("Executing export_config.getExportTables() - Finished")
		return result_df

	def addExportTable(self, hiveDB, hiveTable, dbalias, schema, table):
		""" Add source table to export_tables """
		logging.debug("Executing export_config.addImportTable()")
		returnValue = True

		query = ("insert into export_tables "
				"("
				"    hive_db, "
				"    hive_table, "
				"    dbalias, "
				"    target_schema, "
				"    target_table "
				") values ( %s, %s, %s, %s, %s )")

		logging.debug("hiveDB:    %s"%(hiveDB))
		logging.debug("hiveTable: %s"%(hiveTable))
		logging.debug("dbalias:   %s"%(dbalias))
		logging.debug("schema:    %s"%(schema))
		logging.debug("table:     %s"%(table))

		try:
			self.mysql_cursor01.execute(query, (hiveDB, hiveTable, dbalias, schema, table ))
			self.mysql_conn.commit()
		except mysql.connector.errors.IntegrityError:
			logging.warning("Hive table %s.%s cant be added. The Connection name, Schema and Table already exists"%(hiveDB, hiveTable))
			returnValue = False

		logging.debug("Executing export_config.addImportTable() - Finished")
		return returnValue

# =====================================================================================================================================
# DELETE ALL AFTER THIS
# =====================================================================================================================================

#	def removeFKforTable(self):
#		# This function will remove all ForeignKey definitions for the current table.
#		logging.debug("")
#		logging.debug("Executing import_config.removeFKforTable()")
#
#		# Update the import_tables.last_update_from_source with the current date
#		query = "delete from import_foreign_keys where table_id = %s"
#		logging.debug("")
#		logging.debug("Deleting FK's from import_foreign_keys")
#		self.mysql_cursor01.execute(query, (self.table_id, ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#		self.mysql_conn.commit()
#
#		logging.debug("Executing import_config.removeFKforTable() - Finished")
#
#	def stripUnwantedCharComment(self, work_string):
#		if work_string == None: return
#		work_string = work_string.replace('`', '')
#		work_string = work_string.replace('\'', '')
#		work_string = work_string.replace(';', '')
#		work_string = work_string.replace('\n', '')
#		work_string = work_string.replace('\\', '')
#		work_string = work_string.replace('’', '')
#		work_string = work_string.replace('"', '')
#		return work_string.strip()
#
#	def stripUnwantedCharColumnName(self, work_string):
#		if work_string == None: return
#		work_string = work_string.replace('`', '')
#		work_string = work_string.replace('\'', '')
#		work_string = work_string.replace(';', '')
#		work_string = work_string.replace('\n', '')
#		work_string = work_string.replace('\\', '')
#		work_string = work_string.replace('’', '')
#		work_string = work_string.replace(':', '')
#		work_string = work_string.replace(',', '')
#		work_string = work_string.replace('.', '')
#		work_string = work_string.replace('"', '')
#		return work_string.strip()

#	def convertHiveColumnNameReservedWords(self, columnName):
#		if columnName.lower() in ("synchronized", "date", "interval"):
#			columnName = "%s_hive"%(columnName)
#
#		return columnName
#
#	def saveColumnDataOLD(self, ):
#		# This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
#		# and insert/update the data in the import_columns table. This also takes care of column type conversions if it's needed
#		logging.debug("")
#		logging.debug("Executing import_config.saveColumnData()")
#		logging.info("Saving column data to MySQL table - import_columns")
#
#		# IS_NULLABLE
#		# SCHEMA_NAME
#		# SOURCE_COLUMN_COMMENT
#		# SOURCE_COLUMN_NAME
##		# SOURCE_COLUMN_TYPE
##		# TABLE_COMMENT
##		# TABLE_NAME
##		self.sqlGeneratedHiveColumnDefinition = ""
#		self.sqlGeneratedSqoopQuery = ""
##		self.sqoop_mapcolumnjava=[]
#		columnOrder = 0
#		self.sqoop_use_generated_sql = False
#
#		for index, row in self.common_config.source_columns_df.iterrows():
##			column_name = self.stripUnwantedCharColumnName(row['SOURCE_COLUMN_NAME']).lower()
#			column_name = self.stripUnwantedCharColumnName(row['SOURCE_COLUMN_NAME'])
#			column_type = row['SOURCE_COLUMN_TYPE'].lower()
#			source_column_type = column_type
#			source_column_name = row['SOURCE_COLUMN_NAME']
#			source_column_comment = self.stripUnwantedCharComment(row['SOURCE_COLUMN_COMMENT'])
#			self.table_comment = self.stripUnwantedCharComment(row['TABLE_COMMENT'])
#
#			# Get the settings for this specific column
#			query  = "select "
#			query += "	column_id, "
#			query += "	include_in_import, "
#			query += "	column_name_override, "
#			query += "	column_type_override "
#			query += "from import_columns where table_id = %s and source_column_name = %s "
#			self.mysql_cursor01.execute(query, (self.table_id, source_column_name))
#			logging.debug("SQL Statement executed: \n%s" % (self.mysql_cursor01.statement) )
#
#			includeColumnInImport = True
#			columnID = None
#			columnTypeOverride = None
#
#			columnRow = self.mysql_cursor01.fetchone()
#			if columnRow != None:
#				columnID = columnRow[0]
#				if columnRow[1] == 0:
#					includeColumnInImport = False
#				if columnRow[2] != None and columnRow[2].strip() != "":
#					column_name = columnRow[2].strip().lower()
#				if columnRow[3] != None and columnRow[3].strip() != "":
#					columnTypeOverride = columnRow[3].strip().lower()
#
#			# Handle reserved column names in Hive
#			column_name = self.convertHiveColumnNameReservedWords(column_name)
#
#			# TODO: Get the maximum column comment size from Hive Metastore and use that instead
#			if source_column_comment != None and len(source_column_comment) > 256:
#				source_column_comment = source_column_comment[:256]
#
#			columnOrder += 1
#
#			# sqoop_column_type is just an information that will be stored in import_columns. We might be able to remove this. TODO. Verify
#			sqoop_column_type = None
#
#			if self.common_config.db_mssql == True:
#				# If source type is BIT, we convert them to INTEGER
#				if source_column_type == "bit": 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=Integer")
#					sqoop_column_type = "Integer"
#			
#				if source_column_type == "float": 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=Float")
#					sqoop_column_type = "Float"
#			
#				if source_column_type in ("uniqueidentifier", "ntext", "xml", "text", "nvarchar(-1)"): 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					sqoop_column_type = "String"
#					column_type = "string"
#
#				column_type = re.sub('^nvarchar\(', 'varchar(', column_type)
#				column_type = re.sub('^datetime$', 'timestamp', column_type)
#				column_type = re.sub('^datetime2$', 'timestamp', column_type)
#				column_type = re.sub('^smalldatetime$', 'timestamp', column_type)
#				column_type = re.sub('^bit$', 'tinyint', column_type)
#				column_type = re.sub('^binary\([0-9]*\)$', 'binary', column_type)
#				column_type = re.sub('^varbinary$', 'binary', column_type)
#				column_type = re.sub('^varbinary\([0-9]*\)$', 'binary', column_type)
#				column_type = re.sub('^geometry$', 'binary', column_type)
#				column_type = re.sub('^image$', 'binary', column_type)
#				column_type = re.sub('^money$', 'decimal', column_type)
#				column_type = re.sub('^nchar\(', 'char(', column_type)
#				column_type = re.sub('^numeric\(', 'decimal(', column_type)
#				column_type = re.sub('^real$', 'float', column_type)
#				column_type = re.sub('^time$', 'string', column_type)
#				column_type = re.sub('^varchar\(-1\)$', 'string', column_type)
#				column_type = re.sub('^varchar\(65355\)$', 'string', column_type)
#				column_type = re.sub('^smallmoney$', 'float', column_type)
#
#			if self.common_config.db_oracle == True:
#				# If source type is BIT, we convert them to INTEGER
#				if source_column_type.startswith("rowid("): 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					sqoop_column_type = "String"
#			
#				if source_column_type.startswith("sdo_geometry("): 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					sqoop_column_type = "String"
#					column_type = "binary"
#			
#				if source_column_type.startswith("jtf_pf_page_object("): 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					sqoop_column_type = "String"
#					column_type = "binary"
#			
#				if source_column_type.startswith("wf_event_t("): 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					sqoop_column_type = "String"
#					column_type = "binary"
#			
#				if source_column_type.startswith("ih_bulk_type("): 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					sqoop_column_type = "String"
#					column_type = "binary"
#			
#				if source_column_type.startswith("anydata("): 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					sqoop_column_type = "String"
#					column_type = "binary"
#			
#				if source_column_type in ("clob", "nclob", "nlob", "long raw"): 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					sqoop_column_type = "String"
#					column_type = "string"
#
#				if source_column_type.startswith("float"): 
##					self.sqoop_mapcolumnjava.append(source_column_name + "=Float")
#					sqoop_column_type = "Float"
#			
#				column_type = re.sub('^nvarchar\(', 'varchar(', column_type)
#				column_type = re.sub(' char\)$', ')', column_type)
#				column_type = re.sub(' byte\)$', ')', column_type)
#				column_type = re.sub('^char\(0\)$', 'char(4000)', column_type)
#				column_type = re.sub('^varchar2\(0\)$', 'varchar(4000)', column_type)
#				column_type = re.sub('^varchar2\(', 'varchar(', column_type)
#				column_type = re.sub('^nvarchar2\(', 'varchar(', column_type)
#				column_type = re.sub('^rowid\(', 'varchar(', column_type)
#				column_type = re.sub('^number$', 'decimal(38,19)', column_type)
#				if re.search('^number\([0-9]\)', column_type):
#					column_type = "int"
#					self.sqoop_mapcolumnjava.append(source_column_name + "=Integer")
#					sqoop_column_type = "Integer"
#				if re.search('^number\(1[0-8]\)', column_type):
#					column_type = "bigint"
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					sqoop_column_type = "String"
#				column_type = re.sub('^number\(', 'decimal(', column_type)
#				column_type = re.sub('^date$', 'timestamp', column_type)
#				column_type = re.sub('^timestamp\([0-9]*\) with time zone', 'timestamp', column_type)
#				column_type = re.sub('^blob$', 'binary', column_type)
##				column_type = re.sub('^clob$', 'string', column_type)
##				column_type = re.sub('^nclob$', 'string', column_type)
##				column_type = re.sub('^nlob$', 'string', column_type)
#				column_type = re.sub('^long$', 'binary', column_type)
#				column_type = re.sub('^xmltype\([0-9]*\)$', 'string', column_type)
#				column_type = re.sub('^raw$', 'binary', column_type)
#				column_type = re.sub('^raw\([0-9]*\)$', 'binary', column_type)
#				column_type = re.sub('^timestamp\([0-9]\)', 'timestamp', column_type)
##				column_type = re.sub('^long raw$', 'string', column_type)
#				column_type = re.sub('^long raw\([0-9]*\)$', 'string', column_type)
#				column_type = re.sub('^decimal\(3,4\)', 'decimal(8,4)', column_type)    # Very stange Oracle type number(3,4), how can pricision be smaller than scale?
#				if re.search('^decimal\([0-9][0-9]\)$', column_type) != None:
#					column_type = re.sub('\)$', ',0)', column_type)
#					
#			if self.common_config.db_mysql == True:
#				if source_column_type == "bit": 
#					column_type = "tinyint"
#					self.sqoop_mapcolumnjava.append(source_column_name + "=Integer")
#					sqoop_column_type = "Integer"
#
#				if source_column_type in ("smallint", "mediumint"): 
#					column_type = "int"
#					self.sqoop_mapcolumnjava.append(source_column_name + "=Integer")
#					sqoop_column_type = "Integer"
#		
##				column_type = re.sub('^character varying\(', 'varchar(', column_type)
#				column_type = re.sub('^mediumtext\([0-9]*\)', 'string', column_type)
#				column_type = re.sub('^tinytext\([0-9]*\)', 'varchar(255)', column_type)
##				column_type = re.sub('^text\([0-9]*\)', 'string', column_type)
#				column_type = re.sub('^datetime$', 'timestamp', column_type)
#				column_type = re.sub('^varbinary$', 'binary', column_type)
#				column_type = re.sub('^varbinary\([0-9]*\)$', 'binary', column_type)
#				if column_type == "time": 
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					column_type = "string"
#					sqoop_column_type = "String"
#
#			if self.common_config.db_postgresql == True:
#				column_type = re.sub('^character varying\(', 'varchar(', column_type)
#				column_type = re.sub('^text', 'string', column_type)
#
#			if self.common_config.db_progress == True:
#				column_type = re.sub('^integer', 'int', column_type)
#				column_type = re.sub('^numeric\(', 'decimal(', column_type)
#				column_type = re.sub('^date\([0-9]\)', 'date', column_type)
#				column_type = re.sub('^bit\([1]\)', 'boolean', column_type)
#				column_type = re.sub(',none\)', ',0)', column_type)
#
#			if self.common_config.db_db2udb == True:
#				column_type = re.sub('^integer', 'int', column_type)
#				column_type = re.sub('^timestmp', 'timestamp', column_type)
#				column_type = re.sub('^blob', 'binary', column_type)
#				column_type = re.sub('^clob', 'string', column_type)
#				column_type = re.sub('^real', 'float', column_type)
#				column_type = re.sub('^vargraph', 'varchar', column_type)
#				column_type = re.sub('^graphic', 'varchar', column_type)
#				column_type = re.sub('^time\([0-9]\)', 'timestamp', column_type)
#
#			if self.common_config.db_db2as400 == True:
#				column_type = re.sub('^integer', 'int', column_type)
#				column_type = re.sub('^timestmp', 'timestamp', column_type)
#				column_type = re.sub('^timestamp\(.*\)', 'timestamp', column_type)
#				column_type = re.sub('^varbinary$', 'binary', column_type)
#				column_type = re.sub('^varbinary\([0-9]*\)$', 'binary', column_type)
#				column_type = re.sub('^blob', 'binary', column_type)
#				column_type = re.sub('^real', 'float', column_type)
#				if re.search('^numeric\(', column_type):
#					column_type = re.sub('^numeric\(', 'decimal(', column_type)
#					column_type = re.sub('\)$', ',0)', column_type)
#				if re.search('^clob', column_type):
#					column_type = "string"
#					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#					sqoop_column_type = "String"
#
#			if self.common_config.db_mongodb == True:
#				column_type = re.sub(':null', ':string', column_type)
#				column_type = re.sub('^null$', 'string', column_type)
#
#			# Hive only allow max 255 in size for char's. If we get a char that is larger than 255, we convert it to a varchar
#			if column_type.startswith("char("):
#				column_precision = int(column_type.split("(")[1].split(")")[0])
#				if column_precision > 255:
#					column_type = re.sub('^char\(', 'varchar(', column_type)
#
#			# Remove precision from datatypes that doesnt include a precision
#			column_type = re.sub('^float\([0-9]*\)', 'float', column_type)
#			column_type = re.sub('^bigint\([0-9]*\)', 'bigint', column_type)
#			column_type = re.sub('^int\([0-9]*\)', 'int', column_type)
#				
#			if columnTypeOverride != None:
#				column_type = columnTypeOverride
#
#			# As Parquet imports some column types wrong, we need to map them all to string
#			if column_type in ("timestamp", "date", "bigint"): 
#				self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#				sqoop_column_type = "String"
#			if re.search('^decimal\(', column_type):
#				self.sqoop_mapcolumnjava.append(source_column_name + "=String")
#				sqoop_column_type = "String"
#
#			if includeColumnInImport == True:
#				# Add , between column names in the list
#				if len(self.sqlGeneratedHiveColumnDefinition) > 0: self.sqlGeneratedHiveColumnDefinition = self.sqlGeneratedHiveColumnDefinition + ", "
#			# Add the column to the sqlGeneratedHiveColumnDefinition variable. This will be the base for the auto generated SQL
#				self.sqlGeneratedHiveColumnDefinition += "`" + column_name + "` " + column_type 
#				if source_column_comment != None:
#					self.sqlGeneratedHiveColumnDefinition += " COMMENT '" + source_column_comment + "'"
#
#			# Add the column to the SQL query that can be used by sqoop
#				column_name_parquet_supported = self.getParquetColumnName(column_name)
#				quote = self.common_config.getQuoteAroundColumn()
#
#				if len(self.sqlGeneratedSqoopQuery) == 0: 
#					self.sqlGeneratedSqoopQuery = "select "
#				else:
#					self.sqlGeneratedSqoopQuery += ", "
#				
#				if source_column_name != column_name_parquet_supported:
#					if re.search('"', source_column_name):
#						self.sqlGeneratedSqoopQuery += "'" + source_column_name + "' as \"" + column_name_parquet_supported + "\""
#					else:
#						self.sqlGeneratedSqoopQuery += quote + source_column_name + quote + " as " + quote + column_name_parquet_supported + quote
#					self.sqoop_use_generated_sql = True
#				else:
#					self.sqlGeneratedSqoopQuery += quote + source_column_name + quote
#
#			# Fetch if we should force this column to 'string' in Hive
#			columnForceString = self.getColumnForceString(column_name)
#			if columnForceString == True:
#				if column_type.startswith("char(") == True or column_type.startswith("varchar("):
#					column_type = "string"
#
#			# Run a query to see if the column already exists. Will be used to determine if we do an insert or update
##			query = "select column_id from import_columns where table_id = %s and source_column_name = %s "
##			self.mysql_cursor01.execute(query, (self.table_id, source_column_name))
##			logging.debug("SQL Statement executed: \n%s" % (self.mysql_cursor01.statement) )
#
##			if self.mysql_cursor01.rowcount == 0:
#			if columnID == None:
#				query = ("insert into import_columns "
#						"("
#						"    table_id,"
#						"    hive_db,"
#						"    hive_table,"
#						"    column_name,"
#						"    column_order,"
#						"    source_column_name,"
#						"    column_type,"
#						"    source_column_type,"
#						"    source_database_type,"
#						"    sqoop_column_type,"
#						"    last_update_from_source,"
#						"    comment"
#						") values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )")
#	
#				self.mysql_cursor01.execute(query, (self.table_id, self.Hive_DB, self.Hive_Table, column_name.lower(), columnOrder, source_column_name, column_type, source_column_type, self.common_config.jdbc_servertype, sqoop_column_type, self.startDate, source_column_comment))
#				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#			else:
#				query = ("update import_columns set "
#						"    hive_db = %s, "
#						"    hive_table = %s, "
#						"    column_name = %s, "
#						"    column_order = %s, "
#						"    column_type = %s, "
#						"    source_column_type = %s, "
#						"    source_database_type = %s, "
#						"    sqoop_column_type = %s, "
#						"    source_primary_key = NULL, "
#						"    last_update_from_source = %s, "
#						"    comment = %s "
#						"where table_id = %s and source_column_name = %s ")
#
#				self.mysql_cursor01.execute(query, (self.Hive_DB, self.Hive_Table, column_name.lower(), columnOrder, column_type, source_column_type, self.common_config.jdbc_servertype, sqoop_column_type, self.startDate, source_column_comment, self.table_id, source_column_name))
#				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#
#				if self.common_config.post_column_data == True:
#					jsonData = {}
#					jsonData["type"] = "column_data"
#					jsonData["date"] = self.startDate 
#					jsonData["source_database_server_type"] = self.common_config.jdbc_servertype 
#					jsonData["source_database_server"] = self.common_config.jdbc_hostname
#					jsonData["source_database"] = self.common_config.jdbc_database
##					jsonData["source_schema"] = self.source_schema
#					jsonData["source_table"] = self.source_table
#					jsonData["hive_db"] = self.Hive_DB
#					jsonData["hive_table"] = self.Hive_Table
#					jsonData["column"] = column_name.lower()
#					jsonData["source_column"] = source_column_name
#					jsonData["source_column_type"] = source_column_type
#					jsonData["column_type"] = column_type
#	
#					logging.debug("Sending the following JSON to the REST interface: %s"% (json.dumps(jsonData, sort_keys=True, indent=4)))
#					response = self.rest.sendData(json.dumps(jsonData))
#					if response != 200:
#						# There was something wrong with the REST call. So we save it to the database and handle it later
#						logging.debug("REST call failed!")
#						logging.debug("Saving the JSON to the json_to_rest table instead")
#						query = "insert into json_to_rest (type, status, jsondata) values ('import_column', 0, %s)"
#						self.mysql_cursor01.execute(query, (json.dumps(jsonData), ))
#						logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#				
#		# Commit all the changes to the import_column column
#		self.mysql_conn.commit()
#
#		# Add the source the to the generated sql query
#		self.sqlGeneratedSqoopQuery += " from %s"%(self.common_config.getJDBCsqlFromTable(schema=self.source_schema, table=self.source_table))
#		
#		# Add ( and ) to the Hive column definition so it contains a valid string for later use in the solution
#		self.sqlGeneratedHiveColumnDefinition = "( " + self.sqlGeneratedHiveColumnDefinition + " )"
#
#		logging.debug("Settings from import_config.saveColumnData()")
###		logging.debug("    sqlGeneratedSqoopQuery = %s"%(self.sqlGeneratedSqoopQuery))
#		logging.debug("    sqlGeneratedHiveColumnDefinition = %s"%(self.sqlGeneratedHiveColumnDefinition))
##		logging.debug("Executing import_config.saveColumnData() - Finished")
#
#	def getParquetColumnName(self, column_name):
#		
#		column_name = (column_name.lower() 
#			.replace(' ', '_')
#			.replace('%', 'pct')
#			.replace('(', '_')
#			.replace(')', '_')
#			.replace('ü', 'u')
#			.replace('å', 'a')
#			.replace('ä', 'a')
#			.replace('ö', 'o')
#			.replace('`', '')
#			.replace('\'', '')
#			.replace(';', '')
#			.replace('\n', '')
#			.replace('\\', '')
#			.replace('’', '')
#			.replace(':', '')
#			.replace(',', '')
#			.replace('.', '')
#			.replace('"', '')
#			)
#		return column_name
#
#	def setPrimaryKeyColumn(self, ):
#		# This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
#		# and update the source_primary_key column in the import_columns table with the information on what key is part of the PK
#		logging.debug("")
#		logging.debug("Executing import_config.setPrimaryKeyColumn()")
#		logging.info("Setting PrimaryKey information in MySQL table - import_columns")
#
#		# COL_DATA_TYPE
#		# COL_KEY_POSITION
#		# COL_NAME
#		# CONSTRAINT_NAME
#		# CONSTRAINT_TYPE
#		# REFERENCE_COL_NAME
#		# REFERENCE_SCHEMA_NAME
#		# REFERENCE_TABLE_NAME
#		# SCHEMA_NAME
#		# TABLE_NAME
#
#		self.generatedPKcolumns = ""
#
#		for index, row in self.common_config.source_keys_df.iterrows():
#			key_type = row['CONSTRAINT_TYPE']
#			key_id = row['COL_KEY_POSITION']
#			column_name = self.stripUnwantedCharColumnName(row['COL_NAME'])
#
#			# Handle reserved column names in Hive
#			if column_name == "date": column_name = column_name + "_HIVE"
#			if column_name == "interval": column_name = column_name + "_HIVE"
#
## TODO: Loop through only PK's in the for loop when external Python code is completed. Same as we do for FK's
#			# Determine what the PK is called in the Pandas dataframe for each database type
#			key_type_reference = None
#			if self.common_config.db_mssql == True:      key_type_reference = "PRIMARY_KEY_CONSTRAINT"
##			if self.common_config.db_oracle == True:     key_type_reference = "P"
##			if self.common_config.db_mysql == True:      key_type_reference = "PRIMARY"
##			if self.common_config.db_postgresql == True: key_type_reference = "p"
#
#			if key_type_reference == None: key_type_reference = constant.PRIMARY_KEY
#
#			# As we only work with PK in this function, we ignore all other kind of keys (thats usually Foreign Keys we ignore)
#			if key_type != key_type_reference: continue
#
#			logging.debug("	key_type: %s" % (key_type))	
#			logging.debug("	key_type_reference: %s" % (key_type_reference))	
#			logging.debug("	key_id: %s" % (key_id))	
#			logging.debug("	column_name: %s" % (column_name))	
#
#			# Append the column_name to the generated PK list.
#			if len(self.generatedPKcolumns) > 0: self.generatedPKcolumns += ","
#			self.generatedPKcolumns += column_name
#
#			query = ("update import_columns set "
#					"    source_primary_key = %s "
#					"where table_id = %s and lower(source_column_name) = %s ")
#
#			self.mysql_cursor01.execute(query, (key_id, self.table_id, column_name))
#			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#				
#		# Commit all the changes to the import_column column
#		self.mysql_conn.commit()
#
#		logging.debug("Executing import_config.setPrimaryKeyColumn() - Finished")
#
#	def saveKeyData(self, ):
#		# This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
#		# and update the import_foreign_keys table with the information on what FK's are available for the table
#		logging.debug("")
#		logging.debug("Executing import_config.saveKeyData()")
##		logging.info("Setting ForeignKey information in MySQL table - import_foreign_keys")
#
#		# COL_DATA_TYPE
#		# COL_KEY_POSITION
#		# COL_NAME
#		# CONSTRAINT_NAME
#		# CONSTRAINT_TYPE
#		# REFERENCE_COL_NAME
#		# REFERENCE_SCHEMA_NAME
#		# REFERENCE_TABLE_NAME
#		# SCHEMA_NAME
#		# TABLE_NAME
#
#
#		key_type_reference = ""
##		column_name_list = ""
##		ref_column_name_list = ""
#		fk_index = 1
#		fk_index_counter = 1
#		fk_index_dict = {}
#
#		# Set the correct string for a FK key in different database types
##		if self.common_config.db_mssql == True:      key_type_reference = "FK"
##		if self.common_config.db_oracle == True:     key_type_reference = "R"
##		if self.common_config.db_mysql == True:      key_type_reference = "FK"
##		if self.common_config.db_postgresql == True: key_type_reference = "f"
#		if key_type_reference == "": key_type_reference = constant.FOREIGN_KEY
#
#		if self.common_config.source_keys_df.empty == True: return
#
#		# Create a new DF that only contains FK's
#		source_fk_df = self.common_config.source_keys_df[self.common_config.source_keys_df['CONSTRAINT_TYPE'] == key_type_reference]
#
#		# Loop through all unique ForeignKeys that is presented in the source.
##		for source_fk in source_fk_df[source_fk_df['CONSTRAINT_TYPE'] == key_type_reference].CONSTRAINT_NAME.unique():
#		for source_fk in source_fk_df.CONSTRAINT_NAME.unique():
#			logging.debug("Parsing FK with name '%s'"%(source_fk))
#		
#			# Iterate over the rows that matches the FK name
#			for index, row in source_fk_df[source_fk_df['CONSTRAINT_NAME'] == source_fk].iterrows():
##				print("Key row: %s"%(row))
## #SCHEMA_NAME|TABLE_NAME|CONSTRAINT_NAME|CONSTRAINT_TYPE|COL_NAME|COL_DATA_TYPE|REFERENCE_SCHEMA_NAME|REFERENCE_TABLE_NAME|REFERENCE_COL_NAME|COL_KEY_POSITION
#				source_fk_name = row['CONSTRAINT_NAME']
#				column_name = row['COL_NAME']
#				ref_schema_name = row['REFERENCE_SCHEMA_NAME']
#				ref_table_name = row['REFERENCE_TABLE_NAME']
#				ref_column_name = row['REFERENCE_COL_NAME']
#				key_position = row['COL_KEY_POSITION']
#
#				# If we already worked with this FK, we fetch the fk_index from the dictinary.
#				# If not, we add it to the dictionary and increase the counter so the next FK gets a higher index number.
#				if source_fk_name in fk_index_dict:
##					fk_index = fk_index_dict[source_fk_name]
#				else:
##					fk_index_dict[source_fk_name] = fk_index_counter
#					fk_index = fk_index_counter
#					fk_index_counter += 1
#			
#				# MySQL dont have schemas. So we make them default to dash
#				if self.common_config.db_mysql == True:	ref_schema_name = "-"
#
#				# Select table_id's that matches the source based on dbalias, schema and table name
#				# This can be many table_id's as we might have imported the same source multiple times
				# under different names.
##				# TODO: Right now, we only support reference to one table per FK. If we have imported the same table more than once
#				# the FK will add with primary key validation. To get this error, remove the "limit 1" in the sql statement and import
#				# the same source table twice
#				query = "select table_id from import_tables where dbalias = %s and source_schema = %s and source_table = %s limit 1"
#				self.mysql_cursor01.execute(query, (self.connection_alias, ref_schema_name, ref_table_name ))
#				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#
##				if self.mysql_cursor01.rowcount == 0:
#					logging.warning("Reference table '%s.%s' on alias '%s' does not exist in MySQL. Skipping FK on this table"%(ref_schema_name, ref_table_name, self.connection_alias))
##				else:
##					rows = self.mysql_cursor01.fetchall()
#					for ref_table_id in rows:
#						# Looping through all table_id's that matches the source from the previous select
#						logging.debug("Parsing referenced table with table_id: %s"%(ref_table_id[0]))
#
#						# Fetch the column_id for the table own column
#						query = "select column_id from import_columns where table_id = %s and lower(source_column_name) = %s"
#						self.mysql_cursor02.execute(query, (self.table_id, column_name ))
#						logging.debug("SQL Statement executed: %s" % (self.mysql_cursor02.statement) )
#						row = self.mysql_cursor02.fetchone()
#						source_column_id = row[0]
#
#						# Fetch the column_id for the column in the referensed table
#						query = "select column_id from import_columns where table_id = %s and lower(source_column_name) = %s"
#						self.mysql_cursor02.execute(query, (ref_table_id[0], ref_column_name ))
#						logging.debug("SQL Statement executed: %s" % (self.mysql_cursor02.statement) )
#						row = self.mysql_cursor02.fetchone()
#
#						if self.mysql_cursor02.rowcount == 0:
#							logging.warning("Referenced column '%s' in '%s.%s' cant be found in MySQL. Skipping FK on this table"%(ref_column_name, ref_schema_name, ref_table_name))
#							ref_column_id = None
#						else:
#		#					row = self.mysql_cursor02.fetchone()
#							ref_column_id = row[0]
#
#							# Save the FK data to the MySQL table
#							query = ("insert into import_foreign_keys "
#									"("
#									"    table_id, "
#									"    column_id, "
#									"    fk_index, "
#									"    fk_table_id, "
#									"    fk_column_id, "
#									"    key_position "
#									") values ( %s, %s, %s, %s, %s, %s )")
#
#							logging.debug("self.table_id:    %s"%(self.table_id))
#							logging.debug("source_column_id: %s"%(source_column_id))
#							logging.debug("fk_index:         %s"%(fk_index))
#							logging.debug("ref_table_id[0]:  %s"%(ref_table_id[0]))
#							logging.debug("ref_column_id:    %s"%(ref_column_id))
#							logging.debug("key_position:     %s"%(key_position))
#							self.mysql_cursor02.execute(query, (self.table_id, source_column_id, fk_index, ref_table_id[0], ref_column_id, key_position  ))
#							self.mysql_conn.commit()
#							logging.debug("SQL Statement executed: %s" % (self.mysql_cursor02.statement) )
#
#		logging.debug("Executing import_config.saveKeyData() - Finished")
#
#	def saveGeneratedData(self):
#		# This will save data to the generated* columns in import_table
#		logging.debug("")
#		logging.debug("Executing import_config.saveGeneratedData()")
#		logging.info("Saving generated data to MySQL table - import_table")
#
#		# Create a valid self.generatedSqoopOptions value
#		self.generatedSqoopOptions = None
#		if len(self.sqoop_mapcolumnjava) > 0:
#			self.generatedSqoopOptions = "--map-column-java "
#			for column_map in self.sqoop_mapcolumnjava:
#				column, value=column_map.split("=")
#
#				column = self.convertHiveColumnNameReservedWords(column)
#				if column.lower() in ("synchronized"):
##					column = column + "_HIVE"

#				self.generatedSqoopOptions += ("%s=%s,"%(self.getParquetColumnName(column), value)) 
#			# Remove the last ","
#			self.generatedSqoopOptions = self.generatedSqoopOptions[:-1]
#			
#		if self.generatedPKcolumns == "": self.generatedPKcolumns = None
#
#		if self.sqoop_use_generated_sql == True:
#			sqoop_use_generated_sql_local = 1
#		else:
#			sqoop_use_generated_sql_local = 0
#
#		query = ("update import_tables set "
#				"    generated_hive_column_definition = %s, "
#				"    generated_sqoop_query = %s, "
#				"    comment = %s, "
#				"    generated_sqoop_options = %s, "
#				"    generated_pk_columns = %s, "
#				"    generated_foreign_keys = NULL, "
#				"    sqoop_use_generated_sql = %s "
#				"where table_id = %s ")
#
#		self.mysql_cursor01.execute(query, (self.sqlGeneratedHiveColumnDefinition, self.sqlGeneratedSqoopQuery, self.table_comment, self.generatedSqoopOptions, self.generatedPKcolumns, sqoop_use_generated_sql_local, self.table_id))
#		self.mysql_conn.commit()
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#				
#		logging.debug("Executing import_config.saveGeneratedData() - Finished")
#
#	def getIncrWhereStatement_OLD(self, forceIncr=False, ignoreIfOnlyIncrMax=False, whereForSourceTable=False, whereForSqoop=False):
#		""" Returns the where statement that is needed to only work on the rows that was loaded incr """
#		logging.debug("Executing import_config.getIncrWhereStatement()")
#
#		whereStatement = None
#
#		if self.import_is_incremental == True:
#			if whereForSqoop == True:
#				# If it is the where for sqoop, we need to get the max value for the configured column and save that into the config database
##				maxValue = self.common_config.getJDBCcolumnMaxValue(self.source_schema, self.source_table, self.sqoop_incr_column)
#				if self.sqoop_incr_mode == "append":
#					self.sqoopIncrMaxvaluePending = maxValue
#				if self.sqoop_incr_mode == "lastmodified":
#					if re.search('\.([0-9]{3}|[0-9]{6})$', maxValue):
#						self.sqoopIncrMaxvaluePending = datetime.strptime(maxValue, '%Y-%m-%d %H:%M:%S.%f')
#					else:
#						self.sqoopIncrMaxvaluePending = datetime.strptime(maxValue, '%Y-%m-%d %H:%M:%S')
#
#					if self.common_config.jdbc_servertype == constant.MSSQL:
#						#MSSQL gets and error if there are microseconds in the timestamp
#						(dt, micro) = self.sqoopIncrMaxvaluePending.strftime('%Y-%m-%d %H:%M:%S.%f').split(".")
#						self.sqoopIncrMaxvaluePending = "%s.%03d" % (dt, int(micro) / 1000)
#
#				self.sqoopIncrMinvaluePending = self.sqoop_incr_lastvalue
#
#				query = ("update import_tables set incr_minvalue_pending = %s, incr_maxvalue_pending = %s where table_id = %s")
#				self.mysql_cursor01.execute(query, (self.sqoopIncrMinvaluePending, self.sqoopIncrMaxvaluePending, self.table_id))
#				self.mysql_conn.commit()
#				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#
#			if self.sqoopIncrMaxvaluePending == None:
#				# COALESCE is needed if you are going to call a function that counts source target rows outside the normal import
#				query = ("select COALESCE(incr_maxvalue_pending, incr_maxvalue) from import_tables where table_id = %s")
#				self.mysql_cursor01.execute(query, (self.table_id, ))
#				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#	
#				row = self.mysql_cursor01.fetchone()
#				self.sqoopIncrMaxvaluePending = row[0]
#
#			if self.sqoop_incr_mode == "lastmodified":
#				if self.common_config.jdbc_servertype == constant.MSSQL and ( whereForSourceTable == True or whereForSqoop == True ):
#					whereStatement = "%s <= CONVERT(datetime, '%s', 121) "%(self.sqoop_incr_column, self.sqoopIncrMaxvaluePending) 
#				elif self.common_config.jdbc_servertype == constant.MYSQL and ( whereForSourceTable == True or whereForSqoop == True ):
#					whereStatement = "%s <= STR_TO_DATE('%s', "%(self.sqoop_incr_column, self.sqoopIncrMaxvaluePending) + "'%Y-%m-%d %H:%i:%s') "
#				elif whereForSourceTable == True or whereForSqoop == True:
#					whereStatement = "%s <= TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF') "%(self.sqoop_incr_column, self.sqoopIncrMaxvaluePending) 
#				else:
#					whereStatement = "%s <= '%s' "%(self.sqoop_incr_column, self.sqoopIncrMaxvaluePending) 
#			else:
#				whereStatement = "%s <= %s "%(self.sqoop_incr_column, self.sqoopIncrMaxvaluePending) 
#
#			whereStatementSaved = whereStatement
#
#			if self.sqoop_sql_where_addition != None and self.sqoop_sql_where_addition.strip() != "":
#				whereStatement += "and %s "%(self.sqoop_sql_where_addition)
#
#			if ( self.sqoop_incr_validation_method == "incr" or forceIncr == True or whereForSqoop == True ) and self.sqoop_incr_lastvalue != None:
#				if self.sqoop_incr_mode == "lastmodified":
#					if self.common_config.jdbc_servertype == constant.MSSQL and ( whereForSourceTable == True or whereForSqoop == True ):
#						whereStatement += "and %s > CONVERT(datetime, '%s', 121) "%(self.sqoop_incr_column, self.sqoop_incr_lastvalue)
#					elif self.common_config.jdbc_servertype == constant.MYSQL and ( whereForSourceTable == True or whereForSqoop == True ):
##						whereStatement += "and %s > STR_TO_DATE('%s', "%(self.sqoop_incr_column, self.sqoop_incr_lastvalue) + "'%Y-%m-%d %H:%i:%s') "
#					elif whereForSourceTable == True or whereForSqoop == True:
#						whereStatement += "and %s > TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF') "%(self.sqoop_incr_column, self.sqoop_incr_lastvalue) 
#					else:
##						whereStatement += "and %s > '%s' "%(self.sqoop_incr_column, self.sqoop_incr_lastvalue) 
#				else:
#					whereStatement += "and %s > %s "%(self.sqoop_incr_column, self.sqoop_incr_lastvalue)
#			
#			if whereStatementSaved == whereStatement and ignoreIfOnlyIncrMax == True:
#				# We have only added the MAX value to there WHERE statement and have a setting to ignore it if that is the only value
#				whereStatement = None
#
#		logging.debug("whereStatement: %s"%(whereStatement))
#		logging.debug("Executing import_config.getIncrWhereStatement() - Finished")
#		return whereStatement
##
#	def getHiveTableComment(self,):
#		""" Returns the table comment stored in import_tables.comment """
#		logging.debug("Executing import_config.getHiveTableComment()")
#		hiveColumnDefinition = ""
#
#		query  = "select comment from import_tables where table_id = %s "
#		self.mysql_cursor01.execute(query, (self.table_id, ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#
#		if self.mysql_cursor01.rowcount == 0:
#			logging.error("Error: Zero rows returned from query on 'import_table'")
#			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor01.statement) )
#			raise Exception
#
#		row = self.mysql_cursor01.fetchone()
#				
#		logging.debug("Executing import_config.getHiveTableComment() - Finished")
#		return row[0]
#
#	def getPKcolumns(self, PKforMerge=False):
#		""" Returns a comma seperated list of columns that is part of the PK """
#		logging.debug("Executing import_config.getPKcolumns()")
#		returnValue = None
#
#		# First fetch the override if it exists
#		query  = "select pk_column_override, pk_column_override_mergeonly from import_tables where table_id = %s "
#		self.mysql_cursor01.execute(query, (self.table_id, ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#
#		row = self.mysql_cursor01.fetchone()
#		self.pk_column_override = row[0]
#		self.pk_column_override_mergeonly = row[1]
#
#		if self.pk_column_override != None and self.pk_column_override.strip() != "" and PKforMerge == False:
#			logging.debug("Executing import_config.getPKcolumns() - Finished")
#			self.pk_column_override = re.sub(', *', ',', self.pk_column_override.strip())
#			self.pk_column_override = re.sub(' *,', ',', self.pk_column_override)
#			return self.pk_column_override
#
#		if self.pk_column_override_mergeonly != None and self.pk_column_override_mergeonly.strip() != "" and PKforMerge == True:
#			logging.debug("Executing import_config.getPKcolumns() - Finished")
#			self.pk_column_override_mergeonly = re.sub(', *', ',', self.pk_column_override_mergeonly.strip())
#			self.pk_column_override_mergeonly = re.sub(' *,', ',', self.pk_column_override_mergeonly)
#			return self.pk_column_override_mergeonly
#
#		# If we reach this part, it means that we didnt override the PK. So lets fetch the real one	
#		query  = "select c.source_column_name "
#		query += "from import_tables t "
#		query += "join import_columns c on t.table_id = c.table_id "
#		query += "where t.table_id = %s and c.source_primary_key is not null and t.last_update_from_source = c.last_update_from_source "
#		query += "order by c.source_primary_key "
#
#		self.mysql_cursor01.execute(query, (self.table_id, ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#
#		for row in self.mysql_cursor01.fetchall():
#			if returnValue == None:
#				returnValue = str(row[0].lower())
#			else:
#				returnValue += "," + str(row[0].lower()) 
#
#		logging.debug("Executing import_config.getPKcolumns() - Finished")
#		return returnValue
#
#	def getForeignKeysFromConfig(self,):
#		""" Reads the ForeignKeys from the configuration tables and return the result in a Pandas DF """
#		logging.debug("Executing import_config.getForeignKeysFromConfig()")
#		result_df = None
#
#		query  = "select "
#		query += "   s.hive_db as source_hive_db, "
#		query += "   s.hive_table as source_hive_table, " 
#		query += "   lower(s.column_name) as source_column_name, "
#		query += "   lower(s.source_column_name) as source_source_column_name, "
#		query += "   refT.source_schema as ref_source_schema, "
#		query += "   refT.source_table as ref_source_table, "
#		query += "   ref.column_name as ref_column_name, "
#		query += "   lower(ref.source_column_name) as ref_source_column_name, "
#		query += "   ref.hive_db as ref_hive_db, "
#		query += "   ref.hive_table as ref_hive_table, "
#		query += "   fk.fk_index as fk_index, "
#		query += "   fk.key_position as key_position "
#		query += "from import_foreign_keys fk "
#		query += "   join import_columns s on s.table_id = fk.table_id and s.column_id = fk.column_id "
#		query += "   join import_columns ref on ref.table_id = fk.fk_table_id and ref.column_id = fk.fk_column_id "
#		query += "   join import_tables refT on fk.fk_table_id = refT.table_id "
#		query += "where fk.table_id = %s "
#		query += "order by fk.fk_index, fk.key_position "
#
#		self.mysql_cursor01.execute(query, (self.table_id, ))
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#
#		if self.mysql_cursor01.rowcount == 0:
#			return pd.DataFrame()
#
#		result_df = pd.DataFrame(self.mysql_cursor01.fetchall())
#
#		# Set the correct column namnes in the DataFrame
#		result_df_columns = []
#		for columns in self.mysql_cursor01.description:
#			result_df_columns.append(columns[0])    # Name of the column is in the first position
#		result_df.columns = result_df_columns

#		result_df = (result_df.groupby(by=['fk_index', 'source_hive_db', 'source_hive_table', 'ref_hive_db', 'ref_hive_table'] )
#			.aggregate({'source_column_name': lambda a: ",".join(a),
#						'ref_column_name': lambda a: ",".join(a)})
#			.reset_index())
#
#		result_df.rename(columns={'fk_index':'fk_name'}, inplace=True)
#
#		result_df['fk_name'] = (result_df['source_hive_db'] + "__" + result_df['source_hive_table'] + "__fk__" + result_df['ref_hive_table'] + "__" + result_df['source_column_name'].str.replace(',', '_'))
#
#		logging.debug("Executing import_config.getForeignKeysFromConfig() - Finished")
#		return result_df.sort_values(by=['fk_name'], ascending=True)
#
#	def getAllActiveIncrImports(self):
#		""" Return all rows from import_stage that uses an incremental import_type """
#		logging.debug("Executing import_config.getAllActiveIncrImports()")
#		result_df = None
#
#		query  = "select "
#		query += "	stage.hive_db, "	
#		query += "	stage.hive_table, "	
#		query += "	stage.stage, "	
#		query += "	tabl.import_type "	
#		query += "from import_stage stage "	
##		query += "join import_tables tabl "	
#		query += "	on stage.hive_db = tabl.hive_db and stage.hive_table = tabl.hive_table "
#		query += "where tabl.import_type like 'incr%' "
#
#		self.mysql_cursor01.execute(query)
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
#
#		if self.mysql_cursor01.rowcount == 0:
#			return pd.DataFrame()
#
#		result_df = pd.DataFrame(self.mysql_cursor01.fetchall())
#
#		# Set the correct column namnes in the DataFrame
#		result_df_columns = []
#		for columns in self.mysql_cursor01.description:
#			result_df_columns.append(columns[0])    # Name of the column is in the first position
#		result_df.columns = result_df_columns
#
#		logging.debug("Executing import_config.getAllActiveIncrImports() - Finished")
#		return result_df
#
#	def generateSqoopSplitBy(self):
#		""" This function will try to generate a split-by """
#		logging.debug("Executing import_config.generateSqoopSplitBy()")
#
#		self.sqoopSplitByColumn = ""	
#
#		if self.generatedPKcolumns != None and self.generatedPKcolumns != "":
#			self.sqoopSplitByColumn = self.generatedPKcolumns.split(",")[0]
#
#			if "split-by" not in self.sqoop_options.lower():
#				if self.sqoop_options != "": self.sqoop_options += " "
##				self.sqoop_options += "--split-by \"%s\""%(self.sqoopSplitByColumn)
#				self.sqoop_options += "--split-by %s"%(self.sqoopSplitByColumn)
#
#		logging.debug("Executing import_config.generateSqoopSplitBy() - Finished")
#
#	def generateSqoopBoundaryQuery(self):
#		logging.debug("Executing import_config.generateSqoopBoundaryQuery()")
#		self.generateSqoopSplitBy()
#
##		print("self.sqoopSplitByColumn: %s"%(self.sqoopSplitByColumn))
##		print("self.sqoop_options: %s"%(self.sqoop_options))
#
##		if self.sqoopSplitByColumn != "" and "split-by" in self.sqoop_options.lower():
#		if "split-by" in self.sqoop_options.lower():
##			for id, value in enumerate(self.sqoop_options.lower().split(" ")):
#			for id, value in enumerate(self.sqoop_options.split(" ")):
#				if value == "--split-by":
#					self.sqoopSplitByColumn = self.sqoop_options.split(" ")[id + 1]
#		
##		print("self.sqoopSplitByColumn: %s"%(self.sqoopSplitByColumn))
#
#		if self.sqoopSplitByColumn != "":
#			self.sqoopBoundaryQuery = "select min(%s), max(%s) from %s"%(self.sqoopSplitByColumn, self.sqoopSplitByColumn, self.common_config.getJDBCsqlFromTable(schema=self.source_schema, table=self.source_table))
#
#		logging.debug("SplitByColumn = %s"%(self.sqoopSplitByColumn))	
#		logging.debug("sqoopBoundaryQuery = %s"%(self.sqoopBoundaryQuery))	
#		logging.debug("Executing import_config.generateSqoopBoundaryQuery() - Finished")
#
##	def getQuoteAroundColumn(self):
##		quoteAroundColumn = ""
##		if self.common_config.jdbc_servertype == constant.MSSQL:		quoteAroundColumn = "\""
##		if self.common_config.jdbc_servertype == constant.ORACLE:		quoteAroundColumn = "\""
##		if self.common_config.jdbc_servertype == constant.MYSQL:		quoteAroundColumn = "`"
##		if self.common_config.jdbc_servertype == constant.POSTGRESQL:	quoteAroundColumn = "\""
##		if self.common_config.jdbc_servertype == constant.PROGRESS:		quoteAroundColumn = "\""
##		if self.common_config.jdbc_servertype == constant.DB2_UDB:		quoteAroundColumn = "\""
##		if self.common_config.jdbc_servertype == constant.DB2_AS400:	quoteAroundColumn = "\""
#
#		return quoteAroundColumn

#	def getJDBCsqlFromTable(self):
#		logging.debug("Executing import_config.getJDBCsqlFromTable()")
#
#		fromTable = "" 
#		if self.common_config.jdbc_servertype == constant.MSSQL:		fromTable = "[%s].[%s].[%s]"%(self.common_config.jdbc_database, self.source_schema, self.source_table)
#		if self.common_config.jdbc_servertype == constant.ORACLE:		fromTable = "\"%s\".\"%s\""%(self.source_schema.upper(), self.source_table.upper())
#		if self.common_config.jdbc_servertype == constant.MYSQL:		fromTable = "%s"%(self.source_table)
#		if self.common_config.jdbc_servertype == constant.POSTGRESQL:	fromTable = "\"%s\".\"%s\""%(self.source_schema, self.source_table)
#		if self.common_config.jdbc_servertype == constant.PROGRESS:		fromTable = "\"%s\".\"%s\""%(self.source_schema, self.source_table)
#		if self.common_config.jdbc_servertype == constant.DB2_UDB:		fromTable = "\"%s\".\"%s\""%(self.source_schema, self.source_table)
#		if self.common_config.jdbc_servertype == constant.DB2_AS400:	fromTable = "\"%s\".\"%s\""%(self.source_schema, self.source_table)
#
#		logging.debug("Executing import_config.getJDBCsqlFromTable() - Finished")
#		return fromTable

