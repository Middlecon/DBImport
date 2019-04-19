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
from DBImportConfig import common_definitions
from DBImportConfig import constants as constant
from DBImportConfig import stage as stage
from mysql.connector import errorcode
from datetime import datetime
import pandas as pd

class config(object):
	def __init__(self, Hive_DB, Hive_Table):
		logging.debug("Executing import_definitions.__init__()")
		self.Hive_DB = Hive_DB.lower()	 
		self.Hive_Table = Hive_Table.lower()	 
		self.mysql_conn = None
		self.mysql_cursor01 = None
		self.startDate = None
		self.common_config = None

		self.connection_alias = None
		self.source_schema = None
		self.source_table = None
		self.table_id = None
		self.import_type = None
		self.import_type_description = None
		self.sqoop_mappers = None
		self.validate_import = None
		self.truncate_hive_table = None
		self.incr_mode = None
		self.incr_column = None
		self.sqoop_use_generated_sql = None
		self.incr_maxvalue = None
		self.validate_diff_allowed = None
		self.soft_delete_during_merge = None
		self.sqoop_last_size = None
		self.sqoop_last_rows = None
		self.sqoop_allow_text_splitter = None
		self.datalake_source = None
		self.datalake_source_table = None
		self.datalake_source_connection = None
		self.sqoop_last_execution = None
		self.sqoop_last_execution_timestamp = None
		self.hive_merge_javaheap = None
		self.sqoop_hdfs_location = None
		self.sqoop_hdfs_location_pkonly = None
		self.sqoop_incr_mode = None
		self.sqoop_incr_column = None
		self.sqoop_incr_lastvalue = None
		self.sqoop_import_type = None
		self.import_is_incremental = None
		self.create_table_with_acid = None
		self.import_with_merge = None
		self.import_with_history_table = None
		self.full_table_compare = None
		self.create_datalake_import_column = None
		self.nomerge_ingestion_sql_addition = None
		self.sqoop_sql_where_addition = None
		self.sqoop_options = None
		self.import_to_raw_database = None
		self.table_comment = None
		self.sqlGeneratedHiveColumnDefinition = None
		self.sqlGeneratedSqoopQuery = None
		self.generatedSqoopOptions = None
		self.generatedPKcolumns = None
		self.sqoop_mapcolumnjava = []
		self.sqoopStartUTS = None
#		self.sqoopSize = None
#		self.sqoopRows = None
		self.sqoopIncrMaxvaluePending = None

		self.sqlSessions = None

		self.common_config = common_definitions.config(Hive_DB, Hive_Table)

		self.startDate    = self.common_config.startDate
		self.mysql_conn = self.common_config.mysql_conn
		self.mysql_cursor01 = self.mysql_conn.cursor(buffered=True)
		self.mysql_cursor02 = self.mysql_conn.cursor(buffered=True)

		# Initialize the stage class that will handle all stage operations for us
		self.stage = stage.stage(self.mysql_conn, self.Hive_DB, self.Hive_Table)
		
		logging.debug("Executing import_definitions.__init__() - Finished")

	def logColumnAdd(self, column, columnType, description=None, hiveDB=None, hiveTable=None):
		self.common_config.logColumnAdd(column=column, columnType=columnType, description=description, hiveDB=hiveDB, hiveTable=hiveTable) 

	def logColumnTypeChange(self, column, columnType, previous_columnType=None, description=None, hiveDB=None, hiveTable=None):
		self.common_config.logColumnTypeChange(column, columnType, previous_columnType=previous_columnType, description=description, hiveDB=hiveDB, hiveTable=hiveTable)

	def logColumnRename(self, columnName, previous_columnName, description=None, hiveDB=None, hiveTable=None):
		self.common_config.logColumnRename(columnName, previous_columnName, description=description, hiveDB=hiveDB, hiveTable=hiveTable)

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

	def lookupConnectionAlias(self):
		self.common_config.lookupConnectionAlias(self.connection_alias)

	def checkTimeWindow(self):
		self.common_config.checkTimeWindow(self.connection_alias)

	def getJDBCTableDefinition(self):
		self.common_config.getJDBCTableDefinition(self.source_schema, self.source_table)

	def getImportConfig(self):
		logging.debug("Executing import_definitions.getImportConfig()")
	
		query = ("select "
				"    dbalias,"
				"    source_schema,"
				"    source_table,"
				"    table_id,"
				"    import_type,"
				"    mappers,"
				"    validate_import,"
				"    truncate_hive,"
				"    incr_mode,"
				"    incr_column,"
				"    sqoop_use_generated_sql,"
				"    incr_maxvalue,"
				"    validate_diff_allowed,"
				"    soft_delete_during_merge,"
				"    sqoop_last_size,"
				"    sqoop_last_rows,"
				"    sqoop_allow_text_splitter,"
				"    datalake_source,"
				"    sqoop_last_execution,"
				"    hive_merge_heap, "
				"    nomerge_ingestion_sql_addition, "
				"    sqoop_sql_where_addition, "
				"    sqoop_options, "
				"    generated_sqoop_options, "
				"    generated_sqoop_query "
				"from import_tables "
				"where "
				"    hive_db = %s" 
				"    and hive_table = %s ")
	
		self.mysql_cursor01.execute(query, (self.Hive_DB, self.Hive_Table))
		if self.mysql_cursor01.rowcount != 1:
			logging.error("Error: The specified Hive Database and Table can't be found in the configuration database. Please check that you specified the correct database and table and that the configuration exists in the configuration database")
			raise Exception
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		row = self.mysql_cursor01.fetchone()

		self.connection_alias = row[0]
		self.source_schema = row[1]
		self.source_table = row[2]
		self.table_id = row[3]
		self.import_type = row[4]
		self.sqoop_mappers = row[5]

		if row[6] == 0: 
			self.validate_import = False
		else:
			self.validate_import = True

		if row[7] == 1: 
			self.truncate_hive_table = True
		else:
			self.truncate_hive_table = False

		self.incr_mode = row[8]
		self.incr_column = row[9]

		if row[10] == 1: 
			self.sqoop_use_generated_sql = True
		else:
			self.sqoop_use_generated_sql = False
		
		self.incr_maxvalue = row[11]
		self.validate_diff_allowed = row[12]

		if row[13] == 1: 
			self.soft_delete_during_merge = True
		else:
			self.soft_delete_during_merge = False
		
		self.sqoop_last_size = row[14]
		self.sqoop_last_rows = row[15]

		if row[16] == 1: 
			self.sqoop_allow_text_splitter = True
		else:
			self.sqoop_allow_text_splitter = False

		self.datalake_source_table = row[17]
		self.sqoop_last_execution = row[18]
		self.hive_merge_javaheap = row[19]
		self.nomerge_ingestion_sql_addition = row[20]
		self.sqoop_sql_where_addition = row[21]
		self.sqoop_options = row[22]
		self.generatedSqoopOptions = row[23]
		self.sqlGeneratedSqoopQuery = row[24]
		if self.sqoop_options == None: self.sqoop_options = ""	# This is needed as we check if this contains a --split-by and it needs to be string for that

		# If the self.sqoop_last_execution contains an unix time stamp, we convert it so it's usuable by sqoop
		if self.sqoop_last_execution != None:
			self.sqoop_last_execution_timestamp = datetime.utcfromtimestamp(self.sqoop_last_execution).strftime('%Y-%m-%d %H:%M:%S.000')

		if self.validate_diff_allowed == -1:
			self.validate_diff_allowed = None

		# Validate that self.incr_mode have a valid configuration if it's not NULL
		if self.incr_mode != None:
			if self.incr_mode != "append" and self.incr_mode != "lastmodified":
				logging.error("Only the values 'append' or 'lastmodified' is valid for column incr_mode in import_tables.")
				raise Exception

		# Set sqoop NULL values
		if self.sqoop_last_size == None:
			self.sqoop_last_size = 0

		if self.sqoop_last_rows == None:
			self.sqoop_last_rows = 0

		# Set various sqoop variables
		self.sqoop_hdfs_location = "/data/etl/import/"+ self.Hive_DB + "/" + self.Hive_Table + "/data"
		self.sqoop_hdfs_location_pkonly = "/data/etl/import/"+ self.Hive_DB + "/" + self.Hive_Table + "/data_PKonly"
		self.sqoop_incr_mode = self.incr_mode
		self.sqoop_incr_column = self.incr_column
		self.sqoop_incr_lastvalue = self.incr_maxvalue

		# Set correct import_types. We do this because of old names of import_type
		if self.import_type == "merge_acid":
			self.import_type = "incr_merge_direct"

		if self.import_type == "full_merge":
			self.import_type = "full_merge_direct"

		if self.import_type == "full_history":
			self.import_type = "full_merge_direct_history"


		# Check to see that we have a valid import type
#		if self.import_type not in ("full", "incr", "full_direct"):
		if self.import_type not in ("full", "full_direct"):
			logging.error("Import type '%s' is not a valid type. Please check configuration"%(self.import_type))
			raise Exception

		if self.import_type == "full_direct":
			self.import_type_description = "Full import of table to Hive with Parquet files on HDFS (overrides Hive direct import)"

		if self.import_type == "full_hbase":
			self.import_type_description = "Full import of table directly to HBase"

		if self.import_type == "full_merge_direct":
			self.import_type_description = "Full import and merge of table directly to Hive"

		if self.import_type == "full_merge_direct_history":
			self.import_type_description = "Full import and merge of table directly to Hive. Will also create a History table"

		if self.import_type == "incr_merge_direct":
			self.import_type_description = "Incremental & merge import of table directly to Hive"

		if self.import_type == "full":
			self.import_type_description = "Full import of table to Hive with Parquet files on HDFS"

		if self.import_type == "incr":
			self.import_type_description = "Incremental import of table to Hive with Parquet files on HDFS"

		if self.import_type == "incr_merge_delete":
			self.import_type_description = "Incremental & merge import of table to Hive with text files on HDFS. Handle deletes in source system."

		if self.import_type == "incr_merge_delete_history":
			self.import_type_description = "Incremental & merge import of table to Hive with text files on HDFS. Handle deletes in source system and will also create a History table."

		# Determine the import_type for sqoop
#		if self.import_type == "full" or self.import_type == "incr" or self.import_type == "incr_merge_delete" or self.import_type == "incr_merge_delete_history":
#			self.sqoop_import_type = "hdfs"
#		elif self.import_type == "full_hbase":
# As we only support Parquet files and not directly to Hive anymore (due to Hive 3 and ACID), we force all imports to be on HDFS
		if self.import_type == "full_hbase":
			self.sqoop_import_type = "hbase"
		else:
			self.sqoop_import_type = "hdfs"

		# Determine if it's an incremental import based on the import_type
		if self.import_type == "incr_merge_direct" or self.import_type == "incr" or self.import_type == "incr_merge_delete" or self.import_type == "incr_merge_delete_history":
			self.import_is_incremental = True
		else:
			self.import_is_incremental = False

		# incr_merge_direct import_type does not support validation, so we turn it of if that is the import_type
		if self.import_type == "incr_merge_direct":
			self.validate_import = False

		# Determine if it's an merge and need ACID tables
		if self.import_type == "full_merge_direct" or self.import_type == "incr_merge_direct" or self.import_type == "full_merge_direct_history" or self.import_type == "incr_merge_delete" or self.import_type == "incr_merge_delete_history":
			self.create_table_with_acid = True
			self.import_with_merge = True
		else:
			self.create_table_with_acid = False
			self.import_with_merge = False

		# Determine if a history table should be created
		if self.import_type == "full_merge_direct_history" or self.import_type == "incr_merge_delete_history":
			self.import_with_history_table = True
		else:
			self.import_with_history_table = False

		# Determine if we are doing a full table compare
		if self.import_type == "full_merge_direct" or self.import_type == "full_merge_direct_history":
			self.full_table_compare = True
			if self.soft_delete_during_merge == True:
				# If we use SOFT DELETE, we cant verify the rows as they will never be correct
				self.validate_import = False
		else:
			self.full_table_compare = False

		# Fetch data from jdbc_connection table
		query = "select create_datalake_import, datalake_source from jdbc_connections where dbalias = %s "
		self.mysql_cursor01.execute(query, (self.connection_alias, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		if self.mysql_cursor01.rowcount != 1:
			logging.error("Error: Number of rows returned from query on 'jdbc_connections' was not one.")
			logging.error("Rows returned: %d" % (self.mysql_cursor01.rowcount) )
			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor01.statement) )
			raise Exception

		row = self.mysql_cursor01.fetchone()

		if row[0] == 1: 
			self.create_datalake_import_column = True
		else:
			self.create_datalake_import_column = False

		self.datalake_source_connection = row[1]

		# Set self.datalake_source based on values from either self.datalake_source_table or self.datalake_source_connection
		if self.datalake_source_table != None:
			self.datalake_source = self.datalake_source_table
		elif self.datalake_source_connection != None:
			self.datalake_source = self.datalake_source_connection

		if self.Hive_DB == "raw_import":
			self.import_to_raw_database = True
			self.Hive_Import_DB = self.Hive_DB
			self.Hive_Import_Table = self.Hive_Table
		else:
			self.import_to_raw_database = False
			self.Hive_Import_DB = "etl_import_staging"
			self.Hive_Import_Table = self.Hive_DB + "__" + self.Hive_Table + "__staging"

		# Set the name of the history tables, temporary tables and such
		self.Hive_History_DB = self.Hive_DB
		self.Hive_History_Table = self.Hive_Table + "_history"
		self.Hive_HistoryTemp_DB = "etl_import_staging"
		self.Hive_HistoryTemp_Table = self.Hive_DB + "__" + self.Hive_Table + "__temporary"
		self.Hive_Import_PKonly_DB = "etl_import_staging"
		self.Hive_Import_PKonly_Table = self.Hive_DB + "__" + self.Hive_Table + "__pkonly__staging"
		self.Hive_Import_Delete_DB = "etl_import_staging"
		self.Hive_Import_Delete_Table = self.Hive_DB + "__" + self.Hive_Table + "__pkonly__deleted"

		logging.debug("Settings from import_definitions.getImportConfig()")
		logging.debug("    connection_alias = %s"%(self.connection_alias))
		logging.debug("    source_schema = %s"%(self.source_schema))
		logging.debug("    source_table = %s"%(self.source_table))
		logging.debug("    table_id = %s"%(self.table_id))
		logging.debug("    import_type = %s"%(self.import_type))
		logging.debug("    validate_import = %s"%(self.validate_import))
		logging.debug("    truncate_hive_table = %s"%(self.truncate_hive_table))
		logging.debug("    incr_mode = %s"%(self.incr_mode))
		logging.debug("    incr_column = %s"%(self.incr_column))
		logging.debug("    incr_maxvalue = %s"%(self.incr_maxvalue))
		logging.debug("    validate_diff_allowed = %s"%(self.validate_diff_allowed))
		logging.debug("    soft_delete_during_merge = %s"%(self.soft_delete_during_merge))
		logging.debug("    datalake_source = %s"%(self.datalake_source))
		logging.debug("    datalake_source_table = %s"%(self.datalake_source_table))
		logging.debug("    datalake_source_connection = %s"%(self.datalake_source_connection))
		logging.debug("    hive_merge_javaheap = %s"%(self.hive_merge_javaheap))
		logging.debug("    sqoop_use_generated_sql = %s"%(self.sqoop_use_generated_sql))
		logging.debug("    sqoop_mappers = %s"%(self.sqoop_mappers))
		logging.debug("    sqoop_last_size = %s"%(self.sqoop_last_size))
		logging.debug("    sqoop_last_rows = %s"%(self.sqoop_last_rows))
		logging.debug("    sqoop_allow_text_splitter = %s"%(self.sqoop_allow_text_splitter))
		logging.debug("    sqoop_last_execution = %s"%(self.sqoop_last_execution))
		logging.debug("    sqoop_last_execution_timestamp = %s"%(self.sqoop_last_execution_timestamp))
		logging.debug("    sqoop_hdfs_location = %s"%(self.sqoop_hdfs_location))
		logging.debug("    sqoop_hdfs_location_pkonly = %s"%(self.sqoop_hdfs_location_pkonly))
		logging.debug("    sqoop_incr_mode = %s"%(self.sqoop_incr_mode))
		logging.debug("    sqoop_incr_column = %s"%(self.sqoop_incr_column))
		logging.debug("    sqoop_incr_lastvalue = %s"%(self.sqoop_incr_lastvalue))
		logging.debug("    sqoop_import_type = %s"%(self.sqoop_import_type))
		logging.debug("    sqoop_options = %s"%(self.sqoop_options))
		logging.debug("    import_is_incremental = %s"%(self.import_is_incremental))
		logging.debug("    create_table_with_acid = %s"%(self.create_table_with_acid)) 
		logging.debug("    import_with_merge = %s"%(self.import_with_merge)) 
		logging.debug("    import_with_history_table = %s"%(self.import_with_history_table)) 
		logging.debug("    full_table_compare = %s"%(self.full_table_compare)) 
		logging.debug("    create_datalake_import_column = %s"%(self.create_datalake_import_column)) 
		logging.debug("    nomerge_ingestion_sql_addition = %s"%(self.nomerge_ingestion_sql_addition))
		logging.debug("    sqoop_sql_where_addition = %s"%(self.sqoop_sql_where_addition))
		logging.debug("    import_to_raw_database = %s"%(self.import_to_raw_database))
		logging.debug("    Hive_History_DB = %s"%(self.Hive_History_DB))
		logging.debug("    Hive_History_Table = %s"%(self.Hive_History_Table))
		logging.debug("    Hive_HistoryTemp_DB = %s"%(self.Hive_HistoryTemp_DB))
		logging.debug("    Hive_HistoryTemp_Table = %s"%(self.Hive_HistoryTemp_Table))
		logging.debug("    Hive_Import_PKonly_DB = %s"%(self.Hive_Import_PKonly_DB))
		logging.debug("    Hive_Import_PKonly_Table = %s"%(self.Hive_Import_PKonly_Table))
		logging.debug("    Hive_Import_Delete_DB = %s"%(self.Hive_Import_Delete_DB))
		logging.debug("    Hive_Import_Delete_Table = %s"%(self.Hive_Import_Delete_Table))
		logging.debug("Executing import_definitions.getImportConfig() - Finished")

	def updateLastUpdateFromSource(self):
		# This function will update the import_tables.last_update_from_source to the startDate from the common class. This will later
		# be used to determine if all the columns exists in the source system as we set the same datatime on all columns
		# that we read from the source system
		logging.debug("")
		logging.debug("Executing import_definitions.updateLastUpdateFromSource()")

        # Update the import_tables.last_update_from_source with the current date
		query = "update import_tables set last_update_from_source = %s where hive_db = %s and hive_table = %s "
		logging.debug("")
		logging.debug("Updating the last_update_from_source in import_tables")
		self.mysql_cursor01.execute(query, (self.startDate, self.Hive_DB, self.Hive_Table))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		self.mysql_conn.commit()

		logging.debug("Executing import_definitions.updateLastUpdateFromSource() - Finished")

	def removeFKforTable(self):
		# This function will remove all ForeignKey definitions for the current table.
		logging.debug("")
		logging.debug("Executing import_definitions.removeFKforTable()")

		# Update the import_tables.last_update_from_source with the current date
		query = "delete from import_foreign_keys where table_id = %s"
		logging.debug("")
		logging.debug("Deleting FK's from import_foreign_keys")
		self.mysql_cursor01.execute(query, (self.table_id, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		self.mysql_conn.commit()

		logging.debug("Executing import_definitions.removeFKforTable() - Finished")

	def stripUnwantedChar(self, work_string):
		if work_string == None: return
		work_string = work_string.replace('`', '')
		work_string = work_string.replace('\'', '')
		work_string = work_string.replace(';', '')
		work_string = work_string.replace('\n', '')
		work_string = work_string.replace('\\', '')
		work_string = work_string.replace('’', '')
		return work_string.strip()

	def getColumnForceString(self, column_name):
		logging.debug("Executing import_definitions.getColumnForceString()")	
		# Used to determine if char/varchar fields should be forced to string in Hive

		query = ("select "
				"	jc.force_string, "
				"	t.force_string, "
				"	c.force_string "
				"from import_tables t "
				"left join jdbc_connections jc "
				"	on t.dbalias = jc.dbalias "
				"left join import_columns c "
				"	on c.hive_db = t.hive_db and c.hive_table = t.hive_table and c.column_name = %s "
				"where "
				"	t.hive_db = %s "
				"	and t.hive_table = %s ")

		self.mysql_cursor01.execute(query, (column_name, self.Hive_DB, self.Hive_Table))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		if self.mysql_cursor01.rowcount != 1:
			logging.error("Error: Number of rows returned from query on 'jdbc_connections' was not one.")
			logging.error("Rows returned: %d" % (self.mysql_cursor01.rowcount) )
			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor01.statement) )
			raise Exception

		row = self.mysql_cursor01.fetchone()
		force_string_connection = row[0]
		force_string_table = row[1]
		force_string_column = row[2]

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
		
		logging.debug("Executing import_definitions.getColumnForceString() - Finished")	
		return forceString

	def saveColumnData(self, ):
		# This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
		# and insert/update the data in the import_columns table. This also takes care of column type conversions if it's needed
		logging.debug("")
		logging.debug("Executing import_definitions.saveColumnData()")
		logging.info("Saving column data to MySQL table - import_columns")

		# IS_NULLABLE
		# SCHEMA_NAME
		# SOURCE_COLUMN_COMMENT
		# SOURCE_COLUMN_NAME
		# SOURCE_COLUMN_TYPE
		# TABLE_COMMENT
		# TABLE_NAME
		self.sqlGeneratedHiveColumnDefinition = ""
		self.sqlGeneratedSqoopQuery = ""
		self.sqoop_mapcolumnjava=[]
		columnOrder = 0

		for index, row in self.common_config.source_columns_df.iterrows():
			column_name = self.stripUnwantedChar(row['SOURCE_COLUMN_NAME']).replace(':', '').replace(',', '').replace('.', '').lower()
			column_type = row['SOURCE_COLUMN_TYPE'].lower()
			source_column_type = column_type
			source_column_name = row['SOURCE_COLUMN_NAME']
			source_column_comment = self.stripUnwantedChar(row['SOURCE_COLUMN_COMMENT'])
			self.table_comment = self.stripUnwantedChar(row['TABLE_COMMENT'])

			# Handle reserved column names in Hive
			if column_name == "date": column_name = column_name + "_HIVE"
			if column_name == "interval": column_name = column_name + "_HIVE"

			columnOrder += 1

			# Add , between column names in the list
			if len(self.sqlGeneratedHiveColumnDefinition) > 0: self.sqlGeneratedHiveColumnDefinition = self.sqlGeneratedHiveColumnDefinition + ", "
			if len(self.sqlGeneratedSqoopQuery) == 0: 
				self.sqlGeneratedSqoopQuery = "select "
			else:
				self.sqlGeneratedSqoopQuery += ", "
				
			# sqoop_column_type is just an information that will be stored in import_columns. We might be able to remove this. TODO. Verify
			sqoop_column_type = None

			if self.common_config.db_mssql == True:
				# If source type is BIT, we convert them to INTEGER
				if source_column_type == "bit": 
					self.sqoop_mapcolumnjava.append(source_column_name + "=Integer")
					sqoop_column_type = "Integer"
			
				if source_column_type == "float": 
					self.sqoop_mapcolumnjava.append(source_column_name + "=Float")
			
				column_type = re.sub('^nvarchar\(', 'varchar(', column_type)
				column_type = re.sub('^uniqueidentifier$', 'varchar(36)', column_type)
				column_type = re.sub('^datetime$', 'timestamp', column_type)
				column_type = re.sub('^datetime2$', 'timestamp', column_type)
				column_type = re.sub('^smalldatetime$', 'timestamp', column_type)
				column_type = re.sub('^bit$', 'tinyint', column_type)
				column_type = re.sub('^binary\([0-9]*\)$', 'binary', column_type)
				column_type = re.sub('^varbinary$', 'binary', column_type)
				column_type = re.sub('^varbinary\([0-9]*\)$', 'binary', column_type)
				column_type = re.sub('^geometry$', 'binary', column_type)
				column_type = re.sub('^image$', 'binary', column_type)
				column_type = re.sub('^money$', 'decimal', column_type)
				column_type = re.sub('^nchar\(', 'char(', column_type)
				column_type = re.sub('^numeric\(', 'decimal(', column_type)
				column_type = re.sub('^real$', 'float', column_type)
				column_type = re.sub('^ntext$', 'string', column_type)
				column_type = re.sub('^xml$', 'string', column_type)
				column_type = re.sub('^text$', 'string', column_type)
				column_type = re.sub('^time$', 'string', column_type)
				column_type = re.sub('^varchar\(-1\)$', 'string', column_type)
				column_type = re.sub('^varchar\(65355\)$', 'string', column_type)
				column_type = re.sub('^smallmoney$', 'float', column_type)

			if self.common_config.db_oracle == True:
				# If source type is BIT, we convert them to INTEGER
				if source_column_type.startswith("rowid("): 
					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
					sqoop_column_type = "String"
			
				if source_column_type.startswith("sdo_geometry("): 
					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
					sqoop_column_type = "String"
					column_type = "binary"
			
				if source_column_type.startswith("jtf_pf_page_object("): 
					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
					sqoop_column_type = "String"
					column_type = "binary"
			
				if source_column_type.startswith("wf_event_t("): 
					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
					sqoop_column_type = "String"
					column_type = "binary"
			
				if source_column_type.startswith("ih_bulk_type("): 
					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
					sqoop_column_type = "String"
					column_type = "binary"
			
				if source_column_type.startswith("anydata("): 
					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
					sqoop_column_type = "String"
					column_type = "binary"
			
				column_type = re.sub('^nvarchar\(', 'varchar(', column_type)
				column_type = re.sub(' char\)$', ')', column_type)
				column_type = re.sub(' byte\)$', ')', column_type)
				column_type = re.sub('^char\(0\)$', 'char(4000)', column_type)
				column_type = re.sub('^varchar2\(0\)$', 'varchar(4000)', column_type)
				column_type = re.sub('^varchar2\(', 'varchar(', column_type)
				column_type = re.sub('^nvarchar2\(', 'varchar(', column_type)
				column_type = re.sub('^rowid\(', 'varchar(', column_type)
				if re.search('^number\([0-9]\)', column_type) or column_type == "number":
					column_type = "int"
					self.sqoop_mapcolumnjava.append(source_column_name + "=Integer")
				if re.search('^number\(1[0-8]\)', column_type):
					column_type = "bigint"
					self.sqoop_mapcolumnjava.append(source_column_name + "=Integer")
#				column_type = re.sub('^number\([0-9]\)', 'int', column_type)
#				column_type = re.sub('^number\(1[0-8]\)', 'bigint', column_type)
				column_type = re.sub('^number\(', 'decimal(', column_type)
#				column_type = re.sub('^number$', 'int', column_type)
				column_type = re.sub('^date$', 'timestamp', column_type)
				column_type = re.sub('^timestamp\([0-9]*\) with time zone', 'timestamp', column_type)
				column_type = re.sub('^blob$', 'binary', column_type)
				column_type = re.sub('^clob$', 'string', column_type)
				column_type = re.sub('^nclob$', 'string', column_type)
				column_type = re.sub('^nlob$', 'string', column_type)
				column_type = re.sub('^long$', 'binary', column_type)
				column_type = re.sub('^xmltype\([0-9]*\)$', 'string', column_type)
				column_type = re.sub('^raw$', 'binary', column_type)
				column_type = re.sub('^raw\([0-9]*\)$', 'binary', column_type)
				column_type = re.sub('^timestamp\([0-9]\)', 'timestamp', column_type)
				column_type = re.sub('^long raw$', 'string', column_type)
				column_type = re.sub('^long raw\([0-9]*\)$', 'string', column_type)
				column_type = re.sub('^decimal\(3,4\)', 'decimal(8,4)', column_type)    # Very stange Oracle type number(3,4), how can pricision be smaller than scale?
				if re.search('^decimal\([0-9][0-9]\)$', column_type) != None:
					column_type = re.sub('\)$', ',0)', column_type)
					
			if self.common_config.db_mysql == True:
				column_type = re.sub('^bit$', 'tinyint', column_type)
				column_type = re.sub('^character varying\(', 'varchar(', column_type)
				column_type = re.sub('^mediumtext\([0-9]*\)', 'string', column_type)
				column_type = re.sub('^tinytext\([0-9]*\)', 'varchar(255)', column_type)
				column_type = re.sub('^text\([0-9]*\)', 'string', column_type)
				column_type = re.sub('^datetime$', 'timestamp', column_type)
				column_type = re.sub('^varbinary$', 'binary', column_type)
				column_type = re.sub('^varbinary\([0-9]*\)$', 'binary', column_type)
				if column_type == "time": 
					self.sqoop_mapcolumnjava.append(source_column_name + "=String")
					column_type = "string"

			if self.common_config.db_postgresql == True:
				column_type = re.sub('^character varying\(', 'varchar(', column_type)
				column_type = re.sub('^text', 'string', column_type)

			if self.common_config.db_progress == True:
				column_type = re.sub('^integer', 'int', column_type)
				column_type = re.sub('^numeric\(', 'decimal(', column_type)
				column_type = re.sub('^date\([0-9]\)', 'date', column_type)
				column_type = re.sub('^bit\([1]\)', 'boolean', column_type)
				column_type = re.sub(',none\)', ',0)', column_type)

			if self.common_config.db_db2udb == True:
				column_type = re.sub('^integer', 'int', column_type)
				column_type = re.sub('^timestmp', 'timestamp', column_type)
				column_type = re.sub('^blob', 'binary', column_type)
				column_type = re.sub('^clob', 'string', column_type)
				column_type = re.sub('^real', 'float', column_type)
				column_type = re.sub('^vargraph', 'varchar', column_type)
				column_type = re.sub('^graphic', 'varchar', column_type)
				column_type = re.sub('^time\([0-9]\)', 'timestamp', column_type)

			if self.common_config.db_db2as400 == True:
				column_type = re.sub('^integer', 'int', column_type)
				column_type = re.sub('^timestmp', 'timestamp', column_type)
				column_type = re.sub('^timestamp\(.*\)', 'timestamp', column_type)
				column_type = re.sub('^varbinary$', 'binary', column_type)
				column_type = re.sub('^varbinary\([0-9]*\)$', 'binary', column_type)
				column_type = re.sub('^blob', 'binary', column_type)
				column_type = re.sub('^real', 'float', column_type)
				if re.search('^numeric\(', column_type):
					column_type = re.sub('^numeric\(', 'decimal(', column_type)
					column_type = re.sub('\)$', ',0)', column_type)
				if re.search('^clob', column_type):
					column_type = "string"
					self.sqoop_mapcolumnjava.append(source_column_name + "=String")

			if self.common_config.db_mongodb == True:
				column_type = re.sub(':null', ':string', column_type)
				column_type = re.sub('^null$', 'string', column_type)

			# Hive only allow max 255 in size for char's. If we get a char that is larger than 255, we convert it to a varchar
			if column_type.startswith("char("):
				column_precision = int(column_type.split("(")[1].split(")")[0])
				if column_precision > 255:
					column_type = re.sub('^char\(', 'varchar(', column_type)

			# Remove precision from datatypes that doesnt include a precision
			column_type = re.sub('^float\([0-9]*\)', 'float', column_type)
			column_type = re.sub('^bigint\([0-9]*\)', 'bigint', column_type)
			column_type = re.sub('^int\([0-9]*\)', 'int', column_type)
				
			# As Parquet imports some column types wrong, we need to map them all to string
			if column_type == "timestamp":
				self.sqoop_mapcolumnjava.append(source_column_name + "=String")
			if column_type == "date":
				self.sqoop_mapcolumnjava.append(source_column_name + "=String")
			if re.search('^decimal\(', column_type):
				self.sqoop_mapcolumnjava.append(source_column_name + "=String")

			# Add the column to the sqlGeneratedHiveColumnDefinition variable. This will be the base for the auto generated SQL
			self.sqlGeneratedHiveColumnDefinition += "`" + column_name + "` " + column_type 
			if source_column_comment != None:
				self.sqlGeneratedHiveColumnDefinition += " COMMENT '" + source_column_comment + "'"

			# Add the column to the SQL query that can be used by sqoop
			if source_column_name.lower() != column_name.lower():
				self.sqlGeneratedSqoopQuery += "\"" + source_column_name + "\" as \"" + column_name + "\""
			else:
				self.sqlGeneratedSqoopQuery += "\"" + source_column_name + "\""

			# TODO: Add the JSON file that will be posted to Nifi

			# Fetch if we should force this column to 'string' in Hive
			columnForceString = self.getColumnForceString(column_name)
			if columnForceString == True:
				if column_type.startswith("char(") == True or column_type.startswith("varchar("):
					column_type = "string"

			# Run a query to see if the column already exists. Will be used to determine if we do an insert or update
			query = "select column_id from import_columns where table_id = %s and source_column_name = %s "
			self.mysql_cursor01.execute(query, (self.table_id, source_column_name))
			logging.debug("SQL Statement executed: \n%s" % (self.mysql_cursor01.statement) )

			if self.mysql_cursor01.rowcount == 0:
				query = ("insert into import_columns "
						"("
						"    table_id,"
						"    hive_db,"
						"    hive_table,"
						"    column_name,"
						"    column_order,"
						"    source_column_name,"
						"    column_type,"
						"    source_column_type,"
						"    source_database_type,"
						"    sqoop_column_type,"
						"    last_update_from_source,"
						"    comment"
						") values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )")
	
				self.mysql_cursor01.execute(query, (self.table_id, self.Hive_DB, self.Hive_Table, column_name.lower(), columnOrder, source_column_name, column_type, source_column_type, self.common_config.jdbc_servertype, sqoop_column_type, self.startDate, source_column_comment))
				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
			else:
				query = ("update import_columns set "
						"    hive_db = %s, "
						"    hive_table = %s, "
						"    column_order = %s, "
						"    column_type = %s, "
						"    source_column_type = %s, "
						"    source_database_type = %s, "
						"    sqoop_column_type = %s, "
						"    source_primary_key = NULL, "
						"    last_update_from_source = %s, "
						"    comment = %s "
						"where table_id = %s and source_column_name = %s ")

				self.mysql_cursor01.execute(query, (self.Hive_DB, self.Hive_Table, columnOrder, column_type, source_column_type, self.common_config.jdbc_servertype, sqoop_column_type, self.startDate, source_column_comment, self.table_id, source_column_name))
				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

				if self.common_config.post_column_data:
# CREATE TABLE `json_to_rest` (
#   `endpoint` text NOT NULL,
#   `status` tinyint(4) NOT NULL,
#   `jsondata` text NOT NULL,
#   `id` bigint(20) NOT NULL AUTO_INCREMENT,
#   PRIMARY KEY (`id`)
# ) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1
					jsonData = {}
					jsonData["event_date"] = self.startDate 
					jsonData["source_database_server_type"] = self.common_config.jdbc_servertype 
					jsonData["source_database_server"] = self.common_config.jdbc_hostname
					jsonData["source_database"] = self.common_config.jdbc_database
					jsonData["source_schema"] = self.source_schema
					jsonData["source_table"] = self.source_table
					jsonData["hive_db"] = self.Hive_DB
					jsonData["hive_table"] = self.Hive_Table
					jsonData["column"] = column_name.lower()
					jsonData["source_column"] = source_column_name
					jsonData["source_column_type"] = source_column_type
					jsonData["column_type"] = column_type
					logging.debug("Saving the following JSON to json_to_rest table: %s"% (json.dumps(jsonData, sort_keys=True, indent=4)))
	
					query = "insert into json_to_rest (endpoint, status, jsondata) values (%s, %s, %s)"
					self.mysql_cursor01.execute(query, (self.common_config.rest_column_data_endpoint, 0, json.dumps(jsonData)))
					logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
				
		# Commit all the changes to the import_column column
		self.mysql_conn.commit()

		# Add the source the to the generated sql query
		if self.source_schema == "-":
			self.sqlGeneratedSqoopQuery += " from " + self.source_table
		else:
			self.sqlGeneratedSqoopQuery += " from " + self.source_schema + "." + self.source_table
		
		# Add ( and ) to the Hive column definition so it contains a valid string for later use in the solution
		self.sqlGeneratedHiveColumnDefinition = "( " + self.sqlGeneratedHiveColumnDefinition + " )"

		logging.debug("Settings from import_definitions.saveColumnData()")
		logging.debug("    sqlGeneratedSqoopQuery = %s"%(self.sqlGeneratedSqoopQuery))
		logging.debug("    sqlGeneratedHiveColumnDefinition = %s"%(self.sqlGeneratedHiveColumnDefinition))
		logging.debug("Executing import_definitions.saveColumnData() - Finished")

	def setPrimaryKeyColumn(self, ):
		# This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
		# and update the source_primary_key column in the import_columns table with the information on what key is part of the PK
		logging.debug("")
		logging.debug("Executing import_definitions.setPrimaryKeyColumn()")
		logging.info("Setting PrimaryKey information in MySQL table - import_columns")

		# COL_DATA_TYPE
		# COL_KEY_POSITION
		# COL_NAME
		# CONSTRAINT_NAME
		# CONSTRAINT_TYPE
		# REFERENCE_COL_NAME
		# REFERENCE_SCHEMA_NAME
		# REFERENCE_TABLE_NAME
		# SCHEMA_NAME
		# TABLE_NAME

		self.generatedPKcolumns = ""

		for index, row in self.common_config.source_keys_df.iterrows():
			key_type = row['CONSTRAINT_TYPE']
			key_id = row['COL_KEY_POSITION']
			column_name = self.stripUnwantedChar(row['COL_NAME']).replace(':', '').replace(',', '').replace('.', '').lower()

			# Handle reserved column names in Hive
			if column_name == "date": column_name = column_name + "_HIVE"
			if column_name == "interval": column_name = column_name + "_HIVE"

# TODO: Loop through only PK's in the for loop when external Python code is completed. Same as we do for FK's
			# Determine what the PK is called in the Pandas dataframe for each database type
			key_type_reference = None
#			if self.common_config.db_mssql == True:      key_type_reference = "PRIMARY_KEY_CONSTRAINT"
#			if self.common_config.db_oracle == True:     key_type_reference = "P"
#			if self.common_config.db_mysql == True:      key_type_reference = "PRIMARY"
#			if self.common_config.db_postgresql == True: key_type_reference = "p"

			if key_type_reference == None: key_type_reference = constant.PRIMARY_KEY

			# As we only work with PK in this function, we ignore all other kind of keys (thats usually Foreign Keys we ignore)
			if key_type != key_type_reference: continue

			logging.debug("	key_type: %s" % (key_type))	
			logging.debug("	key_type_reference: %s" % (key_type_reference))	
			logging.debug("	key_id: %s" % (key_id))	
			logging.debug("	column_name: %s" % (column_name))	

			# Append the column_name to the generated PK list.
			if len(self.generatedPKcolumns) > 0: self.generatedPKcolumns += ", "
			self.generatedPKcolumns += column_name

			query = ("update import_columns set "
					"    source_primary_key = %s "
					"where table_id = %s and lower(source_column_name) = %s ")

			self.mysql_cursor01.execute(query, (key_id, self.table_id, column_name))
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
				
		# Commit all the changes to the import_column column
		self.mysql_conn.commit()

		logging.debug("Executing import_definitions.setPrimaryKeyColumn() - Finished")

	def saveKeyData(self, ):
		# This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
		# and update the import_foreign_keys table with the information on what FK's are available for the table
		logging.debug("")
		logging.debug("Executing import_definitions.saveKeyData()")
		logging.info("Setting ForeignKey information in MySQL table - import_foreign_keys")

		# COL_DATA_TYPE
		# COL_KEY_POSITION
		# COL_NAME
		# CONSTRAINT_NAME
		# CONSTRAINT_TYPE
		# REFERENCE_COL_NAME
		# REFERENCE_SCHEMA_NAME
		# REFERENCE_TABLE_NAME
		# SCHEMA_NAME
		# TABLE_NAME


		key_type_reference = ""
#		column_name_list = ""
#		ref_column_name_list = ""
		fk_index = 1
		fk_index_counter = 1
		fk_index_dict = {}

		# Set the correct string for a FK key in different database types
#		if self.common_config.db_mssql == True:      key_type_reference = "FK"
#		if self.common_config.db_oracle == True:     key_type_reference = "R"
#		if self.common_config.db_mysql == True:      key_type_reference = "FK"
#		if self.common_config.db_postgresql == True: key_type_reference = "f"
		if key_type_reference == "": key_type_reference = constant.FOREIGN_KEY

		if self.common_config.source_keys_df.empty == True: return

		# Create a new DF that only contains FK's
		source_fk_df = self.common_config.source_keys_df[self.common_config.source_keys_df['CONSTRAINT_TYPE'] == key_type_reference]

		# Loop through all unique ForeignKeys that is presented in the source.
#		for source_fk in source_fk_df[source_fk_df['CONSTRAINT_TYPE'] == key_type_reference].CONSTRAINT_NAME.unique():
		for source_fk in source_fk_df.CONSTRAINT_NAME.unique():
			logging.debug("Parsing FK with name '%s'"%(source_fk))
		
			# Iterate over the rows that matches the FK name
			for index, row in source_fk_df[source_fk_df['CONSTRAINT_NAME'] == source_fk].iterrows():
#				print("Key row: %s"%(row))
# #SCHEMA_NAME|TABLE_NAME|CONSTRAINT_NAME|CONSTRAINT_TYPE|COL_NAME|COL_DATA_TYPE|REFERENCE_SCHEMA_NAME|REFERENCE_TABLE_NAME|REFERENCE_COL_NAME|COL_KEY_POSITION
				source_fk_name = row['CONSTRAINT_NAME']
				column_name = row['COL_NAME']
				ref_schema_name = row['REFERENCE_SCHEMA_NAME']
				ref_table_name = row['REFERENCE_TABLE_NAME']
				ref_column_name = row['REFERENCE_COL_NAME']
				key_position = row['COL_KEY_POSITION']

				# If we already worked with this FK, we fetch the fk_index from the dictinary.
				# If not, we add it to the dictionary and increase the counter so the next FK gets a higher index number.
				if source_fk_name in fk_index_dict:
					fk_index = fk_index_dict[source_fk_name]
				else:
					fk_index_dict[source_fk_name] = fk_index_counter
					fk_index = fk_index_counter
					fk_index_counter += 1
			
				# MySQL dont have schemas. So we make them default to dash
				if self.common_config.db_mysql == True:	ref_schema_name = "-"

				# Select table_id's that matches the source based on dbalias, schema and table name
				# This can be many table_id's as we might have imported the same source multiple times
				# under different names.
				# TODO: Right now, we only support reference to one table per FK. If we have imported the same table more than once
				# the FK will add with primary key validation. To get this error, remove the "limit 1" in the sql statement and import
				# the same source table twice
				query = "select table_id from import_tables where dbalias = %s and source_schema = %s and source_table = %s limit 1"
				self.mysql_cursor01.execute(query, (self.connection_alias, ref_schema_name, ref_table_name ))
				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

				if self.mysql_cursor01.rowcount == 0:
					logging.warning("Reference table '%s.%s' on alias '%s' does not exist in MySQL. Skipping FK on this table"%(ref_schema_name, ref_table_name, self.connection_alias))
				else:
					rows = self.mysql_cursor01.fetchall()
					for ref_table_id in rows:
						# Looping through all table_id's that matches the source from the previous select
						logging.debug("Parsing referenced table with table_id: %s"%(ref_table_id[0]))

						# Fetch the column_id for the table own column
						query = "select column_id from import_columns where table_id = %s and lower(source_column_name) = %s"
						self.mysql_cursor02.execute(query, (self.table_id, column_name ))
						logging.debug("SQL Statement executed: %s" % (self.mysql_cursor02.statement) )
						row = self.mysql_cursor02.fetchone()
						source_column_id = row[0]

						# Fetch the column_id for the column in the referensed table
						query = "select column_id from import_columns where table_id = %s and lower(source_column_name) = %s"
						self.mysql_cursor02.execute(query, (ref_table_id[0], ref_column_name ))
						logging.debug("SQL Statement executed: %s" % (self.mysql_cursor02.statement) )
						row = self.mysql_cursor02.fetchone()

						if self.mysql_cursor02.rowcount == 0:
							logging.warning("Referenced column '%s' in '%s.%s' cant be found in MySQL. Skipping FK on this table"%(ref_column_name, ref_schema_name, ref_table_name))
							ref_column_id = None
						else:
		#					row = self.mysql_cursor02.fetchone()
							ref_column_id = row[0]

							# Save the FK data to the MySQL table
							query = ("insert into import_foreign_keys "
									"("
									"    table_id, "
									"    column_id, "
									"    fk_index, "
									"    fk_table_id, "
									"    fk_column_id, "
									"    key_position "
									") values ( %s, %s, %s, %s, %s, %s )")

							logging.debug("self.table_id:    %s"%(self.table_id))
							logging.debug("source_column_id: %s"%(source_column_id))
							logging.debug("fk_index:         %s"%(fk_index))
							logging.debug("ref_table_id[0]:  %s"%(ref_table_id[0]))
							logging.debug("ref_column_id:    %s"%(ref_column_id))
							logging.debug("key_position:     %s"%(key_position))
							self.mysql_cursor02.execute(query, (self.table_id, source_column_id, fk_index, ref_table_id[0], ref_column_id, key_position  ))
							self.mysql_conn.commit()
							logging.debug("SQL Statement executed: %s" % (self.mysql_cursor02.statement) )

		logging.debug("Executing import_definitions.saveKeyData() - Finished")

	def saveGeneratedData(self, ):
		# This will save data to the generated* columns in import_table
		logging.debug("")
		logging.debug("Executing import_definitions.saveGeneratedData()")
		logging.info("Saving generated data to MySQL table - import_table")

		# Create a valid self.generatedSqoopOptions value
		self.generatedSqoopOptions = None
		if len(self.sqoop_mapcolumnjava) > 0:
			self.generatedSqoopOptions = "--map-column-java "
			for column_map in self.sqoop_mapcolumnjava:
				self.generatedSqoopOptions += column_map + "," 
			# Remove the last ","
			self.generatedSqoopOptions = self.generatedSqoopOptions[:-1]
			
		if self.generatedPKcolumns == "": self.generatedPKcolumns = None

		query = ("update import_tables set "
				"    generated_hive_column_definition = %s, "
				"    generated_sqoop_query = %s, "
				"    comment = %s, "
				"    generated_sqoop_options = %s, "
				"    generated_pk_columns = %s, "
				"    generated_foreign_keys = NULL "
				"where table_id = %s ")

		self.mysql_cursor01.execute(query, (self.sqlGeneratedHiveColumnDefinition, self.sqlGeneratedSqoopQuery, self.table_comment, self.generatedSqoopOptions, self.generatedPKcolumns, self.table_id))
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
				
		logging.debug("Executing import_definitions.saveGeneratedData() - Finished")

	def calculateJobMappers(self):
		logging.debug("Executing import_definitions.calculateJobMappers()")
		logging.info("Calculating the number of SQL sessions in the source system that the import will use")

		# Fetch the configured max and default value from configuration file
		sqlSessionsMin = 1
		sqlSessionsMax = int(configuration.get("Import", "max_sql_sessions"))
		sqlSessionsDefault = int(configuration.get("Import", "default_sql_sessions"))

		# Execute SQL query that calculates the value
		query = ("select sqoop_last_size, " 
				"cast(sqoop_last_size / (1024*1024*128) as unsigned) as calculated_mappers, "
				"mappers "
				"from import_tables where "
				"table_id = %s")
		self.mysql_cursor01.execute(query, (self.table_id, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		if self.mysql_cursor01.rowcount != 1:
			logging.error("Error: Number of rows returned from query on 'import_table' was not one.")
			logging.error("Rows returned: %d" % (self.mysql_cursor01.rowcount) )
			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor01.statement) )
			raise Exception

		row = self.mysql_cursor01.fetchone()
		sqlSessionsLast = row[0]
		self.sqlSessions = row[1]
		self.sqoop_mappers = row[2]

		if self.sqoop_mappers > 0: self.sqlSessions = self.sqoop_mappers
		if self.sqlSessions == None:
			logging.info("Cant find the previous number of SQL sessions used. Will default to %s"%(sqlSessionsDefault))
			self.sqlSessions = sqlSessionsDefault
		else:
			if self.sqlSessions < sqlSessionsMin:
				self.sqlSessions = sqlSessionsMin
			elif self.sqlSessions > sqlSessionsMax:
				self.sqlSessions = sqlSessionsMax

			logging.info("The import will use %s parallell SQL sessions in the source system."%(self.sqlSessions)) 

		logging.debug("Executing import_definitions.calculateJobMappers() - Finished")

	def clearTableRowCount(self):
		logging.debug("Executing import_definitions.clearTableRowCount()")
		logging.info("Clearing rowcounts from previous imports")

		query = ("update import_tables set source_rowcount = NULL, hive_rowcount = NULL where table_id = %s")
		self.mysql_cursor01.execute(query, (self.table_id, ))
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		logging.debug("Executing import_definitions.clearTableRowCount() - Finished")

	def getJDBCTableRowCount(self):
		logging.debug("Executing import_definitions.getJDBCtableRowCount()")
		logging.info("Reading number of rows in source table. This will later be used for validating the import")

		whereStatement = None
		if self.import_is_incremental == True:
			logging.error("Incremental row counts is not supported yet!")
			raise Exception
		else:
			whereStatement = self.sqoop_sql_where_addition

		# We cant have "None" as value, so if it's null we set it to an empty string
		if whereStatement == None: whereStatement = ""

		JDBCRows = self.common_config.getJDBCTableRowCount(self.source_schema, self.source_table, whereStatement)
		logging.debug("Source table contains %s rows"%(JDBCRows))

		# Save the value to the database
		query = ("update import_tables set source_rowcount = %s where table_id = %s")
		self.mysql_cursor01.execute(query, (JDBCRows, self.table_id))
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		logging.debug("Executing import_definitions.getJDBCtableRowCount() - Finished")

	def saveHiveTableRowCount(self, rowCount):
		logging.debug("Executing import_definitions.saveHiveTableRowCount()")
		logging.info("Saving the number of rows in the Hive Table to the configuration database")

		# Save the value to the database
		query = ("update import_tables set hive_rowcount = %s where table_id = %s")
		self.mysql_cursor01.execute(query, (rowCount, self.table_id))
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		logging.debug("Executing import_definitions.saveHiveTableRowCount() - Finished")

	def saveSqoopStatistics(self, sqoopStartUTS, sqoopSize, sqoopRows, sqoopIncrMaxvaluePending):
		logging.debug("Executing import_definitions.saveSqoopStatistics()")
		logging.info("Saving sqoop statistics")

		self.sqoopStartUTS = sqoopStartUTS
#		self.sqoopSize = sqoopSize
#		self.sqoopRows = sqoopRows
		self.sqoop_last_size = sqoopSize
		self.sqoop_last_rows = sqoopRows
		self.sqoopIncrMaxvaluePending = sqoopIncrMaxvaluePending
		self.sqoop_last_execution_timestamp = datetime.utcfromtimestamp(sqoopStartUTS).strftime('%Y-%m-%d %H:%M:%S.000')

		query =  "update import_tables set "
		query += "sqoop_last_execution = %s, sqoop_last_size = %s, sqoop_last_rows = %s, incr_maxvalue_pending = %s "
		query += "where table_id = %s "

		self.mysql_cursor01.execute(query, (sqoopStartUTS, sqoopSize, sqoopRows, sqoopIncrMaxvaluePending, self.table_id))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )
		self.mysql_conn.commit()

		logging.debug("Executing import_definitions.saveSqoopStatistics() - Finished")

	def getColumnsFromConfigDatabase(self,):
		""" Reads the columns from the configuration database and returns the information in a Pandas DF with the columns name, type and comment """
		logging.debug("Executing import_definitions.getColumnsFromConfigDatabase()")
		hiveColumnDefinition = ""

		query  = "select c.column_name as name, c.column_type as type, c.comment "
		query += "from import_tables t " 
		query += "join import_columns c on t.table_id = c.table_id "
		query += "where t.table_id = %s and t.last_update_from_source = c.last_update_from_source "
		query += "order by c.column_order"
		self.mysql_cursor01.execute(query, (self.table_id, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		if self.mysql_cursor01.rowcount == 0:
			logging.error("Error: Zero rows returned from query on 'import_table'")
			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor01.statement) )
			raise Exception

		result_df = pd.DataFrame(self.mysql_cursor01.fetchall())

		# Set the correct column namnes in the DataFrame
		result_df_columns = []
		for columns in self.mysql_cursor01.description:
			result_df_columns.append(columns[0])    # Name of the column is in the first position
		result_df.columns = result_df_columns

		logging.debug("Executing import_definitions.getColumnsFromConfigDatabase() - Finished")
		return result_df

	def getHiveTableComment(self,):
		""" Returns the table comment stored in import_tables.comment """
		logging.debug("Executing import_definitions.getHiveTableComment()")
		hiveColumnDefinition = ""

		query  = "select comment from import_tables where table_id = %s "
		self.mysql_cursor01.execute(query, (self.table_id, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		if self.mysql_cursor01.rowcount == 0:
			logging.error("Error: Zero rows returned from query on 'import_table'")
			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor01.statement) )
			raise Exception

		row = self.mysql_cursor01.fetchone()
				
		logging.debug("Executing import_definitions.getHiveTableComment() - Finished")
		return row[0]

	def validateRowCount(self, validateSqoop=False):
		""" Validates the rows based on values stored in import_tables -> source_columns and target_columns. Returns True or False """
		logging.debug("Executing import_definitions.validateRowCount()")
		returnValue = None

		if self.validate_import == True:
			# Reading the saved number from the configurationdatabase
			if validateSqoop == False:
				query  = "select source_rowcount, hive_rowcount from import_tables where table_id = %s "
				validateText = "Hive table"
			else:
				query  = "select source_rowcount from import_tables where table_id = %s "
				validateText = "Sqoop import"
			self.mysql_cursor01.execute(query, (self.table_id, ))
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

			row = self.mysql_cursor01.fetchone()
			source_rowcount = row[0]
			if validateSqoop == False:
				target_rowcount = row[1]
			else:
				target_rowcount = self.sqoop_last_rows
			diffAllowed = 0
			logging.debug("source_rowcount: %s"%(source_rowcount))
			logging.debug("target_rowcount: %s"%(target_rowcount))

			if source_rowcount > 0:
				if self.validate_diff_allowed != None:
					diffAllowed = int(self.validate_diff_allowed)
				else:
					diffAllowed = int(source_rowcount*(50/(100*math.sqrt(source_rowcount))))

			upperValidationLimit = source_rowcount + diffAllowed
			lowerValidationLimit = source_rowcount - diffAllowed

			logging.debug("upperValidationLimit: %s"%(upperValidationLimit))
			logging.debug("lowerValidationLimit: %s"%(lowerValidationLimit))
			logging.debug("diffAllowed: %s"%(diffAllowed))


			if target_rowcount > upperValidationLimit or target_rowcount < lowerValidationLimit:
#			if 1 == 1:
				logging.error("Validation failed! The %s exceedes the allowed limit compared top the source table"%(validateText))
				if target_rowcount > source_rowcount:
					logging.info("Diff between source and target table: %s"%(target_rowcount - source_rowcount))
				else:
					logging.info("Diff between source and target table: %s"%(source_rowcount - target_rowcount))
				logging.info("Source table rowcount: %s"%(source_rowcount))
				logging.info("%s rowcount: %s"%(validateText, target_rowcount))
				logging.info("Validation diff allowed: %s"%(diffAllowed))
				logging.info("Upper limit: %s"%(upperValidationLimit))
				logging.info("Lower limit: %s"%(lowerValidationLimit))
				returnValue = False 
			else:
				logging.info("%s validation successful!"%(validateText))
				logging.info("Source table rowcount: %s"%(source_rowcount))
				logging.info("%s rowcount: %s"%(validateText, target_rowcount))
				logging.info("")
				returnValue = True 

		else:
			returnValue = True

		logging.debug("Executing import_definitions.validateRowCount() - Finished")
		return returnValue
		
	def getPKcolumns(self,):
		""" Returns a comma seperated list of columns that is part of the PK """
		logging.debug("Executing import_definitions.getPKcolumns()")
		returnValue = None

		# First fetch the override if it exists
		query  = "select pk_column_override from import_tables where table_id = %s "
		self.mysql_cursor01.execute(query, (self.table_id, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		if self.mysql_cursor01.rowcount == 0:
			logging.error("Error: Zero rows returned from query on 'import_table'")
			logging.error("SQL Statement that generated the error: %s" % (self.mysql_cursor01.statement) )
			raise Exception

		row = self.mysql_cursor01.fetchone()
		if row[0] != None:
			logging.debug("Executing import_definitions.getPKcolumns() - Finished")
			return row[0]

		# If we reach this part, it means that we didnt override the PK. So lets fetch the real one	
		query  = "select c.source_column_name "
		query += "from import_tables t "
		query += "join import_columns c on t.table_id = c.table_id "
		query += "where t.table_id = %s and c.source_primary_key is not null and t.last_update_from_source = c.last_update_from_source "
		query += "order by c.source_primary_key "

		self.mysql_cursor01.execute(query, (self.table_id, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		for row in self.mysql_cursor01.fetchall():
			if returnValue == None:
				returnValue = str(row[0].lower())
			else:
				returnValue += "," + str(row[0].lower()) 

		logging.debug("Executing import_definitions.getPKcolumns() - Finished")
		return returnValue

	def getForeignKeysFromConfig(self,):
		""" Reads the ForeignKeys from the configuration tables and return the result in a Pandas DF """
		logging.debug("Executing import_definitions.getForeignKeysFromConfig()")
		result_df = None

		query  = "select "
		query += "   s.hive_db as source_hive_db, "
		query += "   s.hive_table as source_hive_table, " 
		query += "   lower(s.column_name) as source_column_name, "
		query += "   lower(s.source_column_name) as source_source_column_name, "
		query += "   refT.source_schema as ref_source_schema, "
		query += "   refT.source_table as ref_source_table, "
		query += "   ref.column_name as ref_column_name, "
		query += "   lower(ref.source_column_name) as ref_source_column_name, "
		query += "   ref.hive_db as ref_hive_db, "
		query += "   ref.hive_table as ref_hive_table, "
		query += "   fk.fk_index as fk_index, "
		query += "   fk.key_position as key_position "
		query += "from import_foreign_keys fk "
		query += "   join import_columns s on s.table_id = fk.table_id and s.column_id = fk.column_id "
		query += "   join import_columns ref on ref.table_id = fk.fk_table_id and ref.column_id = fk.fk_column_id "
		query += "   join import_tables refT on fk.fk_table_id = refT.table_id "
		query += "where fk.table_id = %s "
		query += "order by fk.fk_index, fk.key_position "

		self.mysql_cursor01.execute(query, (self.table_id, ))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement) )

		if self.mysql_cursor01.rowcount == 0:
			return pd.DataFrame()

		result_df = pd.DataFrame(self.mysql_cursor01.fetchall())

		# Set the correct column namnes in the DataFrame
		result_df_columns = []
		for columns in self.mysql_cursor01.description:
			result_df_columns.append(columns[0])    # Name of the column is in the first position
		result_df.columns = result_df_columns

		result_df = (result_df.groupby(by=['fk_index', 'source_hive_db', 'source_hive_table', 'ref_hive_db', 'ref_hive_table'] )
			.aggregate({'source_column_name': lambda a: ",".join(a),
						'ref_column_name': lambda a: ",".join(a)})
			.reset_index())

		result_df.rename(columns={'fk_index':'fk_name'}, inplace=True)

		result_df['fk_name'] = (result_df['source_hive_db'] + "__" + result_df['source_hive_table'] + "__fk__" + result_df['ref_hive_table'] + "__" + result_df['source_column_name'].str.replace(',', '_'))

		logging.debug("Executing import_definitions.getForeignKeysFromConfig() - Finished")
		return result_df.sort_values(by=['fk_name'], ascending=True)
