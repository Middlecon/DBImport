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
import json
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from datetime import time, date, datetime, timedelta
from DBImportConfig import rest
import time
import pandas as pd

class stage(object):
	def __init__(self, mysql_conn, Hive_DB, Hive_Table):
		logging.debug("Executing stage.__init__()")

		self.Hive_DB = Hive_DB
		self.Hive_Table = Hive_Table
		self.mysql_conn = mysql_conn
		self.mysql_cursor = self.mysql_conn.cursor(buffered=False)
		self.currentStage = None
		self.memoryStage = False
		self.stageTimeStart = None
		self.stageTimeStop = None
		self.stageDurationStart = float()
		self.stageDurationStop = float()
		self.stageDurationTime = float()

		if configuration.get("REST_statistics", "post_import_data").lower() == "true":
			self.post_import_data = True
		else:
			self.post_import_data = False

		self.rest = rest.restInterface()

	def setHiveTable(self, Hive_DB, Hive_Table):
		""" Sets the parameters to work against a new Hive database and table """
		self.Hive_DB = Hive_DB.lower()
		self.Hive_Table = Hive_Table.lower()

	def getStageDescription(self, stage):
		stageDescription = ""

		# RULES FOR STAGE NUMBERS
		# 1. Take a series of even 50 numbers to a specific method
		# 2. Never use a number between 0 - 999
		# 3. 0, 1000 and 9999 is reserved
		# 4. IMPORT PHASE numbers must end with ??49 and the descripton "Import Phase Completed"
		# 5. 1001 -> 1999 is reserved for IMPORT PHASE
		# 6. 2000 -> 2999 is reserved for COPY PHASE
		# 7. 3000 -> 3999 is reserved for ETL PHASE

		# Import_Phase_FULL 
		if   stage == 1010: stageDescription = "Getting source tableschema"
		elif stage == 1011: stageDescription = "Clear table rowcount"
		elif stage == 1012: stageDescription = "Get source table rowcount"
		elif stage == 1013: stageDescription = "Sqoop import"
		elif stage == 1014: stageDescription = "Validate sqoop import"
		elif stage == 1049: stageDescription = "Import Phase Completed"

		# Import_Phase_INCR 
		elif stage == 1110: stageDescription = "Getting source tableschema"
		elif stage == 1111: stageDescription = "Clear table rowcount"
		elif stage == 1112: stageDescription = "Sqoop import"
		elif stage == 1113: stageDescription = "Get source table rowcount"
		elif stage == 1114: stageDescription = "Validate sqoop import"
		elif stage == 1149: stageDescription = "Import Phase Completed"

		# Import_Phase_ORACLE_FLASHBACK 
		elif stage == 1210: stageDescription = "Getting source tableschema"
		elif stage == 1211: stageDescription = "Clear table rowcount"
		elif stage == 1212: stageDescription = "Sqoop import"
		elif stage == 1213: stageDescription = "Get source table rowcount"
		elif stage == 1214: stageDescription = "Validate sqoop import"
		elif stage == 1249: stageDescription = "Import Phase Completed"

		# Import_Phase_FULL & ETL_Phase_TRUNCATEINSERT
		elif stage == 3050: stageDescription = "Connecting to Hive"
		elif stage == 3051: stageDescription = "Creating the import table in the staging database"
		elif stage == 3052: stageDescription = "Get Import table rowcount"
		elif stage == 3053: stageDescription = "Validate import table"
		elif stage == 3054: stageDescription = "Removing Hive locks by force"
		elif stage == 3055: stageDescription = "Creating the target table"
		elif stage == 3056: stageDescription = "Truncate target table"
		elif stage == 3057: stageDescription = "Copy rows from import to target table"
		elif stage == 3058: stageDescription = "Update Hive statistics on target table"
		elif stage == 3059: stageDescription = "Get Target table rowcount"
		elif stage == 3060: stageDescription = "Validate target table"

		# Import_Phase_INCR & ETL_Phase_INSERT
		elif stage == 3150: stageDescription = "Connecting to Hive"
		elif stage == 3151: stageDescription = "Creating the import table in the staging database"
		elif stage == 3152: stageDescription = "Get Import table rowcount"
		elif stage == 3153: stageDescription = "Validate import table"
		elif stage == 3154: stageDescription = "Removing Hive locks by force"
		elif stage == 3155: stageDescription = "Creating the target table"
		elif stage == 3156: stageDescription = "Copy rows from import to target table"
		elif stage == 3157: stageDescription = "Update Hive statistics on target table"
		elif stage == 3158: stageDescription = "Get Target table rowcount"
		elif stage == 3159: stageDescription = "Validate target table"
		elif stage == 3160: stageDescription = "Saving pending incremental values"

		# Import_Phase_FULL & ETL_Phase_MERGEHISTORYAUDIT
		elif stage == 3200: stageDescription = "Connecting to Hive"
		elif stage == 3201: stageDescription = "Creating the import table in the staging database"
		elif stage == 3202: stageDescription = "Get Import table rowcount"
		elif stage == 3203: stageDescription = "Validate import table"
		elif stage == 3204: stageDescription = "Removing Hive locks by force"
		elif stage == 3205: stageDescription = "Creating the target table"
		elif stage == 3206: stageDescription = "Creating the history table"
		elif stage == 3207: stageDescription = "Creating the delete table"
		elif stage == 3208: stageDescription = "Merge Import table with Target table"
		elif stage == 3209: stageDescription = "Update Hive statistics on target table"
		elif stage == 3210: stageDescription = "Get Target table rowcount"
		elif stage == 3211: stageDescription = "Validate target table"

		# Import_Phase_FULL & ETL_Phase_MERGEONLY
		elif stage == 3250: stageDescription = "Connecting to Hive"
		elif stage == 3251: stageDescription = "Creating the import table in the staging database"
		elif stage == 3252: stageDescription = "Get Import table rowcount"
		elif stage == 3253: stageDescription = "Validate import table"
		elif stage == 3254: stageDescription = "Removing Hive locks by force"
		elif stage == 3255: stageDescription = "Creating the target table"
		elif stage == 3256: stageDescription = "Creating the delete table"
		elif stage == 3257: stageDescription = "Merge Import table with Target table"
		elif stage == 3258: stageDescription = "Update Hive statistics on target table"
		elif stage == 3259: stageDescription = "Get Target table rowcount"
		elif stage == 3260: stageDescription = "Validate target table"

		# Import_Phase_INCR & ETL_Phase_MERGEONLY
		elif stage == 3300: stageDescription = "Connecting to Hive"
		elif stage == 3301: stageDescription = "Creating the import table in the staging database"
		elif stage == 3302: stageDescription = "Get Import table rowcount"
		elif stage == 3303: stageDescription = "Validate import table"
		elif stage == 3304: stageDescription = "Removing Hive locks by force"
		elif stage == 3305: stageDescription = "Creating the target table"
		elif stage == 3306: stageDescription = "Merge Import table with Target table"
		elif stage == 3307: stageDescription = "Update Hive statistics on target table"
		elif stage == 3308: stageDescription = "Get Target table rowcount"
		elif stage == 3309: stageDescription = "Validate target table"
		elif stage == 3310: stageDescription = "Saving pending incremental values"

		# Import_Phase_INCR & ETL_Phase_MERGEHISTORYAUDIT
		elif stage == 3350: stageDescription = "Connecting to Hive"
		elif stage == 3351: stageDescription = "Creating the import table in the staging database"
		elif stage == 3352: stageDescription = "Get Import table rowcount"
		elif stage == 3353: stageDescription = "Validate import table"
		elif stage == 3354: stageDescription = "Removing Hive locks by force"
		elif stage == 3355: stageDescription = "Creating the target table"
		elif stage == 3356: stageDescription = "Creating the history table"
		elif stage == 3357: stageDescription = "Merge Import table with Target table"
		elif stage == 3358: stageDescription = "Update Hive statistics on target table"
		elif stage == 3359: stageDescription = "Get Target table rowcount"
		elif stage == 3360: stageDescription = "Validate target table"
		elif stage == 3361: stageDescription = "Saving pending incremental values"

		elif stage == 9999: stageDescription = "DBImport completed successfully"

		return stageDescription

	def getStageShortName(self, stage):

		stageShortName = ""
		if   stage == 0:    stageShortName = "skip"

		# RULES FOR STAGE NUMBERS
		# 1. Take a series of even 50 numbers to a specific method
		# 2. Never use a number between 0 - 999
		# 3. 0, 1000 and 9999 is reserved
		# 4. IMPORT PHASE numbers must end with ??49 and the descripton "Import Phase Completed"
		# 5. 1001 -> 1999 is reserved for IMPORT PHASE
		# 6. 2000 -> 2999 is reserved for COPY PHASE
		# 7. 3000 -> 3999 is reserved for ETL PHASE

		# Import_Phase_FULL 
		elif stage == 1010: stageShortName = "get_source_tableschema"
		elif stage == 1011: stageShortName = "clear_table_rowcount"
		elif stage == 1012: stageShortName = "get_source_rowcount"
		elif stage == 1013: stageShortName = "sqoop"
		elif stage == 1014: stageShortName = "validate_sqoop_import"
		elif stage == 1049: stageShortName = "skip"

		# Import_Phase_INCR 
		elif stage == 1110: stageShortName = "get_source_tableschema"
		elif stage == 1111: stageShortName = "clear_table_rowcount"
		elif stage == 1112: stageShortName = "sqoop"
		elif stage == 1113: stageShortName = "get_source_rowcount"
		elif stage == 1114: stageShortName = "validate_sqoop_import"
		elif stage == 1149: stageShortName = "skip"

		# Import_Phase_ORACLE_FLASHBACK
		elif stage == 1210: stageShortName = "get_source_tableschema"
		elif stage == 1211: stageShortName = "clear_table_rowcount"
		elif stage == 1212: stageShortName = "sqoop"
		elif stage == 1213: stageShortName = "get_source_rowcount"
		elif stage == 1214: stageShortName = "validate_sqoop_import"
		elif stage == 1249: stageShortName = "skip"

		# Import_Phase_FULL & ETL_Phase_TRUNCATEINSERT
		elif stage == 3050: stageShortName = "connect_to_hive"
		elif stage == 3051: stageShortName = "create_import_table"
		elif stage == 3052: stageShortName = "get_import_rowcount"
		elif stage == 3053: stageShortName = "validate_import_table"
		elif stage == 3054: stageShortName = "clear_hive_locks"
		elif stage == 3055: stageShortName = "create_target_table"
		elif stage == 3056: stageShortName = "truncate_target_table"
		elif stage == 3057: stageShortName = "hive_import"
		elif stage == 3058: stageShortName = "update_statistics"
		elif stage == 3059: stageShortName = "get_target_rowcount"
		elif stage == 3060: stageShortName = "validate_target_table"

		# Import_Phase_INCR & ETL_Phase_INSERT
		elif stage == 3150: stageShortName = "connect_to_hive"
		elif stage == 3151: stageShortName = "create_import_table"
		elif stage == 3152: stageShortName = "get_import_rowcount"
		elif stage == 3153: stageShortName = "validate_import_table"
		elif stage == 3154: stageShortName = "clear_hive_locks"
		elif stage == 3155: stageShortName = "create_target_table"
		elif stage == 3156: stageShortName = "hive_import"
		elif stage == 3157: stageShortName = "update_statistics"
		elif stage == 3158: stageShortName = "get_target_rowcount"
		elif stage == 3159: stageShortName = "validate_target_table"
		elif stage == 3160: stageShortName = "skip"

		# Import_Phase_FULL & ETL_Phase_MERGEHISTORYAUDIT
		elif stage == 3200: stageShortName = "connect_to_hive"
		elif stage == 3201: stageShortName = "create_import_table"
		elif stage == 3202: stageShortName = "get_import_rowcount"
		elif stage == 3203: stageShortName = "validate_import_table"
		elif stage == 3204: stageShortName = "clear_hive_locks"
		elif stage == 3205: stageShortName = "create_target_table"
		elif stage == 3206: stageShortName = "create_history_table"
		elif stage == 3207: stageShortName = "create_delete_table"
		elif stage == 3208: stageShortName = "merge_table"
		elif stage == 3209: stageShortName = "update_statistics"
		elif stage == 3210: stageShortName = "get_target_rowcount"
		elif stage == 3211: stageShortName = "validate_target_table"

		# Import_Phase_FULL & ETL_Phase_MERGEONLY
		elif stage == 3250: stageShortName = "connect_to_hive"
		elif stage == 3251: stageShortName = "create_import_table"
		elif stage == 3252: stageShortName = "get_import_rowcount"
		elif stage == 3253: stageShortName = "validate_import_table"
		elif stage == 3254: stageShortName = "clear_hive_locks"
		elif stage == 3255: stageShortName = "create_target_table"
		elif stage == 3256: stageShortName = "create_delete_table"
		elif stage == 3257: stageShortName = "merge_table"
		elif stage == 3258: stageShortName = "update_statistics"
		elif stage == 3259: stageShortName = "get_target_rowcount"
		elif stage == 3260: stageShortName = "validate_target_table"

		# Import_Phase_INCR & ETL_Phase_MERGEONLY
		elif stage == 3300: stageShortName = "connect_to_hive"
		elif stage == 3301: stageShortName = "create_import_table"
		elif stage == 3302: stageShortName = "get_import_rowcount"
		elif stage == 3303: stageShortName = "validate_import_table"
		elif stage == 3304: stageShortName = "clear_hive_locks"
		elif stage == 3305: stageShortName = "create_target_table"
		elif stage == 3306: stageShortName = "merge_table"
		elif stage == 3307: stageShortName = "update_statistics"
		elif stage == 3308: stageShortName = "get_target_rowcount"
		elif stage == 3309: stageShortName = "validate_target_table"
		elif stage == 3310: stageShortName = "skip"

		# Import_Phase_INCR & ETL_Phase_MERGEHISTORYAUDIT
		elif stage == 3350: stageShortName = "connect_to_hive"
		elif stage == 3351: stageShortName = "create_import_table"
		elif stage == 3352: stageShortName = "get_import_rowcount"
		elif stage == 3353: stageShortName = "validate_import_table"
		elif stage == 3354: stageShortName = "clear_hive_locks"
		elif stage == 3355: stageShortName = "create_target_table"
		elif stage == 3356: stageShortName = "create_history_table"
		elif stage == 3357: stageShortName = "merge_table"
		elif stage == 3358: stageShortName = "update_statistics"
		elif stage == 3359: stageShortName = "get_target_rowcount"
		elif stage == 3360: stageShortName = "validate_target_table"
		elif stage == 3361: stageShortName = "skip"

		elif stage == 9999: stageShortName = "skip"

		return stageShortName

	def convertStageStatisticsToJSON(self, **kwargs):
		""" Reads the import_stage_statistics and convert the information to a JSON document """
		logging.debug("Executing stage.convertStageStatisticsToJSON()")

		if self.post_import_data == False:
			return

		import_stop = None
		jsonData = {}
		jsonData["type"] = "import"

		for key, value in kwargs.items():
			jsonData[key] = value

		query = "select stage, start, stop, duration from import_stage_statistics where hive_db = %s and hive_table = %s"
		self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		for row in self.mysql_cursor.fetchall():
			stage = row[0]
			stage_start = row[1]
			stage_stop = row[2]
			stage_duration = row[3]

			stageShortName = self.getStageShortName(stage)

			if stageShortName == "":
				logging.error("There is no stage description for the JSON data for stage %s"%(stage))
				logging.error("Will put the raw stage number into the JSON document")
				stageShortName = str(stage)

			if stageShortName != "skip":
				jsonData["%s_start"%(stageShortName)] = str(stage_start) 
				jsonData["%s_stop"%(stageShortName)] = str(stage_stop) 
				jsonData["%s_duration"%(stageShortName)] = stage_duration 

			if stage == 0:
				import_start = stage_start
			
			if import_stop == None or stage_stop > import_stop:
				import_stop = stage_stop

		import_duration = int((import_stop - import_start).total_seconds())

		jsonData["start"] = str(import_start)
		jsonData["stop"] = str(import_stop)
		jsonData["duration"] = import_duration

		logging.debug("Sending the following JSON to the REST interface: %s"% (json.dumps(jsonData, sort_keys=True, indent=4)))
		response = self.rest.sendData(json.dumps(jsonData))
		if response != 200:
			# There was something wrong with the REST call. So we save it to the database and handle it later
			logging.debug("REST call failed!")
			logging.debug("Saving the JSON to the json_to_rest table instead")

			query = "insert into json_to_rest (type, status, jsondata) values ('import_statistics', 0, %s)"
			self.mysql_cursor.execute(query, (json.dumps(jsonData), ))
			self.mysql_conn.commit()
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		logging.debug("Executing stage.convertStageStatisticsToJSON() - Finished")

	def saveStageStatistics(self, **kwargs):
		""" Reads the import_stage_statistics and insert it to the import_statistics table """
		logging.debug("Executing stage.saveStageStatistics()")

		columnsPart = [] 
		valuesPart = []
		import_start = None
		import_stop = None
		import_duration = None

		for key, value in kwargs.items():
			columnsPart.append(key)
			if value == True: value = 1
			if value == False: value = 0
			valuesPart.append(value)

		query = "select stage, start, stop, duration from import_stage_statistics where hive_db = %s and hive_table = %s"
		self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		for row in self.mysql_cursor.fetchall():
			stage = row[0]
			stage_start = row[1]
			stage_stop = row[2]
			stage_duration = row[3]

			stageShortName = self.getStageShortName(stage)

			if stageShortName != "" and stageShortName != "skip":
				columnsPart.append("%s_start"%(stageShortName))
				valuesPart.append(str(stage_start))

				columnsPart.append("%s_stop"%(stageShortName))
				valuesPart.append(str(stage_stop))

				columnsPart.append("%s_duration"%(stageShortName))
				valuesPart.append(str(stage_duration))

			if stage == 0:
				import_start = stage_start
			
			if import_stop == None or stage_stop > import_stop:
				import_stop = stage_stop

		import_duration = int((import_stop - import_start).total_seconds())

		columnsPart.append("start")
		valuesPart.append(str(import_start))

		columnsPart.append("stop")
		valuesPart.append(str(import_stop))

		columnsPart.append("duration")
		valuesPart.append(str(import_duration))

		query = "insert into import_statistics "
		insertQuery = "("	
		insertQuery += ", ".join(map(str, columnsPart))
		insertQuery += ") values ("	

		strInsert = ""
		for i in valuesPart:
			if strInsert != "": strInsert += ", "
			strInsert += "%s"

		insertQuery += strInsert
		insertQuery += ")"	
		query += insertQuery
	
#		print(columnsPart)
#		print("-------------------------------------------------")
#		print(valuesPart)
#		print("-------------------------------------------------")
#		print(insertQuery)

		self.mysql_cursor.execute(query, valuesPart)
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		query = "delete from import_statistics_last where hive_db = %s and hive_table = %s"
		self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		query = "insert into import_statistics_last "
		query += insertQuery
		self.mysql_cursor.execute(query, valuesPart)
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		logging.debug("Executing stage.saveStageStatistics() - Finished")

	def setStageUnrecoverable(self):
		""" Removes all stage information from the import_stage table """
		logging.debug("Executing stage.setStageUnrecoverable()")

		if self.memoryStage == False:
			query = "update import_stage set unrecoverable_error = 1 where hive_db = %s and hive_table = %s"
			self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
			self.mysql_conn.commit()
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		logging.debug("Executing stage.setStageUnrecoverable() - Finished")

	def setStageOnlyInMemory(self):
		self.memoryStage = True

	def clearStage(self):
		""" Removes all stage information from the import_stage table """
		logging.debug("Executing stage.clearStage()")

		if self.memoryStage == False:
			query = "delete from import_stage where hive_db = %s and hive_table = %s"
			self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
			self.mysql_conn.commit()
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

			query = "delete from import_stage_statistics where hive_db = %s and hive_table = %s"
			self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
			self.mysql_conn.commit()
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		logging.debug("Executing stage.clearStage() - Finished")


	def getStage(self):
		""" Fetches the stage from import_stage table and return stage value. If no stage, 0 is returned """
		logging.debug("Executing stage.getStage()")

		if self.currentStage == None:
			if self.memoryStage == True:
				self.currentStage = 0
			else:
				query = "select stage from import_stage where hive_db = %s and hive_table = %s"
				self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )
	
				row = self.mysql_cursor.fetchone()
				if row == None:
					self.currentStage = 0
				else:
					self.currentStage = row[0]

		self.stageTimeStart = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
		self.stageTimeStop = None
		self.stageDurationStart = time.monotonic()
		self.stageDurationStop = float()
		self.stageDurationTime = float()

		logging.debug("Executing stage.getStage() - Finished")
		return self.currentStage

	def setStage(self, newStage, force=False):
		""" Saves the stage information that is used for finding the correct step in the retries operations """
		logging.debug("Executing stage.setStage()")

		if force == True:
			self.currentStage = 0

		if self.currentStage != None and self.currentStage > newStage:
			logging.debug("Executing stage.setStage() - Finished (no stage set)")
			return

		if self.memoryStage == True:
			self.currentStage = newStage
			logging.debug("Executing stage.setStage() - Finished (stage only in memory)")
			return

		# Calculate time and save to import_stage_statistics
		self.stage_duration_stop = time.monotonic()
		self.stageTimeStop = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
		self.stageDurationStop = time.monotonic()
		self.stageDurationTime = self.stageDurationStop - self.stageDurationStart

		logging.debug("stageTimeStart: %s"%(self.stageTimeStart))
		logging.debug("stageTimeStop:  %s"%(self.stageTimeStop))
		logging.debug("stageDurationStart: %s"%(self.stageDurationStart))
		logging.debug("stageDurationStop:  %s"%(self.stageDurationStop))
		logging.debug("stageDurationTime:  %s"%(self.stageDurationTime))

		query = "select count(stage) from import_stage_statistics where hive_db = %s and hive_table = %s and stage = %s"
		self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table, self.currentStage))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		row = self.mysql_cursor.fetchone()
		rowCount = row[0]
		if rowCount == 0:
			query  = "insert into import_stage_statistics "
			query += "( hive_db, hive_table, stage, start, stop, duration )"
			query += "values "
			query += "( "
			query += "	'%s', "%(self.Hive_DB)
			query += "	'%s', "%(self.Hive_Table)
			query += "	%s, "%(self.currentStage)
			query += "	'%s', "%(self.stageTimeStart)
			query += "	'%s', "%(self.stageTimeStop)
			query += "	%s "%(round(self.stageDurationTime))
			query += ") "
		else:
			query  = "update import_stage_statistics set"
			query += "	start = '%s', "%(self.stageTimeStart)
			query += "	stop = '%s', "%(self.stageTimeStop)
			query += "	duration = %s "%(round(self.stageDurationTime))
			query += "where "
			query += "	hive_db = '%s' "%(self.Hive_DB)
			query += "	and hive_table = '%s' "%(self.Hive_Table)
			query += "	and stage = '%s' "%(self.currentStage)
	
		self.mysql_cursor.execute(query)
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )


		stageDescription = self.getStageDescription(newStage)

		# Save stage information in import_stage
		query = "select count(stage) from import_stage where hive_db = %s and hive_table = %s"
		self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		row = self.mysql_cursor.fetchone()
		rowCount = row[0]
		if rowCount == 0:
			query  = "insert into import_stage "
			query += "( "
			query += "	hive_db, "
			query += "	hive_table, "
			query += "	stage, "
			query += "	stage_description, "
			query += "	stage_time, "
			query += "	unrecoverable_error "
			query += ") "
			query += "values "
			query += "( "
			query += "	'%s', "%(self.Hive_DB)
			query += "	'%s', "%(self.Hive_Table)
			query += "	%s, "%(newStage)
			query += "	'%s', "%(stageDescription)
			query += "	'%s', "%(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
			query += "	0 "
			query += ") "
		else:
			query  = "update import_stage set"
			query += "	stage = %s, "%(newStage)
			query += "	stage_description = '%s', "%(stageDescription)
			query += "	stage_time = '%s' "%(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
			query += "where "
			query += "	hive_db = '%s' "%(self.Hive_DB)
			query += "	and hive_table = '%s' "%(self.Hive_Table)

		self.mysql_cursor.execute(query)
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		self.currentStage = newStage
		self.stageTimeStart = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
		self.stageTimeStop = None
		self.stageDurationStart = time.monotonic()
		self.stageDurationStop = float()
		self.stageDurationTime = float()

		logging.debug("Executing stage.setStage() - Finished")

	def saveRetryAttempt(self, stage):
		""" Saves the retry attempt in the import_retries_log table """
		logging.debug("Executing stage.saveRetryAttempt()")

		if self.memoryStage == True:
			return

		stageDescription = self.getStageDescription(stage)

		query  = "insert into import_retries_log "
		query += "( "
		query += "	hive_db, "
		query += "	hive_table, "
		query += "	retry_time, "
		query += "	stage, "
		query += "	stage_description "
		query += ") "
		query += "values (%s, %s, %s, %s, %s)"

		self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), stage, stageDescription))
		self.mysql_conn.commit()
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		logging.debug("Executing stage.saveRetryAttempt() - Finished")
