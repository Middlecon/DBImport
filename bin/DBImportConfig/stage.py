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
from DBImportConfig import rest as rest
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

	def getStageDescription(self, stage):
		stageDescription = ""

		# Full imports
		if   stage == 1010: stageDescription = "Getting source tableschema"
		elif stage == 1011: stageDescription = "Clear table rowcount"
		elif stage == 1012: stageDescription = "Get source table rowcount"
		elif stage == 1013: stageDescription = "Sqoop import"
		elif stage == 1014: stageDescription = "Validate sqoop import"
		elif stage == 1049: stageDescription = "Stage1 Completed"
		elif stage == 1050: stageDescription = "Connecting to Hive"
		elif stage == 1051: stageDescription = "Creating the import table in the staging database"
		elif stage == 1052: stageDescription = "Get Import table rowcount"
		elif stage == 1053: stageDescription = "Validate import table"
		elif stage == 1054: stageDescription = "Removing Hive locks by force"
		elif stage == 1055: stageDescription = "Creating the target table"
		elif stage == 1056: stageDescription = "Truncate target table"
		elif stage == 1057: stageDescription = "Copy rows from import to target table"
		elif stage == 1058: stageDescription = "Update Hive statistics on target table"
		elif stage == 1059: stageDescription = "Get Target table rowcount"
		elif stage == 1060: stageDescription = "Validate target table"

		elif stage == 1110: stageDescription = "Getting source tableschema"
		elif stage == 1111: stageDescription = "Clear table rowcount"
		elif stage == 1112: stageDescription = "Sqoop import"
		elif stage == 1113: stageDescription = "Get source table rowcount"
		elif stage == 1114: stageDescription = "Validate sqoop import"
		elif stage == 1149: stageDescription = "Stage1 Completed"
		elif stage == 9999: stageDescription = "DBImport completed successfully"

		return stageDescription

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

			stageJSON = ""
			if   stage == 0:    stageJSON = "skip"
			elif stage == 1010: stageJSON = "get_source_tableschema"
			elif stage == 1011: stageJSON = "clear_table_rowcount"
			elif stage == 1012: stageJSON = "get_source_rowcount"
			elif stage == 1013: stageJSON = "sqoop"
			elif stage == 1014: stageJSON = "validate_sqoop_import"
			elif stage == 1049: stageJSON = "skip"
			elif stage == 1050: stageJSON = "connect_to_hive"
			elif stage == 1051: stageJSON = "create_import_table"
			elif stage == 1052: stageJSON = "get_import_rowcount"
			elif stage == 1053: stageJSON = "validate_import_table"
			elif stage == 1054: stageJSON = "clear_hive_locks"
			elif stage == 1055: stageJSON = "create_target_table"
			elif stage == 1056: stageJSON = "truncate_target_table"
			elif stage == 1057: stageJSON = "hive_import"
			elif stage == 1058: stageJSON = "update_statistics"
			elif stage == 1059: stageJSON = "get_target_rowcount"
			elif stage == 1060: stageJSON = "validate_target_table"

			elif stage == 1110: stageJSON = "get_source_tableschema"
			elif stage == 1111: stageJSON = "clear_table_rowcount"
			elif stage == 1112: stageJSON = "sqoop"
			elif stage == 1113: stageJSON = "get_source_rowcount"
			elif stage == 1114: stageJSON = "validate_sqoop_import"
			elif stage == 1149: stageJSON = "skip"

			if stageJSON == "":
				logging.error("There is no stage description for the JSON data for stage %s"%(stage))
				logging.error("Will put the raw stage number into the JSON document")
				stageJSON = str(stage)

			if stageJSON != "skip":
				jsonData["%s_start"%(stageJSON)] = str(stage_start) 
				jsonData["%s_stop"%(stageJSON)] = str(stage_stop) 
				jsonData["%s_duration"%(stageJSON)] = stage_duration 

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

	def setStageUnrecoverable(self):
		""" Removes all stage information from the import_stage table """
		logging.debug("Executing stage.clearStage()")

		if self.memoryStage == False:
			query = "update import_stage set unrecoverable_error = 1 where hive_db = %s and hive_table = %s"
			self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
			self.mysql_conn.commit()
			logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement) )

		logging.debug("Executing stage.clearStage() - Finished")

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
			query += "	unrecoverable_error "
			query += ") "
			query += "values "
			query += "( "
			query += "	'%s', "%(self.Hive_DB)
			query += "	'%s', "%(self.Hive_Table)
			query += "	%s, "%(newStage)
			query += "	'%s', "%(stageDescription)
			query += "	0 "
			query += ") "
		else:
			query  = "update import_stage set"
			query += "	stage = %s, "%(newStage)
			query += "	stage_description = '%s' "%(stageDescription)
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
