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
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, timedelta
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

		return stageDescription


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

		logging.debug("Executing stage.getStage() - Finished")
		return self.currentStage

	def setStage(self, newStage, force=False):
		""" Saves the stage information that is used for finding the correct step in the retries operations """
		logging.debug("Executing stage.setStage()")

		if force == True:
			self.currentStage = 1000

		if self.currentStage != None and self.currentStage > newStage:
			logging.debug("Executing stage.setStage() - Finished (no stage set)")
			return

		if self.memoryStage == True:
			self.currentStage = newStage
			logging.debug("Executing stage.setStage() - Finished (stage only in memory)")
			return

		stageDescription = self.getStageDescription(newStage)

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
