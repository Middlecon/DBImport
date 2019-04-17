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
import subprocess 
import shutil
import jaydebeapi
import base64
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, time, timedelta
import pandas as pd
from DBImportConfig import constants as constant

class initialize(object):
	def __init__(self):
		logging.debug("Executing database.__init__()")

		self.mysql_conn = None
		self.mysql_cursor = None

		# Fetch configuration about MySQL database and how to connect to it
		mysql_hostname = configuration.get("Database", "mysql_hostname")
		mysql_port =     configuration.get("Database", "mysql_port")
		mysql_database = configuration.get("Database", "mysql_database")
		mysql_username = configuration.get("Database", "mysql_username")
		mysql_password = configuration.get("Database", "mysql_password")

		# Esablish a connection to the DBImport database in MySQL
		try:
			self.mysql_conn = mysql.connector.connect(host=mysql_hostname, 
												 port=mysql_port, 
												 database=mysql_database, 
												 user=mysql_username, 
												 password=mysql_password)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				logging.error("Something is wrong with your user name or password")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				logging.error("Database does not exist")
			else:
				logging.error("%s"%err)
			logging.error("Error: There was a problem connecting to the MySQL database. Please check configuration and serverstatus and try again")
			self.remove_temporary_files()
			sys.exit(1)
		else:
			self.mysql_cursor = self.mysql_conn.cursor(buffered=False)

		logging.debug("Executing database.__init__() - Finished")

	def generateSchemaRst(self):

		query =  "select max(length(c.column_name)), max(length(c.column_comment)) "
		query += "from information_schema.columns c "
		query += "left join information_schema.tables t "
		query += "   on c.table_schema = t.table_schema and c.table_name = t.table_name "
		query += "where c.table_schema = 'DBImport' "

		logging.debug("SQL Statement executed: %s" % (query) )
		self.mysql_cursor.execute(query)

		row = self.mysql_cursor.fetchone()
		maxColumnLength = row[0]
		maxDescLength = row[1]

		lineSingle = "+"
		lineDouble = "+"
		lineHeaderColumn = "Column"
		lineHeaderDesc = "Documentation"

		for i in range(maxColumnLength + 2):
			lineSingle += "-"
			lineDouble += "="
		lineSingle += "+"
		lineDouble += "+"
		for i in range(maxDescLength + 2):
			lineSingle += "-"
			lineDouble += "="
		lineSingle += "+"
		lineDouble += "+"
		
		lineHeader = "| %s"%(lineHeaderColumn)
		for i in range(maxColumnLength + 1 - len(lineHeaderColumn)):
			lineHeader += " "
		lineHeader += "| %s"%(lineHeaderDesc)
		for i in range(maxDescLength + 1 - len(lineHeaderDesc)):
			lineHeader += " "
		lineHeader += "|"
		

		print("Database Tables")
		print("===============")
		print("")

		print("As this version of DBImport dont have the web admin tool available, the documentation will be agains each column in the configuration database tables. The admin tool will later use the same fields so whats said in here will later be applicable on the admin tool aswell.")
		print("")



		query =  "select "
		query += "   c.table_name, "
		query += "   t.table_comment, "
		query += "   c.column_name, "
		query += "   c.column_comment "
		query += "from information_schema.columns c "
		query += "left join information_schema.tables t "
		query += "   on c.table_schema = t.table_schema and c.table_name = t.table_name "
		query += "where c.table_schema = 'DBImport' "
#		query += "   and c.table_name = 'import_tables' "
		query += "order by c.table_name, c.ordinal_position "

		logging.debug("SQL Statement executed: %s" % (query) )
		self.mysql_cursor.execute(query)

		previousTable = ""
		for row in self.mysql_cursor.fetchall():
			tableName = row[0]
			tableComment = row[1]
			columnName = row[2]
			columnComment = row[3]
			if previousTable != tableName:
				previousTable =tableName 
				print("")
				print("")
				print("")
				print("Table - %s"%(tableName))
				line = ""
				for i in range(len(tableName) + 8):	
					line += "-"
				print(line)
				print("")
				if tableComment != None:
					print(tableComment)
				print("")
				print(lineSingle)	
				print(lineHeader)	
				print(lineDouble)	
				
			line = "| %s"%(columnName)
			for i in range(maxColumnLength + 1 - len(columnName)):
				line += " "
			line += "| %s"%(columnComment)
			for i in range(maxDescLength + 1 - len(columnComment)):
				line += " "
			line += "|"
			print(line)
			print(lineSingle)
		

