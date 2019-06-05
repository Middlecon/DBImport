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
from common import constants as constant
import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import create_view
# from setupOperation.schema import *
#from setupOperation import schema
from DBImportConfig import configSchema
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text

class initialize(object):
	def __init__(self):
		logging.debug("Executing database.__init__()")

		self.mysql_conn = None
		self.mysql_cursor = None

		# Fetch configuration about MySQL database and how to connect to it
		self.configHostname = configuration.get("Database", "mysql_hostname")
		self.configPort =     configuration.get("Database", "mysql_port")
		self.configDatabase = configuration.get("Database", "mysql_database")
		self.configUsername = configuration.get("Database", "mysql_username")
		self.configPassword = configuration.get("Database", "mysql_password")

		# Esablish a SQLAlchemy connection to the DBImport database 
#		try:
		# TODO: Error handling in SQLAlchemy
		connectStr = "mysql+pymysql://%s:%s@%s:%s/%s"%(
			self.configUsername, 
			self.configPassword, 
			self.configHostname, 
			self.configPort, 
			self.configDatabase)
		self.configDB = sa.create_engine(connectStr, echo = False)

		# Esablish a connection to the DBImport database in MySQL
		try:
			self.mysql_conn = mysql.connector.connect(host=self.configHostname, 
												 port=self.configPort, 
												 database=self.configDatabase, 
												 user=self.configUsername, 
												 password=self.configPassword)
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
		query += "   and c.table_name not like 'airflow%' "
		query += "   and c.table_name != 'etl_jobs' "
		query += "   and c.table_name != 'auto_discovered_tables' "
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
				print(" ")
				print(" ")
				print(" ")
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
		

	def getListOfConfigTables(self):
		logging.debug("Executing common_config.getListOfConfigTables()")

		query =  "select "
		query += "   table_name "
		query += "from information_schema.tables  "
		query += "where table_schema = %s "
		query += "order by table_name "

		logging.debug("SQL Statement executed: %s" % (query) )
		self.mysql_cursor.execute(query, (self.configDatabase, ))

		tablesDF = pd.DataFrame(self.mysql_cursor.fetchall())

		if len(tablesDF) > 0:
			tablesDF.columns = ['table']

		logging.debug("Executing common_config.getListOfConfigTables() - Finished")
		return tablesDF

	def createDB(self, createOnlyTable=None):
		
#		configSchema.dbVersion.__table__.drop(self.configDB)
#		configSchema.configuration.__table__.drop(self.configDB)

		inspector = sa.inspect(self.configDB)		
		allTables = inspector.get_table_names()
		if "db_version" in allTables:
			print("DBImport configuration database is already created. If you are deploying a new version, please run --upgradeDB")
			return

#		allViews = inspector.get_view_names()
#	
#		if "import_foreign_keys_view" not in allViews:
#			
#			query  = "create view `import_foreign_keys_view` as select "
#			query += "    `s`.`hive_db` AS `hive_db`, "
#			query += "    `s`.`hive_table` AS `hive_table`, "
#			query += "    `fk`.`fk_index` AS `fk_index`, "
#			query += "    `s`.`column_name` AS `column_name`, "
#			query += "    `ref`.`hive_db` AS `ref_hive_db`, "
#			query += "    `ref`.`hive_table` AS `ref_hive_table`, "
#			query += "    `ref`.`column_name` AS `ref_column_name` "
#			query += "from `import_foreign_keys` `fk` "
#			query += "join `import_columns` `s` on "
#			query += "    `s`.`table_id` = `fk`.`table_id` and `s`.`column_id` = `fk`.`column_id` "
#			query += "join `import_columns` `ref` on "
#			query += "    `ref`.`table_id` = `fk`.`fk_table_id` and `ref`.`column_id` = `fk`.`fk_column_id` "
#			query += "order by `fk`.`fk_index`,`fk`.`key_position` "
#
#			self.configDB.execute(query)

		# Create all tables with the current schema
		configSchema.metadata.create_all(self.configDB, checkfirst=True)

		# Insert the version of the database schema
		query = sa.insert(configSchema.dbVersion).values(version='04201')
		self.configDB.execute(query)

		self.updateConfigurationValues()

		print("DBImport configuration database is created successfully")
		


	def upgradeDB(self):

		inspector = sa.inspect(self.configDB)		
		allTables = inspector.get_table_names()
		if "db_version" not in allTables:
			print("ERROR: Cant find the configured tables in the database. Have you created the database with --createDB?")
			return


		query = sa.select([sa.func.max(configSchema.dbVersion.version)])
		result = self.configDB.execute(query).fetchone()
		deployedVersion = result[0]
		if deployedVersion == 4201:
			print("This database is at the latest level. No need to upgrade")

		self.updateConfigurationValues()

	def updateConfigurationValues(self):
		
		query = sa.select([configSchema.jdbcConnectionsDrivers.database_type])
		result_df = pd.DataFrame(self.configDB.execute(query).fetchall())

		if result_df.empty or (result_df[0] == 'DB2 AS400').any() == False:
			query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
				database_type='DB2 AS400', 
				version='default', 
				driver='com.ibm.as400.access.AS400JDBCDriver', 
				classpath='add path to JAR file')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'DB2 UDB').any() == False:
			query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
				database_type='DB2 UDB', 
				version='default', 
				driver='com.ibm.db2.jcc.DB2Driver', 
				classpath='add path to JAR file')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'MySQL').any() == False:
			query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
				database_type='MySQL', 
				version='default', 
				driver='com.mysql.jdbc.Driver', 
				classpath='add path to JAR file')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'Oracle').any() == False:
			query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
				database_type='Oracle', 
				version='default', 
				driver='oracle.jdbc.driver.OracleDriver', 
				classpath='add path to JAR file')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'PostgreSQL').any() == False:
			query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
				database_type='PostgreSQL', 
				version='default', 
				driver='org.postgresql.Driver', 
				classpath='add path to JAR file')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'Progress DB').any() == False:
			query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
				database_type='Progress DB', 
				version='default', 
				driver='com.ddtek.jdbc.openedge.OpenEdgeDriver', 
				classpath='add path to JAR file')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'SQL Server').any() == False:
			query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
				database_type='SQL Server', 
				version='default', 
				driver='com.microsoft.sqlserver.jdbc.SQLServerDriver', 
				classpath='add path to JAR file')
			self.configDB.execute(query)

			query = sa.insert(configSchema.jdbcConnectionsDrivers).values(
				database_type='SQL Server', 
				version='jTDS', 
				driver='net.sourceforge.jtds.jdbc.Driver', 
				classpath='add path to JAR file')
			self.configDB.execute(query)


#		query = sa.select([configSchema.dbVersion.version, configSchema.dbVersion.install_date])
		query = sa.select([configSchema.configuration.configKey])
		result_df = pd.DataFrame(self.configDB.execute(query).fetchall())

		if result_df.empty or (result_df[0] == 'airflow_disable').any() == False:
			query = sa.insert(configSchema.configuration).values(
				configKey='airflow_disable', 
				valueInt='0', 
				description='Disable All executions from Airflow. This is what the \"start\" Task is looking at')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'export_stage_disable').any() == False:
			query = sa.insert(configSchema.configuration).values(
				configKey='export_stage_disable', 
				valueInt='0', 
				description='With 1, you prevent new Export tasks from starting and running tasks will stop after the current stage is completed.')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'export_staging_database').any() == False:
			query = sa.insert(configSchema.configuration).values(
				configKey='export_staging_database', 
				valueStr='etl_export_staging', 
				description='Name of staging database to use during Exports')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'export_start_disable').any() == False:
			query = sa.insert(configSchema.configuration).values(
				configKey='export_start_disable', 
				valueInt='0', 
				description='With 1, you prevent new Export tasks from starting. Running tasks will be completed')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'import_stage_disable').any() == False:
			query = sa.insert(configSchema.configuration).values(
				configKey='import_stage_disable', 
				valueInt='0', 
				description='With 1, you prevent new tasks from starting and running Import tasks will stop after the current stage is completed.')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'import_staging_database').any() == False:
			query = sa.insert(configSchema.configuration).values(
				configKey='import_staging_database', 
				valueStr='etl_import_staging', 
				description='Name of staging database to use during Imports')
			self.configDB.execute(query)

		if result_df.empty or (result_df[0] == 'import_start_disable').any() == False:
			query = sa.insert(configSchema.configuration).values(
				configKey='import_start_disable', 
				valueInt='0', 
				description='With 1, you prevent new Import tasks from starting. Running tasks will be completed')
			self.configDB.execute(query)
			
		if result_df.empty or (result_df[0] == 'hive_remove_locks_by_force').any() == False:
			query = sa.insert(configSchema.configuration).values(
				configKey='hive_remove_locks_by_force', 
				valueInt='0', 
				description='With 1, DBImport will remove Hive locks before import by force')
			self.configDB.execute(query)

