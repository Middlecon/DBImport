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
import re
import sys
import pty
import errno
import time
import logging
import signal
import subprocess
import shlex
import pandas as pd
import Crypto
import binascii
import json
from fastapi.encoders import jsonable_encoder

from ConfigReader import configuration
from datetime import date, datetime, timedelta
from common import constants as constant
from common.Exceptions import *
from DBImportConfig import configSchema
# from DBImportConfig import common_config
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select, func
from sqlalchemy.orm import aliased, sessionmaker, Query

class dbCalls:
	def __init__(self):

		logFormat = '%(asctime)s %(levelname)s - %(message)s'
		logging.basicConfig(format=logFormat, level=logging.INFO)
		logPropagate = True

		log = logging.getLogger()
		log.debug("Executing Server.restServerCalls.__init__()")

		self.mysql_conn = None
		self.mysql_cursor = None
		self.debugLogLevel = False

		if logging.root.level == 10:        # DEBUG
			self.debugLogLevel = True

#		self.common_config = common_config.config()

		self.logdir = configuration.get("Server", "logdir")

#		self.crypto = self.common_config.crypto
#		self.crypto.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
#		self.crypto.setPublicKeyFile(configuration.get("Credentials", "public_key"))

#		self.remoteDBImportEngines = {}
#		self.remoteDBImportSessions = {}
#		self.remoteInstanceConfigDB = None

		self.configDBSession = None
		self.configDBEngine = None

#		self.distCPreqQueue = Queue()
#		self.distCPresQueue = Queue()
#		self.threadStopEvent = threading.Event()

		# Start the Atlas Discovery Thread
#		self.atlasDiscoveryThread = atlasDiscovery.atlasDiscovery(self.threadStopEvent)
#		self.atlasDiscoveryThread.daemon = True
#		self.atlasDiscoveryThread.start()

		# Fetch configuration about MySQL database and how to connect to it
		self.configHostname = configuration.get("Database", "mysql_hostname")
		self.configPort =     configuration.get("Database", "mysql_port")
		self.configDatabase = configuration.get("Database", "mysql_database")
		self.configUsername = configuration.get("Database", "mysql_username")
		self.configPassword = configuration.get("Database", "mysql_password")

		session = self.getDBImportSession()
		return


	def disconnectDBImportDB(self):
		""" Disconnects from the database and removes all sessions and engine """
		log = logging.getLogger("server")

		if self.configDBEngine != None:
			log.info("Disconnecting from DBImport database")
			self.configDBEngine.dispose()
			self.configDBEngine = None

		self.configDBSession = None

	def getDBImportSession(self):
		log = logging.getLogger("server")
		if self.configDBSession == None:
			if self.connectDBImportDB() == False:
				raise SQLerror("Can't connect to DBImport database")

		return self.configDBSession()	


	def connectDBImportDB(self):
		# Esablish a SQLAlchemy connection to the DBImport database
		log = logging.getLogger("server")
		self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s"%(
			self.configUsername,
			self.configPassword,
			self.configHostname,
			self.configPort,
			self.configDatabase)

		try:
			self.configDBEngine = sa.create_engine(self.connectStr, echo = self.debugLogLevel, pool_pre_ping=True, pool_recycle=3600)
			self.configDBEngine.connect()
			self.configDBSession = sessionmaker(bind=self.configDBEngine)

		except sa.exc.OperationalError as err:
			log.error("%s"%err)
			self.configDBSession = None
			self.configDBEngine = None
			return False
#			self.common_config.remove_temporary_files()
#			sys.exit(1)

		except:
			print("Unexpected error: ")
			print(sys.exc_info())
			self.configDBSession = None
			self.configDBEngine = None
			return False
#			self.common_config.remove_temporary_files()
#			sys.exit(1)

		log.info("Connected successful against DBImport database")
		return True


	def disconnectRemoteSession(self, instance):
		""" Disconnects from the remote database and removes all sessions and engine """
		log = logging.getLogger("server")

		try:
			engine = self.remoteDBImportEngines.get(instance)
			if engine != None:
				log.info("Disconnecting from remote DBImport database for '%s'"%(instance))
				engine.dispose()
			self.remoteDBImportEngines.pop(instance)
			self.remoteDBImportSessions.pop(instance)
		except KeyError:
			log.debug("Cant remove DBImport session or engine. Key does not exist")


	def getDBImportImportTableDBs(self):
		""" Returns all databases that have imports configured in them """
		log = logging.getLogger()

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importTables = aliased(configSchema.importTables)

		importTableDBs = (session.query(
					importTables.hive_db
				)
				.select_from(importTables)
				.group_by(importTables.hive_db)
				.all()
			)

		listOfDBs = []
		for u in importTableDBs:
			listOfDBs.append(u[0])

		jsonResult = json.dumps(listOfDBs)
		session.close()

		return jsonResult
	

	def getDBImportImportTables(self, db, details):
		""" Returns all import tables in a specific database """
		log = logging.getLogger()

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importTables = aliased(configSchema.importTables)
		listOfTables = []

		if details == False:
			# Return a list of hive tables without the details
			importTablesData = (session.query(
						importTables.hive_table
					)
					.select_from(importTables)
					.filter(importTables.hive_db == db)
					.all()
				)

			for row in importTablesData:
				listOfTables.append(row[0])

		else:
			# Return a list of Hive tables with details
			importTablesData = (session.query(
						importTables.hive_table,
						importTables.dbalias,
						importTables.source_schema,
						importTables.source_table,
						importTables.import_phase_type,
						importTables.etl_phase_type,
						importTables.import_tool
					)
					.select_from(importTables)
					.filter(importTables.hive_db == db)
					.all()
				)

			for row in importTablesData:
				tempDict = {}
				tempDict['hiveTable'] = row[0]
				tempDict['dbAlias'] = row[1]
				tempDict['sourceSchema'] = row[2]
				tempDict['sourceTable'] = row[3]
				tempDict['importPhaseType'] = row[4]
				tempDict['etlPhaseType'] = row[5]
				tempDict['importTool'] = row[6]

				listOfTables.append(tempDict)


		jsonResult = json.dumps(listOfTables)
		session.close()

		return jsonResult
	

		
	def getDBImportImportTableDetails(self, db, table):
		""" Returns all import table details """
		log = logging.getLogger()

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importTables = aliased(configSchema.importTables)
		listOfTables = []

##		# Return a list of hive tables without the details
#		importTablesData = (session.query(*)
#				.select_from(importTables)
#				.filter((importTables.hive_db == db) & (importTables.hive_table == table))
#				.all()).fillna('')
#			)
#
#			for row in importTablesData:
#				listOfTables.append(row[0])

#		else:
		# Return a list of Hive tables with details
		row = (session.query(
    				importTables.table_id,
					importTables.dbalias,
					importTables.source_schema,
					importTables.source_table,
					importTables.import_phase_type,
					importTables.etl_phase_type,
					importTables.import_tool,
					importTables.etl_engine,
					importTables.last_update_from_source,
					importTables.sqoop_sql_where_addition,
					importTables.nomerge_ingestion_sql_addition,
					importTables.include_in_airflow,
					importTables.airflow_priority,
					importTables.validate_import,
					importTables.validationMethod,
					importTables.validate_source,
					importTables.validate_diff_allowed,
					importTables.validationCustomQuerySourceSQL,
					importTables.validationCustomQueryHiveSQL,
					importTables.validationCustomQueryValidateImportTable,
					importTables.truncate_hive,
					importTables.mappers,
					importTables.soft_delete_during_merge,
					importTables.source_rowcount,
					importTables.source_rowcount_incr,
					importTables.hive_rowcount,
					importTables.validationCustomQuerySourceValue,
					importTables.validationCustomQueryHiveValue,
					importTables.incr_mode,
					importTables.incr_column,
					importTables.incr_validation_method,
					importTables.incr_minvalue,
					importTables.incr_maxvalue,
					importTables.incr_minvalue_pending,
					importTables.incr_maxvalue_pending,
					importTables.pk_column_override,
					importTables.pk_column_override_mergeonly,
					importTables.hive_merge_heap,
					importTables.hive_split_count,
					importTables.spark_executor_memory,
					importTables.spark_executors,
					importTables.concatenate_hive_table,
					importTables.split_by_column,
					importTables.sqoop_query,
					importTables.sqoop_options,
					importTables.sqoop_last_size,
					importTables.sqoop_last_rows,
					importTables.sqoop_last_mappers,
					importTables.sqoop_last_execution,
					importTables.sqoop_use_generated_sql,
					importTables.sqoop_allow_text_splitter,
					importTables.force_string,
					importTables.comment,
					importTables.generated_hive_column_definition,
					importTables.generated_sqoop_query,
					importTables.generated_sqoop_options,
					importTables.generated_pk_columns,
					importTables.generated_foreign_keys,
					importTables.datalake_source,
					importTables.operator_notes,
					importTables.copy_finished,
					importTables.copy_slave,
					importTables.create_foreign_keys,
					importTables.custom_max_query,
					importTables.mergeCompactionMethod
				)


#    source_schema = Column(String(256), nullable=False, comment='Name of the schema in the remote database')
#    source_table = Column(String(256), nullable=False, comment='Name of the table in the remote database')
#    import_type = Column(String(32), nullable=True, comment='What import method to use')
#    import_phase_type = Column(Enum('full', 'incr', 'oracle_flashback', 'mssql_change_tracking'), nullable=True, comment="What method to use for Import phase", server_default=text("'full'"))
#    etl_phase_type = Column(Enum('truncate_insert', 'insert', 'merge', 'merge_history_audit', 'none', 'external'), nullable=True, comment="What method to use for ETL phase", server_default=text("'truncate_insert'"))
#    import_tool = Column(Enum('spark', 'sqoop'), server_default=text("'sqoop'"), nullable=False, comment='What tool should be used for importing data')
#    etl_engine = Column(Enum('hive', 'spark'), server_default=text("'hive'"), nullable=False, comment='What engine will be used to process etl stage')
#    last_update_from_source = Column(DateTime, comment='Timestamp of last schema update from source')
#    sqoop_sql_where_addition = Column(String(1024), comment='Will be added AFTER the SQL WHERE. If it\'s an incr import, this will be after the incr limit statements. Example "orderId > 1000"')
#    nomerge_ingestion_sql_addition = Column(String(2048), comment='This will be added to the data ingestion of None-Merge imports (full, full_direct and incr). Usefull to filter out data from import tables to target tables')
#    include_in_airflow = Column(TINYINT(4), nullable=False, comment='Will the table be included in Airflow DAG when it matches the DAG selection', server_default=text("'1'"))
#    airflow_priority = Column(TINYINT(4), comment='This will set priority_weight in Airflow')
#    validate_import = Column(TINYINT(4), nullable=False, comment='Should the import be validated', server_default=text("'1'"))
#    validationMethod = Column(Enum('customQuery', 'rowCount'), nullable=False, comment='Validation method to use', server_default=text("'rowCount'"))
#    validate_source = Column(Enum('query', 'sqoop'), comment="query = Run a 'select count(*) from ...' to get the number of rows in the source table. sqoop = Use the number of rows imported by sqoop as the number of rows in the source table", server_default=text("'query'"))
#    validate_diff_allowed = Column(BIGINT(20), nullable=False, comment='-1 = auto calculated diff allowed. If a positiv number, this is the amount of rows that the diff is allowed to have', server_default=text("'-1'"))
#    validationCustomQuerySourceSQL = Column(Text, comment='Custom SQL query for source table')
#    validationCustomQueryHiveSQL = Column(Text, comment='Custom SQL query for Hive table')
#    validationCustomQueryValidateImportTable = Column(TINYINT(4), nullable=False, comment='1 = Validate Import table, 0 = Dont validate Import table', server_default=text("'1'"))
#    truncate_hive = Column(TINYINT(4), nullable=False, comment='<NOT USED>', server_default=text("'1'"))
#    mappers = Column(TINYINT(4), nullable=False, comment="-1 = auto or positiv number for a fixed number of mappers. If Auto, then it's calculated based of last sqoop import size", server_default=text("'-1'"))
#    soft_delete_during_merge = Column(TINYINT(4), nullable=False, comment='If 1, then the row will be marked as deleted instead of actually being removed from the table. Only used for Merge imports', server_default=text("'0'"))
#    source_rowcount = Column(BIGINT(20), comment='Used for validation. Dont change manually')
#    source_rowcount_incr = Column(BIGINT(20))
#    hive_rowcount = Column(BIGINT(20), comment='Used for validation. Dont change manually')
#    validationCustomQuerySourceValue = Column(Text, comment='Used for validation. Dont change manually')
#    validationCustomQueryHiveValue = Column(Text, comment='Used for validation. Dont change manually')
#    incr_mode = Column(Enum('append', 'lastmodified'), comment='Incremental import mode')
#    incr_column = Column(String(256), comment='What column to use to identify new rows')
#    incr_validation_method = Column(Enum('full', 'incr'), comment='full or incr. Full means that the validation will check to total number of rows up until maxvalue and compare source with target. Incr will only compare the rows between min and max value (the data that sqoop just wrote)', server_default=text("'full'"))
#    incr_minvalue = Column(String(32), comment='Used for incremental imports. Dont change manually')
#    incr_maxvalue = Column(String(32), comment='Used for incremental imports. Dont change manually')
#    incr_minvalue_pending = Column(String(32), comment='Used for incremental imports. Dont change manually')
#    incr_maxvalue_pending = Column(String(32), comment='Used for incremental imports. Dont change manually')
#    pk_column_override = Column(String(1024), comment='Force the import and Hive table to define another PrimaryKey constraint. Comma separeted list of columns')
#    pk_column_override_mergeonly = Column(String(1024), comment='Force the import to use another PrimaryKey constraint during Merge operations. Comma separeted list of columns')
#    hive_merge_heap = Column(Integer, comment='Should be a multiple of Yarn container size. If NULL then it will use the default specified in Yarn and TEZ')
#    hive_split_count = Column(Integer, comment='Sets tez.grouping.split-count in the Hive session')
#    spark_executor_memory = Column(String(8), comment='Memory used by spark when importing data. Overrides default value in global configuration')
#    spark_executors = Column(Integer, comment='Number of Spark executors to use. Overrides default value in global configuration')
#    concatenate_hive_table = Column(TINYINT(4), nullable=False, comment='<NOT USED>', server_default=text("'-1'"))
#    split_by_column = Column(String(64), comment='Column to split by when doing import with multiple sessions')
#    sqoop_query = Column(Text, comment='Use a custom query in sqoop to read data from source table')
#    sqoop_options = Column(Text, comment='Options to send to sqoop.')
#    sqoop_last_size = Column(BIGINT(20), comment='Used to track sqoop operation. Dont change manually')
#    sqoop_last_rows = Column(BIGINT(20), comment='Used to track sqoop operation. Dont change manually')
#    sqoop_last_mappers = Column(TINYINT(4), comment='Used to track sqoop operation. Dont change manually')
#    sqoop_last_execution = Column(BIGINT(20), comment='Used to track sqoop operation. Dont change manually')
#    sqoop_use_generated_sql = Column(TINYINT(4), nullable=False, comment='1 = Use the generated SQL that is saved in the generated_sqoop_query column', server_default=text("'-1'"))
#    sqoop_allow_text_splitter = Column(TINYINT(4), nullable=False, comment='Allow splits on text columns. Use with caution', server_default=text("'0'"))
#    force_string = Column(TINYINT(4), nullable=False, comment='If set to 1, all character based fields (char, varchar) will become string in Hive. Overrides the same setting in jdbc_connections table', server_default=text("'-1'"))
#    comment = Column(Text, comment='Table comment from source system. Dont change manually')
#    generated_hive_column_definition = Column(Text, comment='Generated column definition for Hive create table. Dont change manually')
#    generated_sqoop_query = Column(Text, comment='Generated query for sqoop. Dont change manually')
#    generated_sqoop_options = Column(Text, comment='Generated options for sqoop. Dont change manually')
#    generated_pk_columns = Column(Text, comment='Generated Primary Keys. Dont change manually')
#    generated_foreign_keys = Column(Text, comment='<NOT USED>')
#    datalake_source = Column(String(256), comment='This value will come in the dbimport_source column if present. Overrides the same setting in jdbc_connections table')
#    operator_notes = Column(Text, comment='Free text field to write a note about the import. ')
#    copy_finished = Column(DateTime, comment='Time when last copy from Master DBImport instance was completed. Dont change manually')
#    copy_slave = Column(TINYINT(4), nullable=False, comment='Defines if this table is a Master table or a Slave table. Dont change manually', server_default=text("'0'"))
#    create_foreign_keys = Column(TINYINT(4), nullable=False, comment='-1 (default) = Get information from jdbc_connections table', server_default=text("'-1'"))
#    custom_max_query = Column(String(256), comment='You can use a custom SQL query that will get the Max value from the source database. This Max value will be used in an inremental import to know how much to read in each execution')
#    mergeCompactionMethod = Column(Enum('default', 'none', 'minor', 'minor_and_wait', 'major', 'major_and_wait'), nullable=False, comment='Compaction method to use after import using merge is completed. Default means a major compaction if it is configured to do so in the configuration table', server_default=text("'default'"))



				.select_from(importTables)
				.filter((importTables.hive_db == db) & (importTables.hive_table == table))
#				.all().fillna('')
				.one()
			)

		resultDict = {}
		resultDict['table_id'] = row[0]
		resultDict['hiveDB'] = db
		resultDict['hiveTable'] = table
		resultDict['dbAlias'] = row[1]
		resultDict['sourceSchema'] = row[2]
		resultDict['sourceTable'] = row[3]
		resultDict['importPhaseType'] = row[4]
		resultDict['etlPhaseType'] = row[5]
		resultDict['importTool'] = row[6]
		resultDict['etlEngine'] = row[7]
		try:
			resultDict['lastUpdateFromSource'] = row[8].strftime("%Y-%m-%d %H:%M:%S")
		except AttributeError:
			resultDict['lastUpdateFromSource'] = None
		resultDict['sqoopSQLwhereAddition'] = row[9]
		resultDict['nomergeIngestionSQLaddition'] = row[10]
		resultDict['includeInAirflow'] = row[11]
		resultDict['airflowPriority'] = row[12]
		resultDict['validateImport'] = row[13]
		resultDict['validationMethod'] = row[14]
		resultDict['validateSource'] = row[15]
		resultDict['validateDiffAllowed'] = row[16]
		resultDict['validationCustomQuerySourceSQL'] = row[17]
		resultDict['validationCustomQueryHiveSQL'] = row[18]
		resultDict['validationCustomQueryValidateImportTable'] = row[19]
		resultDict['truncateHive'] = row[20]
		resultDict['mappers'] = row[21]
		resultDict['softDeleteDuringMerge'] = row[22]
		resultDict['sourceRowcount'] = row[23]
		resultDict['sourceRowcountIncr'] = row[24]
		resultDict['hiveRowcount'] = row[25]
		resultDict['validationCustomQuerySourceValue'] = row[26]
		resultDict['validationCustomQueryHiveValue'] = row[27]
		resultDict['incrMode'] = row[28]
		resultDict['incrColumn'] = row[29]
		resultDict['incrValidationMethod'] = row[30]
		resultDict['incrMinvalue'] = row[31]
		resultDict['incrMaxvalue'] = row[32]
		resultDict['incrMinvaluePending'] = row[33]
		resultDict['incrMaxvaluePending'] = row[34]
		resultDict['pkColumnOverride'] = row[35]
		resultDict['pkColumnOverrideMergeonly'] = row[36]
		resultDict['hiveMergeHeap'] = row[37]
		resultDict['hiveSplitCount'] = row[38]
		resultDict['sparkExecutorMemory'] = row[39]
		resultDict['sparkExecutors'] = row[40]
		resultDict['concatenateHiveTable'] = row[41]
		resultDict['splitByColumn'] = row[42]
		resultDict['sqoopQuery'] = row[43]
		resultDict['sqoopOptions'] = row[44]
		resultDict['sqoopLastSize'] = row[45]
		resultDict['sqoopLastRows'] = row[46]
		resultDict['sqoopLastMappers'] = row[47]
		resultDict['sqoopLastExecution'] = row[48]
		resultDict['sqoopUseGeneratedSQL'] = row[49]
		resultDict['sqoopAllowTextSplitter'] = row[50]
		resultDict['forceString'] = row[51]
		resultDict['comment'] = row[52]
		resultDict['generatedHiveColumnDefinition'] = row[53]
		resultDict['generatedSqoopQuery'] = row[54]
		resultDict['generatedQqoopOptions'] = row[55]
		resultDict['generatedPKcolumns'] = row[56]
		resultDict['generatedForeignKeys'] = row[57]
		resultDict['datalakeSource'] = row[58]
		resultDict['operatorNotes'] = row[59]
		try:
			resultDict['copyFinished'] = row[60].strftime("%Y-%m-%d %H:%M:%S")
		except AttributeError:
			resultDict['copyFinished'] = None
		resultDict['copySlave'] = row[61]
		resultDict['createForeignKeys'] = row[62]
		resultDict['customMaxQuery'] = row[63]
		resultDict['mergeCompactionMethod'] = row[64]

		print(resultDict)

		jsonResult = json.dumps(resultDict)
		print(jsonResult)
		session.close()

		return jsonResult
	

		
