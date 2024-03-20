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
from DBImportConfig import common_config
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select, func, update
from sqlalchemy.orm import aliased, sessionmaker, Query
import sqlalchemy.orm.exc

class dbCalls:
	def __init__(self):

		logFormat = '%(asctime)s %(levelname)s - %(message)s'
		logging.basicConfig(format=logFormat, level=logging.INFO)
		logPropagate = True

		self.logger = "server"
		log = logging.getLogger(self.logger)
		log.debug("Executing Server.restServerCalls.__init__()")

		self.mysql_conn = None
		self.mysql_cursor = None
		self.debugLogLevel = False
		self.configDBSession = None

		if logging.root.level == 10:        # DEBUG
			self.debugLogLevel = True

		self.common_config = common_config.config()

		self.logdir = configuration.get("Server", "logdir")

		self.allConfigKeys = [ "airflow_aws_instanceids", "airflow_aws_pool_to_instanceid", "airflow_create_pool_with_task", "airflow_dag_directory", "airflow_dag_file_group", "airflow_dag_file_permission", "airflow_dag_staging_directory", "airflow_dbimport_commandpath", "airflow_default_pool_size", "airflow_disable", "airflow_dummy_task_queue", "airflow_major_version", "airflow_sudo_user", "atlas_discovery_interval", "cluster_name", "export_default_sessions", "export_max_sessions", "export_stage_disable", "export_staging_database", "export_start_disable", "hdfs_address", "hdfs_basedir", "hdfs_blocksize", "hive_acid_with_clusteredby", "hive_insert_only_tables", "hive_major_compact_after_merge", "hive_print_messages", "hive_remove_locks_by_force", "hive_validate_before_execution", "hive_validate_table", "impala_invalidate_metadata", "import_columnname_delete", "import_columnname_histtime", "import_columnname_import", "import_columnname_insert", "import_columnname_iud", "import_columnname_source", "import_columnname_update", "import_default_sessions", "import_history_database", "import_history_table", "import_max_sessions", "import_process_empty", "import_stage_disable", "import_staging_database", "import_staging_table", "import_start_disable", "import_work_database", "import_work_table", "kafka_brokers", "kafka_saslmechanism", "kafka_securityprotocol", "kafka_topic", "kafka_trustcafile", "post_airflow_dag_operations", "post_data_to_kafka", "post_data_to_kafka_extended", "post_data_to_rest", "post_data_to_rest_extended", "rest_timeout", "rest_trustcafile", "rest_url", "rest_verifyssl", "spark_max_executors", "timezone" ]


	def disconnectDBImportDB(self):
		""" Disconnects from the database and removes all sessions and engine """
		log = logging.getLogger(self.logger)
		self.common_config.disconnectSQLAlchemy(logger=self.logger)
		self.configDBSession = None


	def getDBImportSession(self):
		log = logging.getLogger(self.logger)
		if self.configDBSession == None:
			if self.common_config.connectSQLAlchemy(exitIfFailure=False, logger=self.logger) == False:
				raise SQLerror("Can't connect to DBImport database")

		return self.common_config.configDBSession()


	def disconnectRemoteSession(self, instance):
		""" Disconnects from the remote database and removes all sessions and engine """
		log = logging.getLogger(self.logger)

		try:
			engine = self.remoteDBImportEngines.get(instance)
			if engine != None:
				log.info("Disconnecting from remote DBImport database for '%s'"%(instance))
				engine.dispose()
			self.remoteDBImportEngines.pop(instance)
			self.remoteDBImportSessions.pop(instance)
		except KeyError:
			log.debug("Cant remove DBImport session or engine. Key does not exist")

	def getJDBCdrivers(self):
		""" Returns all JDBC Driver configuration """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		tableJDBCconnectionsDrivers = aliased(configSchema.jdbcConnectionsDrivers)
		jdbcConnectionsDrivers = (session.query(
					tableJDBCconnectionsDrivers.database_type,
					tableJDBCconnectionsDrivers.version,
					tableJDBCconnectionsDrivers.driver,
					tableJDBCconnectionsDrivers.classpath
				)
				.select_from(tableJDBCconnectionsDrivers)
				.order_by(tableJDBCconnectionsDrivers.database_type)
				.all()
			)


		listOfJDBCdrivers = []
		for row in jdbcConnectionsDrivers:
			jdbcDriver = {}
			jdbcDriver["databaseType"] = row[0]
			jdbcDriver["version"] = row[1]
			jdbcDriver["driver"] = row[2]
			jdbcDriver["classpath"] = row[3]
			listOfJDBCdrivers.append(jdbcDriver)

		jsonResult = json.loads(json.dumps(listOfJDBCdrivers))
		session.close()

		return jsonResult


	def setJDBCdriver(self, jdbcDriver, currentUser):
		""" Returns all configuration items from the configuration table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		print(jdbcDriver)

#		log.info("User '%s' updated global configuration. %s = %s"%(currentUser, key, getattr(configuration, key)))

		log.debug("databaseType: %s"%getattr(jdbcDriver, "databaseType"))
		log.debug("version: %s"%getattr(jdbcDriver, "version"))
		log.debug("driver: %s"%getattr(jdbcDriver, "driver"))
		log.debug("classpath: %s"%getattr(jdbcDriver, "classpath"))

#		valueColumn, boolValue = self.common_config.getConfigValueColumn(key)	
#
		tableJDBCconnectionsDrivers = aliased(configSchema.jdbcConnectionsDrivers)
		result = "ok"
		returnCode = 200

		try:
			session.execute(update(tableJDBCconnectionsDrivers),
				[
					{
					"database_type": getattr(jdbcDriver, "databaseType"), 
					"version": getattr(jdbcDriver, "version"), 
					"driver": getattr(jdbcDriver, "driver"), 
					"classpath": getattr(jdbcDriver, "classpath") 
					}
				],
				)
			session.commit()
		# except SQLAlchemyError.StaleDataError:
		except sqlalchemy.orm.exc.StaleDataError:
			result = "JDBC Driver databaseType with version is not a supported combination"
			returnCode = 400
					
		resultDict = {}
		resultDict["status"] = result
		session.close()

		jsonResult = json.loads(json.dumps(resultDict))
		return (jsonResult, returnCode)

	def getConfiguration(self):
		""" Returns all configuration items from the configuration table """
		log = logging.getLogger(self.logger)


		self.common_config.reconnectConfigDatabase()

		resultDict = {}
		for key in self.allConfigKeys:
			resultDict[key] = self.common_config.getConfigValue(key)

		jsonResult = json.loads(json.dumps(resultDict))
		return jsonResult

	def setConfiguration(self, configuration, currentUser):
		""" Returns all configuration items from the configuration table """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		configurationTable = aliased(configSchema.configuration)

		for key in self.allConfigKeys:
			if type(getattr(configuration, key)) in (str, bool, int):
				# At this point, only configuration items that exists in the class is iterrated
				log.info("User '%s' updated global configuration. %s = %s"%(currentUser, key, getattr(configuration, key)))

				valueColumn, boolValue = self.common_config.getConfigValueColumn(key)	

				session.execute(update(configurationTable),
					[
						{"configKey": key, valueColumn: getattr(configuration, key) }
					],
					)
				session.commit()
					
		resultDict = {}
		resultDict["status"] = "ok"
		session.close()

		jsonResult = json.loads(json.dumps(resultDict))
		return jsonResult

	def getDBImportImportTableDBs(self):
		""" Returns all databases that have imports configured in them """
		log = logging.getLogger(self.logger)

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

		jsonResult = json.loads(json.dumps(listOfDBs))
		session.close()

		return jsonResult
	

	def getDBImportImportTables(self, db, details):
		""" Returns all import tables in a specific database """
		log = logging.getLogger(self.logger)

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


		jsonResult = json.loads(json.dumps(listOfTables))
		session.close()

		return jsonResult
	

		
	def getDBImportImportTableDetails(self, db, table):
		""" Returns all import table details """
		log = logging.getLogger(self.logger)

		try:
			session = self.getDBImportSession()
		except SQLerror:
			self.disconnectDBImportDB()
			return None

		importTables = aliased(configSchema.importTables)
		listOfTables = []

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
					importTables.mergeCompactionMethod,
					importTables.import_database,
					importTables.import_table,
					importTables.history_database,
					importTables.history_table
				)
				.select_from(importTables)
				.filter((importTables.hive_db == db) & (importTables.hive_table == table))
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
		resultDict['import_database'] = row[65]
		resultDict['import_table'] = row[66]
		resultDict['history_database'] = row[67]
		resultDict['history_table'] = row[68]

		jsonResult = json.loads(json.dumps(resultDict))
		session.close()

		return jsonResult
	

		
