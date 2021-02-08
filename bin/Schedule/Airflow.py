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
import re
from ConfigReader import configuration
from datetime import date, datetime, time, timedelta
import pandas as pd
from common import constants as constant
from common.Exceptions import *
from Schedule import airflowSchema
from DBImportConfig import configSchema
from DBImportConfig import common_config
import sqlalchemy as sa
# from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select
from sqlalchemy.orm import aliased, sessionmaker, Query


class initialize(object):
	def __init__(self):
		logging.debug("Executing Airflow.__init__()")

		self.mysql_conn = None
		self.mysql_cursor = None
		self.debugLogLevel = False

		if logging.root.level == 10:        # DEBUG
			self.debugLogLevel = True

		self.common_config = common_config.config()

		self.dbimportCommandPath = self.common_config.getConfigValue("airflow_dbimport_commandpath")
		self.defaultSudoUser = self.common_config.getConfigValue("airflow_sudo_user")
		self.DAGdirectory = self.common_config.getConfigValue("airflow_dag_directory")
		self.DAGstagingDirectory = self.common_config.getConfigValue("airflow_dag_staging_directory")
		self.DAGfileGroup = self.common_config.getConfigValue("airflow_dag_file_group")
		self.DAGfilePermission = self.common_config.getConfigValue("airflow_dag_file_permission")
		self.TaskQueueForDummy = self.common_config.getConfigValue("airflow_dummy_task_queue")
		self.timeZone = self.common_config.getConfigValue("timezone")
		
		self.DAGfile = None
		self.DAGfilename = None
		self.DAGfilenameInAirflow = None
		self.writeDAG = None

		self.sensorStartTask = None
		self.sensorStopTask = None
		self.preStartTask = None
		self.preStopTask = None
		self.mainStartTask = None
		self.mainStopTask = None
		self.postStartTask = None
		self.postStopTask = None

		# Fetch configuration about MySQL database and how to connect to it
		self.configHostname = configuration.get("Database", "mysql_hostname")
		self.configPort =     configuration.get("Database", "mysql_port")
		self.configDatabase = configuration.get("Database", "mysql_database")
		self.configUsername = configuration.get("Database", "mysql_username")
		self.configPassword = configuration.get("Database", "mysql_password")

		# Esablish a SQLAlchemy connection to the DBImport database 
		self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s"%(
			self.configUsername, 
			self.configPassword, 
			self.configHostname, 
			self.configPort, 
			self.configDatabase)

		try:
			self.configDB = sa.create_engine(self.connectStr, echo = self.debugLogLevel)
			self.configDB.connect()
			self.configDBSession = sessionmaker(bind=self.configDB)

		except sa.exc.OperationalError as err:
			logging.error("%s"%err)
			self.common_config.remove_temporary_files()
			sys.exit(1)
		except:
			print("Unexpected error: ")
			print(sys.exc_info())
			self.common_config.remove_temporary_files()
			sys.exit(1)

		# Esablish a SQLAlchemy connection to the Airflow database
		airflowConnectStr = configuration.get("Airflow", "airflow_alchemy_conn")
		try:
			self.airflowDB = sa.create_engine(airflowConnectStr, echo = self.debugLogLevel)
			self.airflowDB.connect()
			self.airflowDBSession = sessionmaker(bind=self.airflowDB)

		except sa.exc.OperationalError as err:
			logging.error("%s"%err)
			self.common_config.remove_temporary_files()
			sys.exit(1)
		except:
			print("Unexpected error: ")
			print(sys.exc_info())
			self.common_config.remove_temporary_files()
			sys.exit(1)


		logging.debug("Executing Airflow.__init__() - Finished")

	def checkExecution(self):
		""" Checks the 'airflow_disable' settings and exit with 0 or 1 depending on that """

		airflowExecutionDisabled = self.common_config.getConfigValue("airflow_disable")
		if airflowExecutionDisabled == False:
			print("Airflow execution is enabled")
			self.common_config.remove_temporary_files()
			sys.exit(0)
		else:
			print("Airflow execution is disabled")
			self.common_config.remove_temporary_files()
			sys.exit(1)

	def getDBImportCommandPath(self, sudoUser=""):

		if sudoUser == None or sudoUser == "":
			sudoUser = self.defaultSudoUser

		return self.dbimportCommandPath.replace("${SUDO_USER}", sudoUser)

	def generateDAG(self, name=None, writeDAG=False, autoDAGonly=False):

		self.writeDAG = writeDAG

		session = self.configDBSession()
		airflowCustomDags = aliased(configSchema.airflowCustomDags)
		airflowExportDags = aliased(configSchema.airflowExportDags)
		airflowImportDags = aliased(configSchema.airflowImportDags)
		airflowEtlDags = aliased(configSchema.airflowEtlDags)

		exportDAG = pd.DataFrame(session.query(
				airflowExportDags.dag_name,
				airflowExportDags.schedule_interval,
				airflowExportDags.filter_dbalias,
				airflowExportDags.filter_target_schema,
				airflowExportDags.filter_target_table,
				airflowExportDags.retries,
				airflowExportDags.auto_regenerate_dag,
				airflowExportDags.sudo_user
			)
			.select_from(airflowExportDags)
			.all()).fillna('')

		importDAG = pd.DataFrame(session.query(
				airflowImportDags.dag_name,
				airflowImportDags.schedule_interval,
				airflowImportDags.filter_hive,
				airflowImportDags.retries,
				airflowImportDags.retries_stage1,
				airflowImportDags.retries_stage2,
				airflowImportDags.pool_stage1,
				airflowImportDags.pool_stage2,
				airflowImportDags.run_import_and_etl_separate,
				airflowImportDags.finish_all_stage1_first,
				airflowImportDags.auto_regenerate_dag,
				airflowImportDags.sudo_user
			)
			.select_from(airflowImportDags)
			.all()).fillna('')

		etlDAG = pd.DataFrame(session.query(
				airflowEtlDags.dag_name,
				airflowEtlDags.schedule_interval,
				airflowEtlDags.filter_job,
				airflowEtlDags.filter_task,
				airflowEtlDags.filter_source_db,
				airflowEtlDags.filter_target_db,
				airflowEtlDags.retries,
				airflowEtlDags.auto_regenerate_dag,
				airflowEtlDags.sudo_user
			)
			.select_from(airflowEtlDags)
			.all()).fillna('')

		customDAG = pd.DataFrame(session.query(
				airflowCustomDags.dag_name,
				airflowCustomDags.schedule_interval,
				airflowCustomDags.retries,
				airflowCustomDags.auto_regenerate_dag,
				airflowCustomDags.sudo_user
			)
			.select_from(airflowCustomDags)
			.all()).fillna('')

		if name != None:
			if importDAG.empty == False:
				importDAG = importDAG.loc[importDAG['dag_name'] == name]
			if exportDAG.empty == False:
				exportDAG = exportDAG.loc[exportDAG['dag_name'] == name]
			if customDAG.empty == False:
				customDAG = customDAG.loc[customDAG['dag_name'] == name]

		if autoDAGonly == True:
			if importDAG.empty == False:
				importDAG = importDAG.loc[importDAG['auto_regenerate_dag'] == 1]
			if exportDAG.empty == False:
				exportDAG = exportDAG.loc[exportDAG['auto_regenerate_dag'] == 1]
			if customDAG.empty == False:
				customDAG = customDAG.loc[customDAG['auto_regenerate_dag'] == 1]

		dagFound = False

		if name == None or len(importDAG) > 0: 
			dagFound = True
			for index, row in importDAG.iterrows():
				self.generateImportDAG(DAG=row)

		if name == None or len(exportDAG) > 0: 
			dagFound = True
			for index, row in exportDAG.iterrows():
				self.generateExportDAG(DAG=row)

		if name == None or len(customDAG) > 0: 
			dagFound = True
			for index, row in customDAG.iterrows():
				self.generateCustomDAG(DAG=row)

		if dagFound == False and name != None:
			logging.error("Can't find DAG with that name")
			self.common_config.remove_temporary_files()
			sys.exit(1)

		session.close()

	def getAirflowHostPoolName(self):
		hostname = self.common_config.jdbc_hostname.lower().split("/")[0].split("\\")[0] 
		poolName = "DBImport_server_%s"%(hostname)
		
		return poolName[0:50]

	def generateExportDAG(self, DAG):
		""" Generates a Import DAG """

		usedPools = []
		tableFilters = []
		defaultPool = DAG['dag_name']
		sudoUser = DAG['sudo_user']
		usedPools.append(defaultPool)

		cronSchedule = self.convertTimeToCron(DAG["schedule_interval"])
		self.createDAGfileWithHeader(dagName = DAG['dag_name'], cronSchedule = cronSchedule, defaultPool = defaultPool, sudoUser = sudoUser)

		session = self.configDBSession()
		exportTables = aliased(configSchema.exportTables)

		exportTablesQuery = Query([exportTables.target_schema, exportTables.target_table, exportTables.dbalias, exportTables.airflow_priority, exportTables.export_type, exportTables.sqoop_last_mappers])
		exportTablesQuery = exportTablesQuery.filter(exportTables.include_in_airflow == 1)

		filterDBAlias = DAG['filter_dbalias'].strip().replace(r'*', '%')
		filterTargetSchema = DAG['filter_target_schema'].strip().replace(r'*', '%')
		filterTargetTable = DAG['filter_target_table'].strip().replace(r'*', '%')

		if filterDBAlias == '':
			logging.error("'filter_dbalias' in airflow_export_dags cant be empty")
			self.DAGfile.close()
			self.common_config.remove_temporary_files()
			sys.exit(1)

		exportTablesQuery = exportTablesQuery.filter(exportTables.dbalias.like(filterDBAlias))
		if filterTargetSchema != '': exportTablesQuery = exportTablesQuery.filter(exportTables.target_schema.like(filterTargetSchema))
		if filterTargetTable  != '': exportTablesQuery = exportTablesQuery.filter(exportTables.target_table.like(filterTargetTable))
		tables = pd.DataFrame(exportTablesQuery.with_session(session).all()).fillna('')

		if DAG['retries'] == None or DAG['retries'] == '':
			retries = 5
		else:
			retries = int(DAG['retries'])

		# in 'tables' we now have all the tables that will be part of the DAG
		previousConnectionAlias = ""

		for index, row in tables.iterrows():
			if row['dbalias'] != previousConnectionAlias:
				# We save the previousConnectionAlias just to avoid making lookups for every dbalias even if they are all the same
				try:
					self.common_config.lookupConnectionAlias(connection_alias=row['dbalias'], decryptCredentials=False)
					previousConnectionAlias = row['dbalias']

				except invalidConfiguration as errMsg:
					logging.warning("The connection alias '%s' cant be found in the configuration database"%(row['dbalias']))
					previousConnectionAlias = None
					continue
		
#			exportPool = "DBImport_server_%s"%(self.common_config.jdbc_hostname.lower())
			exportPool = self.getAirflowHostPoolName()

			# usedPools is later used to check if the pools that we just are available in Airflow
			if exportPool not in usedPools:
				usedPools.append(exportPool)
	
			taskID = row['target_table'].replace(r'/', '_').replace(r'.', '_')
			dbexportCMD = "%sbin/export"%(self.getDBImportCommandPath(sudoUser = sudoUser)) 
			dbexportClearStageCMD = "%sbin/manage --clearExportStage"%(self.getDBImportCommandPath(sudoUser = sudoUser)) 

			airflowPriority = 1		# Default Airflow Priority
			if row['airflow_priority'] != None and row['airflow_priority'] != '':
				airflowPriority = int(row['airflow_priority'])
			elif row['sqoop_last_mappers'] != None and row['sqoop_last_mappers'] != '':
				airflowPriority = int(row['sqoop_last_mappers'])

			clearStageRequired = False
			if row['export_type'] == "full":
				clearStageRequired = True

			if clearStageRequired == True:
				self.DAGfile.write("%s_clearStage = BashOperator(\n"%(taskID))
				self.DAGfile.write("    task_id='%s_clearStage',\n"%(taskID))
				self.DAGfile.write("    bash_command='%s -a %s -S %s -T %s ',\n"%(dbexportClearStageCMD, row['dbalias'], row['target_schema'], row['target_table']))
				self.DAGfile.write("    pool='%s',\n"%(exportPool))
				self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
				self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    retries=%s,\n"%(retries))
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")

			self.DAGfile.write("%s = BashOperator(\n"%(taskID))
			self.DAGfile.write("    task_id='%s',\n"%(taskID))
			self.DAGfile.write("    bash_command='%s -a %s -S %s -T %s ',\n"%(dbexportCMD, row['dbalias'], row['target_schema'], row['target_table']))
			self.DAGfile.write("    pool='%s',\n"%(exportPool))
			self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    retries=%s,\n"%(retries))
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")

			if clearStageRequired == True:
				self.DAGfile.write("%s.set_downstream(%s_clearStage)\n"%(self.mainStartTask, taskID))
				self.DAGfile.write("%s_clearStage.set_downstream(%s)\n"%(taskID, taskID))
				self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
			else:
				self.DAGfile.write("%s.set_downstream(%s)\n"%(self.mainStartTask, taskID))
				self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
			self.DAGfile.write("\n")

		self.addTasksToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"], defaultRetries=retries, defaultSudoUser=sudoUser)
		self.addSensorsToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"])
		self.createAirflowPools(pools=usedPools)
		self.closeDAGfile()
		session.close()


	def generateImportDAG(self, DAG):
		""" Generates a Import DAG """

		importPhaseFinishFirst = False
		if DAG['finish_all_stage1_first'] == 1:
			importPhaseFinishFirst = True

		runImportAndEtlSeparate = False
		if DAG['run_import_and_etl_separate'] == 1:
			runImportAndEtlSeparate = True

		usedPools = []
		tableFilters = []
		defaultPool = DAG['dag_name']
		sudoUser = DAG['sudo_user']
		usedPools.append(defaultPool)

		cronSchedule = self.convertTimeToCron(DAG["schedule_interval"])
		self.createDAGfileWithHeader(dagName = DAG['dag_name'], cronSchedule = cronSchedule, importPhaseFinishFirst = importPhaseFinishFirst, defaultPool = defaultPool, sudoUser = sudoUser)

		session = self.configDBSession()
		importTables = aliased(configSchema.importTables)

		importTablesQuery = Query([importTables.hive_db, importTables.hive_table, importTables.dbalias, importTables.airflow_priority, importTables.import_type, importTables.import_phase_type, importTables.etl_phase_type, importTables.sqoop_last_mappers, importTables.copy_slave])
		importTablesQuery = importTablesQuery.filter(importTables.include_in_airflow == 1)

		for hiveTarget in DAG['filter_hive'].split(';'):
			hiveDB = hiveTarget.split(".")[0].strip().replace(r'*', '%')
			hiveTable = hiveTarget.split(".")[1].strip().replace(r'*', '%')
			if hiveDB == None or hiveTable == None or hiveDB == "" or hiveTable == "":
				logging.error("Syntax for filter_hive column is <HIVE_DB>.<HIVE_TABLE>;<HIVE_DB>.<HIVE_TABLE>;.....")
				self.DAGfile.close()
				self.common_config.remove_temporary_files()
				sys.exit(1)

			tableFilters.append((importTables.hive_db.like(hiveDB)) & (importTables.hive_table.like(hiveTable)))

		importTablesQuery = importTablesQuery.filter(sa.or_(*tableFilters))
		tables = pd.DataFrame(importTablesQuery.with_session(session).all()).fillna('')

		retries=int(DAG['retries'])
		try:
			retriesImportPhase = int(DAG['retries_stage1'])
		except ValueError:
			retriesImportPhase = retries

		try:
			retriesEtlPhase = int(DAG['retries_stage2'])
		except ValueError:
			retriesEtlPhase = retries

		# in 'tables' we now have all the tables that will be part of the DAG
		previousConnectionAlias = ""

		for index, row in tables.iterrows():
			try:
				if row['dbalias'] != previousConnectionAlias:
					# We save the previousConnectionAlias just to avoid making lookups for every dbalias even if they are all the same
					self.common_config.lookupConnectionAlias(connection_alias=row['dbalias'], decryptCredentials=False)
					previousConnectionAlias = row['dbalias']
			except:
				continue
		
#			importPhasePool = "DBImport_server_%s"%(self.common_config.jdbc_hostname.lower())
			importPhasePool = self.getAirflowHostPoolName()
			etlPhasePool = DAG['dag_name'][0:50]

			if row['copy_slave'] == 1:
				importPhaseAsSensor = True
			else:
				importPhaseAsSensor = False

#			if row['hive_db'] == "user_boszkk" and row['hive_table'] == "tbl_full_mysql":
#				importPhaseAsSensor = True


			if DAG['pool_stage1'] != '':
				importPhasePool = DAG['pool_stage1']

			if DAG['pool_stage2'] != '':
				etlPhasePool = DAG['pool_stage2']
		
			# usedPools is later used to check if the pools that we just are available in Airflow
			if importPhasePool not in usedPools:
				usedPools.append(importPhasePool)

			if etlPhasePool not in usedPools:
				usedPools.append(etlPhasePool)

			dbimportCMD = "%sbin/import"%(self.getDBImportCommandPath(sudoUser = sudoUser)) 
			dbimportClearStageCMD = "%sbin/manage --clearImportStage"%(self.getDBImportCommandPath(sudoUser = sudoUser)) 

			taskID = row['hive_table'].replace(r'/', '_').replace(r'.', '_')

			airflowPriority = 1		# Default Airflow Priority
			if row['airflow_priority'] != None and row['airflow_priority'] != '':
				airflowPriority = int(row['airflow_priority'])
			elif row['sqoop_last_mappers'] != None and row['sqoop_last_mappers'] != '':
				airflowPriority = int(row['sqoop_last_mappers'])

			clearStageRequired = False
			if row['import_type'] in ("full_direct", "full", "oracle_flashback_merge", "full_history", "full_merge_direct_history", "full_merge_direct", "full_append"):
				clearStageRequired = True

			if row['import_phase_type'] == "full":
				clearStageRequired = True

			if row['import_phase_type'] == "oracle_flashback":
				clearStageRequired = True

			if clearStageRequired == True:
				self.DAGfile.write("%s_clearStage = BashOperator(\n"%(taskID))
				self.DAGfile.write("    task_id='%s_clearStage',\n"%(taskID))
				self.DAGfile.write("    bash_command='%s -h %s -t %s ',\n"%(dbimportClearStageCMD, row['hive_db'], row['hive_table']))
				self.DAGfile.write("    pool='%s',\n"%(importPhasePool))
				self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
				self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    retries=%s,\n"%(retries))
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")

			if DAG['finish_all_stage1_first'] == 1 or runImportAndEtlSeparate == True:
				if importPhaseAsSensor == True:
					# Running Import phase as a sensor
					self.DAGfile.write("%s_sensor = SqlSensor(\n"%(taskID))
					self.DAGfile.write("    task_id='%s_sensor',\n"%(taskID))
					self.DAGfile.write("    conn_id='DBImport',\n")
					self.DAGfile.write("    sql=\"\"\"select count(*) from import_tables where hive_db = '%s' and hive_table = '%s' and "%(row['hive_db'], row['hive_table']))
					self.DAGfile.write("copy_finished >= '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S.%f') }}'\"\"\",\n")
					self.DAGfile.write("    pool='%s',\n"%(importPhasePool))
#					if DAG['finish_all_stage1_first'] == 1:
#						# If all stage1 is to be completed first, then we need to have prio on the stage1 task aswell as 
#						# the prio from stage 2 will all be summed up in 'stage1_complete' dummy task
#						self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
#					else:
#						self.DAGfile.write("    priority_weight=0,\n")
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    timeout=18000,\n")
					self.DAGfile.write("    poke_interval=300,\n")
					self.DAGfile.write("    mode='reschedule',\n")
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

				self.DAGfile.write("%s_import = BashOperator(\n"%(taskID))
				self.DAGfile.write("    task_id='%s_import',\n"%(taskID))
				self.DAGfile.write("    bash_command='%s -h %s -t %s -I -C ',\n"%(dbimportCMD, row['hive_db'], row['hive_table']))
				self.DAGfile.write("    pool='%s',\n"%(importPhasePool))
#				if DAG['finish_all_stage1_first'] == 1:
#					# If all stage1 is to be completed first, then we need to have prio on the stage1 task aswell as 
#					# the prio from stage 2 will all be summed up in 'stage1_complete' dummy task
#					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
#				else:
#					self.DAGfile.write("    priority_weight=0,\n")
				self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
				self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    retries=%s,\n"%(retriesImportPhase))
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")
			
				self.DAGfile.write("%s_etl = BashOperator(\n"%(taskID))
				self.DAGfile.write("    task_id='%s_etl',\n"%(taskID))
				self.DAGfile.write("    bash_command='%s -h %s -t %s -E ',\n"%(dbimportCMD, row['hive_db'], row['hive_table']))
				self.DAGfile.write("    pool='%s',\n"%(etlPhasePool))
				self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
				self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    retries=%s,\n"%(retriesEtlPhase))
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")

				if clearStageRequired == True and DAG['finish_all_stage1_first'] == 1:
					self.DAGfile.write("%s.set_downstream(%s_clearStage)\n"%(self.mainStartTask, taskID))
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s_clearStage.set_downstream(%s_sensor)\n"%(taskID, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s_import)\n"%(taskID, taskID))
					else:
						self.DAGfile.write("%s_clearStage.set_downstream(%s_import)\n"%(taskID, taskID))
					self.DAGfile.write("%s_import.set_downstream(Import_Phase_Finished)\n"%(taskID))
					self.DAGfile.write("Import_Phase_Finished.set_downstream(%s_etl)\n"%(taskID))
					self.DAGfile.write("%s_etl.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				elif clearStageRequired == True and DAG['finish_all_stage1_first'] == 0:		# This means that runImportAndEtlSeparate == True
					self.DAGfile.write("%s.set_downstream(%s_clearStage)\n"%(self.mainStartTask, taskID))
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s_clearStage.set_downstream(%s_sensor)\n"%(taskID, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s_import)\n"%(taskID, taskID))
					else:
						self.DAGfile.write("%s_clearStage.set_downstream(%s_import)\n"%(taskID, taskID))
					self.DAGfile.write("%s_import.set_downstream(%s_etl)\n"%(taskID, taskID))
					self.DAGfile.write("%s_etl.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				elif clearStageRequired == False and DAG['finish_all_stage1_first'] == 1:
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s.set_downstream(%s_sensor)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s_import)\n"%(taskID, taskID))
					else:
						self.DAGfile.write("%s.set_downstream(%s_import)\n"%(self.mainStartTask, taskID))
					self.DAGfile.write("%s_import.set_downstream(Import_Phase_Finished)\n"%(taskID))
					self.DAGfile.write("Import_Phase_Finished.set_downstream(%s_etl)\n"%(taskID))
					self.DAGfile.write("%s_etl.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				else:
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s.set_downstream(%s_sensor)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s_import)\n"%(taskID, taskID))
					else:
						self.DAGfile.write("%s.set_downstream(%s_import)\n"%(self.mainStartTask, taskID))
					self.DAGfile.write("%s_import.set_downstream(%s_etl)\n"%(taskID, taskID))
					self.DAGfile.write("%s_etl.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				self.DAGfile.write("\n")

			else:
				if importPhaseAsSensor == True:
					# Running Import phase as a sensor
					self.DAGfile.write("%s_sensor = SqlSensor(\n"%(taskID))
					self.DAGfile.write("    task_id='%s_sensor',\n"%(taskID))
					self.DAGfile.write("    conn_id='DBImport',\n")
					self.DAGfile.write("    sql=\"\"\"select count(*) from import_tables where hive_db = '%s' and hive_table = '%s' and "%(row['hive_db'], row['hive_table']))
					self.DAGfile.write("copy_finished >= '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S.%f') }}'\"\"\",\n")
#					self.DAGfile.write("copy_finished >= '{{ dag_run.start_date.strftime('%Y-%m-%d %H:%M:%S.%f') }}'\"\"\",\n")
					self.DAGfile.write("    pool='%s',\n"%(importPhasePool))
#					if DAG['finish_all_stage1_first'] == 1:
#						# If all stage1 is to be completed first, then we need to have prio on the stage1 task aswell as 
#						# the prio from stage 2 will all be summed up in 'stage1_complete' dummy task
#						self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
#					else:
#						self.DAGfile.write("    priority_weight=0,\n")
					self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
					self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    timeout=18000,\n")
					self.DAGfile.write("    poke_interval=300,\n")
					self.DAGfile.write("    mode='reschedule',\n")
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

				self.DAGfile.write("%s = BashOperator(\n"%(taskID))
				self.DAGfile.write("    task_id='%s',\n"%(taskID))
				self.DAGfile.write("    bash_command='%s -h %s -t %s ',\n"%(dbimportCMD, row['hive_db'], row['hive_table']))
				self.DAGfile.write("    pool='%s',\n"%(etlPhasePool))
				self.DAGfile.write("    priority_weight=%s,\n"%(airflowPriority))
				self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    retries=%s,\n"%(retries))
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")

				if clearStageRequired == True:
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s.set_downstream(%s_clearStage)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s_clearStage.set_downstream(%s_sensor)\n"%(taskID, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s)\n"%(taskID, taskID))
						self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
					else:
						self.DAGfile.write("%s.set_downstream(%s_clearStage)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s_clearStage.set_downstream(%s)\n"%(taskID, taskID))
						self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				else:
					if importPhaseAsSensor == True:
						self.DAGfile.write("%s.set_downstream(%s_sensor)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s_sensor.set_downstream(%s)\n"%(taskID, taskID))
						self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
					else:
						self.DAGfile.write("%s.set_downstream(%s)\n"%(self.mainStartTask, taskID))
						self.DAGfile.write("%s.set_downstream(%s)\n"%(taskID, self.mainStopTask))
				self.DAGfile.write("\n")

		self.addTasksToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"], defaultRetries=retries, defaultSudoUser=sudoUser)
		self.addSensorsToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"])
		self.createAirflowPools(pools=usedPools)
		self.closeDAGfile()
		session.close()

	def createAirflowPools(self, pools): 
		""" Creates the pools in Airflow database """
		session = self.airflowDBSession()
		slotPool = aliased(airflowSchema.slotPool)
		
		airflowPools = pd.DataFrame(session.query(
					slotPool.pool,
					slotPool.slots,
					slotPool.description)
				.select_from(slotPool)
				.all())

		for pool in pools:
			if len(airflowPools) == 0 or len(airflowPools.loc[airflowPools['pool'] == pool]) == 0:
				logging.info("Creating the Airflow pool '%s' with 24 slots"%(pool))
				newPool = airflowSchema.slotPool(pool=pool, slots=24)
				session.add(newPool)
				session.commit()

		session.close()


	def generateCustomDAG(self, DAG):
		""" Generates a Custom DAG """

		session = self.configDBSession()
		airflowTasks = aliased(configSchema.airflowTasks)

		tasks = (session.query(airflowTasks.task_name)
				.select_from(airflowTasks)
				.filter(airflowTasks.dag_name == DAG['dag_name'])
				.filter(airflowTasks.include_in_airflow == 1)
				.filter(airflowTasks.placement == 'in main')
				.count())

		if tasks == 0:
			logging.warning("There are no tasks defined 'in main' for DAG '%s'. This is required for a custom DAG"%(DAG["dag_name"]))
			session.close()
			return

		usedPools = []
		defaultPool = DAG['dag_name']
		sudoUser = DAG['sudo_user']
		usedPools.append(defaultPool)

		if DAG['retries'] == None or DAG['retries'] == '':
			retries = 5
		else:
			retries = int(DAG['retries'])

		cronSchedule = self.convertTimeToCron(DAG["schedule_interval"])
		self.createDAGfileWithHeader(dagName = DAG['dag_name'], cronSchedule = cronSchedule, defaultPool = defaultPool, sudoUser = sudoUser)
		self.addTasksToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"], defaultRetries=retries, defaultSudoUser=sudoUser)
		self.addSensorsToDAGfile(dagName = DAG['dag_name'], mainDagSchedule=DAG["schedule_interval"])
		self.createAirflowPools(pools=usedPools)
		self.closeDAGfile()
		session.close()

	def convertTimeToCron(self, time):
		""" Converts a string in format HH:MM to a CRON time string based on 'minute hour day month weekday' """
		returnValue = time

		if re.search('^[0-2][0-9]:[0-5][0-9]$', time):
			hour = re.sub(':[0-5][0-9]$', '', time) 
			minute = re.sub('^[0-2][0-9]:', '', time) 
			returnValue = "'%s %s * * *'"%(int(minute), int(hour)) 

		return returnValue 


	def createDAGfileWithHeader(self, dagName, cronSchedule, defaultPool, importPhaseFinishFirst=False, sudoUser=""):
		session = self.configDBSession()

		self.sensorStartTask = "start"
		self.sensorStopTask = "dag_sensors_finished"
		self.preStartTask = "start"
		self.preStopTask = "before_tasks_finished"
		self.mainStartTask = "start"
		self.mainStopTask = "stop"
		self.postStartTask = "main_tasks_finished"
		self.postStopTask = "stop"

		if self.TaskQueueForDummy != None and self.TaskQueueForDummy.strip() != "" and self.TaskQueueForDummy.strip() != "default":
			self.TaskQueueForDummy = self.TaskQueueForDummy.strip()
		else:
			self.TaskQueueForDummy = None

		tasksBeforeMainExists = False
		tasksAfterMainExists = False
		tasksSensorsExists = False

		self.DAGfilename = "%s/%s.py"%(self.DAGstagingDirectory, dagName)
		self.DAGfilenameInAirflow = "%s/%s.py"%(self.DAGdirectory, dagName)

		self.DAGfile = open(self.DAGfilename, "w")
		self.DAGfile.write("# -*- coding: utf-8 -*-\n")
		self.DAGfile.write("import airflow\n")
		self.DAGfile.write("from airflow import DAG\n")
		self.DAGfile.write("from airflow.models import Variable\n")
		self.DAGfile.write("from airflow.operators.bash_operator import BashOperator\n")
		self.DAGfile.write("from airflow.operators.python_operator import BranchPythonOperator\n")
		self.DAGfile.write("from airflow.operators.dagrun_operator import TriggerDagRunOperator\n")
		self.DAGfile.write("from airflow.operators.dummy_operator import DummyOperator\n")
		self.DAGfile.write("from airflow.operators.sensors import ExternalTaskSensor\n")
		self.DAGfile.write("from airflow.sensors.sql_sensor import SqlSensor\n")
		self.DAGfile.write("from datetime import datetime, timedelta, timezone\n")
		self.DAGfile.write("import pendulum\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("Email_receiver = Variable.get(\"Email_receiver\")\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("local_tz = pendulum.timezone(\"%s\")\n"%(self.timeZone))
		self.DAGfile.write("\n")
		self.DAGfile.write("default_args = {\n")
		self.DAGfile.write("    'owner': 'airflow',\n")
		self.DAGfile.write("    'depends_on_past': False,\n")
		self.DAGfile.write("    'start_date': datetime(2017, 1, 1, 0, 0, tzinfo=local_tz),\n")
		self.DAGfile.write("    'max_active_runs': 1,\n")
		self.DAGfile.write("    'email': Email_receiver,\n")
		self.DAGfile.write("    'email_on_failure': False,\n")
		self.DAGfile.write("    'email_on_retry': False,\n")
		self.DAGfile.write("    'retries': 0,\n")
		self.DAGfile.write("    'pool': '%s',\n"%(defaultPool))
		self.DAGfile.write("    'retry_delay': timedelta(minutes=5),\n")
		self.DAGfile.write("}\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("dag = DAG(\n")
		self.DAGfile.write("    '%s',\n"%(dagName))
		self.DAGfile.write("    default_args=default_args,\n")
		self.DAGfile.write("    description='%s',\n"%(dagName))
		self.DAGfile.write("    catchup=False,\n")
		self.DAGfile.write("    schedule_interval=%s)\n"%(cronSchedule))
		self.DAGfile.write("\n")
		self.DAGfile.write("start = BashOperator(\n")
		self.DAGfile.write("    task_id='start',\n")
		if self.TaskQueueForDummy != None:
			self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
		self.DAGfile.write("    bash_command='%sbin/manage --checkAirflowExecution ',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser)))
		self.DAGfile.write("    priority_weight=100,\n")
		self.DAGfile.write("    weight_rule='absolute',\n")
		self.DAGfile.write("    dag=dag)\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("stop = DummyOperator(\n")
		self.DAGfile.write("    task_id='stop',\n")
		if self.TaskQueueForDummy != None:
			self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
		self.DAGfile.write("    priority_weight=100,\n")
		self.DAGfile.write("    weight_rule='absolute',\n")
		self.DAGfile.write("    dag=dag)\n")
		self.DAGfile.write("\n")
		self.DAGfile.write("def always_trigger(context, dag_run_obj):\n")
		self.DAGfile.write("    return dag_run_obj\n")
		self.DAGfile.write("\n")

		if importPhaseFinishFirst == True:
			self.DAGfile.write("Import_Phase_Finished = DummyOperator(\n")
			self.DAGfile.write("    task_id='Import_Phase_Finished',\n")
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")

		airflowDAGsensors = aliased(configSchema.airflowDagSensors, name="ads")
		sensors = (session.query(
					airflowDAGsensors.dag_name 
					)
				.select_from(airflowDAGsensors)
				.filter(airflowDAGsensors.dag_name == dagName)
				.count())

		if sensors > 0: tasksSensorsExists = True

		airflowTasks = aliased(configSchema.airflowTasks)
		tasks = pd.DataFrame(session.query(
					airflowTasks.dag_name,
					airflowTasks.placement)
				.select_from(airflowTasks)
				.filter(airflowTasks.dag_name == dagName)
				.all())

		if len(tasks) == 0:
			tasks = pd.DataFrame(columns=['dag_name', 'placement'])

		if len(tasks.loc[tasks['placement'] == 'before main']) > 0:
			tasksBeforeMainExists = True

		if len(tasks.loc[tasks['placement'] == 'after main']) > 0:
			tasksAfterMainExists = True

		if tasksBeforeMainExists == True:
			self.DAGfile.write("%s = DummyOperator(\n"%(self.preStopTask))
			self.DAGfile.write("    task_id='%s',\n"%(self.preStopTask))
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")
			
		if tasksAfterMainExists == True:
			self.DAGfile.write("%s = DummyOperator(\n"%(self.postStartTask))
			self.DAGfile.write("    task_id='%s',\n"%(self.postStartTask))
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")
			
		if tasksSensorsExists == True:
			self.DAGfile.write("%s = DummyOperator(\n"%(self.sensorStopTask))
			self.DAGfile.write("    task_id='%s',\n"%(self.sensorStopTask))
			if self.TaskQueueForDummy != None:
				self.DAGfile.write("    queue='%s',\n"%(self.TaskQueueForDummy.strip()))
			self.DAGfile.write("    priority_weight=100,\n")
			self.DAGfile.write("    weight_rule='absolute',\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")
			
		if tasksSensorsExists == False and tasksBeforeMainExists == True:
			self.mainStartTask = self.preStopTask

		if tasksSensorsExists == True and tasksBeforeMainExists == False:
			self.mainStartTask = self.sensorStopTask

		if tasksSensorsExists == True and tasksBeforeMainExists == True:
			self.preStartTask = self.sensorStopTask
			self.mainStartTask = self.preStopTask

		if tasksAfterMainExists == True:
			self.mainStopTask = self.postStartTask

		session.close()

		logging.debug("sensorStartTask = %s"%(self.sensorStartTask))
		logging.debug("sensorStopTask = %s"%(self.sensorStopTask))
		logging.debug("preStartTask = %s"%(self.preStartTask))
		logging.debug("preStopTask = %s"%(self.preStopTask))
		logging.debug("mainStartTask = %s"%(self.mainStartTask))
		logging.debug("mainStopTask = %s"%(self.mainStopTask))
		logging.debug("postStartTask = %s"%(self.postStartTask))
		logging.debug("postStopTask = %s"%(self.postStopTask))

	def closeDAGfile(self):
		self.DAGfile.close()

		try:
			os.chmod(self.DAGfilename, int(self.DAGfilePermission, 8))	
		except PermissionError:
			logging.warning("Could not change file mode to '%s'"%(self.DAGfilePermission))

		try:
			shutil.chown(self.DAGfilename, group=self.DAGfileGroup)
		except PermissionError:
			logging.warning("Could not change group owner of file to '%s'"%(self.DAGfileGroup))


		if self.writeDAG == True:
			shutil.copy(self.DAGfilename, self.DAGfilenameInAirflow)
			os.chmod(self.DAGfilenameInAirflow, int(self.DAGfilePermission, 8))	
			shutil.chown(self.DAGfilenameInAirflow, group=self.DAGfileGroup)
			print("DAG file written to %s"%(self.DAGfilenameInAirflow))
		else:
			print("DAG file written to %s"%(self.DAGfilename))


	def addFailureTask(self, taskName):
		self.DAGfile.write("%s = BashOperator(\n"%(taskName))
		self.DAGfile.write("    task_id='%s',\n"%(taskName))
		self.DAGfile.write("    bash_command='exit 1 ',\n")
		self.DAGfile.write("    priority_weight=1,\n")
		self.DAGfile.write("    weight_rule='absolute',\n")
		self.DAGfile.write("    dag=dag)\n")
		self.DAGfile.write("\n")

	def addTasksToDAGfile(self, dagName, mainDagSchedule, defaultRetries=0, defaultSudoUser=""):

		session = self.configDBSession()

		if defaultSudoUser == None: defaultSudoUser = ""

		airflowTasks = aliased(configSchema.airflowTasks)

		tasks = pd.DataFrame(session.query(
					airflowTasks.task_name,
					airflowTasks.task_type,
					airflowTasks.placement,
					airflowTasks.airflow_pool,
					airflowTasks.airflow_priority,
					airflowTasks.task_dependency_in_main,
					airflowTasks.task_config,
					airflowTasks.jdbc_dbalias,
					airflowTasks.hive_db,
					airflowTasks.sensor_poke_interval,
					airflowTasks.sensor_timeout_minutes,
					airflowTasks.sensor_connection,
					airflowTasks.sudo_user
					)
				.select_from(airflowTasks)
				.filter(airflowTasks.dag_name == dagName)
				.filter(airflowTasks.include_in_airflow == 1)
				.all()).fillna('')

		taskDependencies = ""
		allTaskDependencies = tasks.filter(['task_dependency_in_main'])

		for index, row in tasks.iterrows():
			sensorPokeInterval = row['sensor_poke_interval']
			taskName = row['task_name']

			sudoUser = row['sudo_user']
			if sudoUser == None or sudoUser == "":
				sudoUser = defaultSudoUser

			if sensorPokeInterval == '':
				# Default to 5 minutes
				sensorPokeInterval = 300

			sensorTimeoutSeconds = row['sensor_timeout_minutes']
			if sensorTimeoutSeconds == '':
				# Default to 5 hours
				sensorTimeoutSeconds = "18000"	
			else:
				sensorTimeoutSeconds = sensorTimeoutSeconds * 60

			if row['task_type'] == "DAG Sensor":
				addDagSensor = True
				airflowCustomDags = aliased(configSchema.airflowCustomDags)
				airflowEtlDags    = aliased(configSchema.airflowEtlDags)
				airflowExportDags = aliased(configSchema.airflowExportDags)
				airflowImportDags = aliased(configSchema.airflowImportDags)

				if "." in row['task_config']:
					waitForDag = row['task_config'].split(".")[0]
					waitForTask = row['task_config'].split(".")[1]
				else:
					waitForDag = row['task_config'].split(".")[0]
					waitForTask = "stop"
				
				importSensorSchedule = session.query(airflowImportDags.schedule_interval).filter(airflowImportDags.dag_name == waitForDag).one_or_none()		
				exportSensorSchedule = session.query(airflowExportDags.schedule_interval).filter(airflowExportDags.dag_name == waitForDag).one_or_none()		
				customSensorSchedule = session.query(airflowCustomDags.schedule_interval).filter(airflowCustomDags.dag_name == waitForDag).one_or_none()		
				etlSensorSchedule    = session.query(airflowEtlDags.schedule_interval).filter(airflowEtlDags.dag_name == waitForDag).one_or_none()		

				waitDagSchedule = ''
				if importSensorSchedule != None:
					waitDagSchedule = importSensorSchedule[0] 
				elif exportSensorSchedule != None:
					waitDagSchedule = exportSensorSchedule[0]
				elif customSensorSchedule != None:
					waitDagSchedule = customSensorSchedule[0]
				elif etlSensorSchedule != None:
					waitDagSchedule = etlSensorSchedule[0]

				if waitDagSchedule == '':
					logging.warning("Issue on DAG '%s'"%(dagName))
					logging.warning("There is no schedule interval for DAG to wait for")
					logging.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")

					row['task_name'] = "%s_FAILURE"%(row['task_name'])
					self.addFailureTask(row['task_name'])
					addDagSensor = False
#					continue
#					logging.error("Cant find schedule interval for DAG to wait for")
#					self.DAGfile.close()
#					self.common_config.remove_temporary_files()
#					sys.exit(1)

				mainDagMatchHourMin = re.search('^[0-2][0-9]:[0-5][0-9]$', mainDagSchedule)
				waitDagMatchHourMin = re.search('^[0-2][0-9]:[0-5][0-9]$', waitDagSchedule)

				timeDiff = "0"
	
				if ( mainDagMatchHourMin == None and waitDagMatchHourMin != None ) or (mainDagMatchHourMin != None and waitDagMatchHourMin == None):
					logging.warning("Issue on DAG '%s'"%(dagName))
					logging.warning("Both the current DAG and the DAG the sensor is waiting for must have the same scheduling format (HH:MM or cron)")
					logging.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")
					row['task_name'] = "%s_FAILURE"%(row['task_name'])
					self.addFailureTask(row['task_name'])
					addDagSensor = False

				if mainDagMatchHourMin != None and addDagSensor == True:
					# Time is in HH:MM format for both DAG's. So now we can calculate the diff in time between them
					noon = datetime.strptime('12:00', '%H:%M')
					mainDagScheduleDateTime = datetime.strptime(mainDagSchedule, '%H:%M')
					waitDagScheduleDateTime = datetime.strptime(waitDagSchedule, '%H:%M')

					# Add or remove half a day so we dont calculate over midnight.
					if mainDagScheduleDateTime > noon:
						mainDagScheduleDateTime = mainDagScheduleDateTime - timedelta(seconds=43200)
					else:
						mainDagScheduleDateTime = mainDagScheduleDateTime + timedelta(seconds=43200)

					if waitDagScheduleDateTime > noon:
						waitDagScheduleDateTime = waitDagScheduleDateTime - timedelta(seconds=43200)
					else:
						waitDagScheduleDateTime = waitDagScheduleDateTime + timedelta(seconds=43200)

					if mainDagScheduleDateTime > waitDagScheduleDateTime:
						timeDiff = mainDagScheduleDateTime - waitDagScheduleDateTime
						minusText = ''
					else:
						timeDiff = waitDagScheduleDateTime - mainDagScheduleDateTime
						minusText = '-'

					timeDiff = str(minusText + str(timeDiff.seconds))
				elif addDagSensor == True:
					if mainDagSchedule != waitDagSchedule:
						logging.warning("Issue on DAG '%s'"%(dagName))
						logging.warning("When using cron or cron alias schedules for DAG sensors, the schedule time in both DAG's must match")
						logging.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")
						row['task_name'] = "%s_FAILURE"%(row['task_name'])
						self.addFailureTask(row['task_name'])
						addDagSensor = False

				if addDagSensor == True:
					self.DAGfile.write("%s = ExternalTaskSensor(\n"%(row['task_name']))
					self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
					self.DAGfile.write("    external_dag_id='%s',\n"%(waitForDag))
					self.DAGfile.write("    external_task_id='%s',\n"%(waitForTask))
					self.DAGfile.write("    retries=0,\n")
					self.DAGfile.write("    execution_delta=timedelta(seconds=%s),\n"%(timeDiff))
					if row['airflow_pool'] != '':
						self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
					if row['airflow_priority'] != '':
						self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
						self.DAGfile.write("    weight_rule='absolute',\n")
					else:
						self.DAGfile.write("    priority_weight=100,\n")
						self.DAGfile.write("    weight_rule='absolute',\n")
					self.DAGfile.write("    timeout=%s,\n"%(int(sensorTimeoutSeconds)))
					self.DAGfile.write("    poke_interval=%s,\n"%(int(sensorPokeInterval)))
					self.DAGfile.write("    mode='reschedule',\n")
					self.DAGfile.write("    dag=dag)\n")
					self.DAGfile.write("\n")

			if row['task_type'] == "SQL Sensor":
				if row['sensor_connection'] == '':
					logging.error("SQL Sensors requires a valid Airflow Connection ID in column 'sensor_connection'")
					self.DAGfile.close()
					self.common_config.remove_temporary_files()
					sys.exit(1)

				sensorPokeInterval = row['sensor_poke_interval']
				if sensorPokeInterval == '':
					# Default to 5 minutes
					sensorPokeInterval = 300

				self.DAGfile.write("%s = SqlSensor(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    conn_id='%s',\n"%(row['sensor_connection']))
				self.DAGfile.write("    sql=\"\"\"%s\"\"\",\n"%(row['task_config']))
				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=100,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    timeout=%s,\n"%(int(sensorTimeoutSeconds)))
				self.DAGfile.write("    poke_interval=%s,\n"%(int(sensorPokeInterval)))
				self.DAGfile.write("    mode='reschedule',\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")

			if row['task_type'] == "Trigger DAG":
				self.DAGfile.write("%s = TriggerDagRunOperator(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    trigger_dag_id='%s',\n"%(row['task_config']))
				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=100,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    python_callable=always_trigger,\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")
				
			if row['task_type'] == "shell script":
				self.DAGfile.write("%s = BashOperator(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    retries=%s,\n"%(defaultRetries))
				self.DAGfile.write("    bash_command='%s ',\n"%(row['task_config']))
				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=1,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")
				
			if row['task_type'] == "Hive SQL Script":
				self.DAGfile.write("%s = BashOperator(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    retries=%s,\n"%(defaultRetries))

				if row['hive_db'] != None and row['hive_db'].strip() != '':
					self.DAGfile.write("    bash_command='%sbin/manage --runHiveScript=%s --hiveDB=%s ',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), row['task_config'], row['hive_db']))
				else:
					self.DAGfile.write("    bash_command='%sbin/manage --runHiveScript=%s ',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), row['task_config']))

				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=1,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")

			if row['task_type'] == "Hive SQL":
				jdbcSQL = row['task_config'].replace(r"'", "\\'")
				self.DAGfile.write("%s = BashOperator(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    retries=%s,\n"%(defaultRetries))
				self.DAGfile.write("    bash_command='%sbin/manage --runHiveQuery=\"%s\" ',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), jdbcSQL))
				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=1,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")
				
			if row['task_type'] == "JDBC SQL":
				jdbcSQL = row['task_config'].replace(r"'", "\\'")
				self.DAGfile.write("%s = BashOperator(\n"%(row['task_name']))
				self.DAGfile.write("    task_id='%s',\n"%(row['task_name']))
				self.DAGfile.write("    retries=%s,\n"%(defaultRetries))
				self.DAGfile.write("    bash_command='%sbin/manage --dbAlias=%s --runJDBCQuery=\"%s\" ',\n"%(self.getDBImportCommandPath(sudoUser=sudoUser), row['jdbc_dbalias'], jdbcSQL))
				if row['airflow_pool'] != '':
					self.DAGfile.write("    pool='%s',\n"%(row['airflow_pool']))
				if row['airflow_priority'] != '':
					self.DAGfile.write("    priority_weight=%s,\n"%(int(row['airflow_priority'])))
					self.DAGfile.write("    weight_rule='absolute',\n")
				else:
					self.DAGfile.write("    priority_weight=1,\n")
					self.DAGfile.write("    weight_rule='absolute',\n")
				self.DAGfile.write("    dag=dag)\n")
				self.DAGfile.write("\n")

			if row['placement'] == "before main":
				taskDependencies += "%s.set_downstream(%s)\n"%(self.preStartTask, row['task_name'])
				taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], self.preStopTask)
				taskDependencies += "\n"
				
			if row['placement'] == "after main":
				taskDependencies += "%s.set_downstream(%s)\n"%(self.postStartTask, row['task_name'])
				taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], self.postStopTask)
				taskDependencies += "\n"
				
			if row['placement'] == "in main":
				if row['task_dependency_in_main'] == '':
					taskDependencies += "%s.set_downstream(%s)\n"%(self.mainStartTask, row['task_name'])
				else:
					# Check if there is any dependencies for this task
					for dep in row['task_dependency_in_main'].split(','):	
						dep = dep.strip()
						if dep == "main_start":
							dep = self.mainStartTask

						taskDependencies += "%s.set_downstream(%s)\n"%(dep, row['task_name'])

				# We also need to check if there is any dependencies on this task. If there is, we dont add a set_downstream as that will be handled by the other task
				foundDependency = False
				for depIndex, dep in allTaskDependencies.iterrows():
					for depTask in dep['task_dependency_in_main'].split(','):
						depTask = depTask.strip()
						if depTask != '' and depTask == row['task_name']:
							foundDependency = True

				if foundDependency == False:
					taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], self.mainStopTask)

				taskDependencies += "\n"

#				self.DAGfile.close()
#				self.common_config.remove_temporary_files()
#				sys.exit(1)


		self.DAGfile.write(taskDependencies)
		session.close()
			
	def addSensorsToDAGfile(self, dagName, mainDagSchedule):
		session = self.configDBSession()

		airflowDAGsensors = aliased(configSchema.airflowDagSensors, name="ads")
		airflowCustomDags = aliased(configSchema.airflowCustomDags, name="acd")
		airflowEtlDags    = aliased(configSchema.airflowEtlDags, name="aetld")
		airflowExportDags = aliased(configSchema.airflowExportDags, name="aed")
		airflowImportDags = aliased(configSchema.airflowImportDags, name="aid")

		sensors = pd.DataFrame(session.query(
					airflowDAGsensors.dag_name,
					airflowDAGsensors.sensor_name,
					airflowDAGsensors.wait_for_dag,
					airflowDAGsensors.wait_for_task,
					airflowDAGsensors.timeout_minutes,
					airflowImportDags.schedule_interval.label("import_schedule"),
					airflowExportDags.schedule_interval.label("export_schedule"),
					airflowCustomDags.schedule_interval.label("custom_schedule"),
					airflowEtlDags.schedule_interval.label("etl_schedule")
					)
				.select_from(airflowDAGsensors)
				.join(airflowImportDags, airflowDAGsensors.wait_for_dag == airflowImportDags.dag_name, isouter=True)
				.join(airflowExportDags, airflowDAGsensors.wait_for_dag == airflowExportDags.dag_name, isouter=True)
				.join(airflowCustomDags, airflowDAGsensors.wait_for_dag == airflowCustomDags.dag_name, isouter=True)
				.join(airflowEtlDags,    airflowDAGsensors.wait_for_dag == airflowEtlDags.dag_name, isouter=True)
				.filter(airflowDAGsensors.dag_name == dagName)
				.all()).fillna('')

		for index, row in sensors.iterrows():
			waitDagSchedule = ''
			if row["import_schedule"] != '':
				waitDagSchedule = row["import_schedule"]
			elif row["export_schedule"] != '':
				waitDagSchedule = row["export_schedule"]
			elif row["custom_schedule"] != '':
				waitDagSchedule = row["custom_schedule"]
			elif row["etl_schedule"] != '':
				waitDagSchedule = row["etl_schedule"]

			if waitDagSchedule == '':
				logging.warning("Issue on DAG '%s'"%(dagName))
				logging.warning("There is no schedule interval for DAG to wait for")
				logging.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")
				self.addFailureTask("%s_FAILURE"%(row['sensor_name']))
				self.DAGfile.write("%s.set_downstream(%s_FAILURE)\n"%(self.sensorStartTask, row['sensor_name']))
				self.DAGfile.write("%s_FAILURE.set_downstream(%s)\n"%(row['sensor_name'], self.sensorStopTask))
				self.DAGfile.write("\n")
				continue
#				logging.error("Cant find schedule interval for DAG to wait for")
#				self.DAGfile.close()
#				self.common_config.remove_temporary_files()
#				sys.exit(1)

			waitForTask = row['wait_for_task']
			if waitForTask == '':
				waitForTask = "stop"

			timeoutSeconds = row['timeout_minutes']
			if timeoutSeconds == '':
				# Default to 5 hours
				timeoutSeconds = "18000"	
			else:
				timeoutSeconds = timeoutSeconds * 60

			mainDagMatchHourMin = re.search('^[0-2][0-9]:[0-5][0-9]$', mainDagSchedule)
			waitDagMatchHourMin = re.search('^[0-2][0-9]:[0-5][0-9]$', waitDagSchedule)

#			print(mainDagSchedule)
#			print(waitDagSchedule)
#			print(mainDagMatchHourMin)
#			print(waitDagMatchHourMin)

			timeDiff = "0"

			if ( mainDagMatchHourMin == None and waitDagMatchHourMin != None ) or (mainDagMatchHourMin != None and waitDagMatchHourMin == None):
				logging.warning("Issue on DAG '%s'"%(dagName))
				logging.warning("Both the current DAG and the DAG the sensor is waiting for must have the same scheduling format (HH:MM or cron)")
				logging.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")
				self.addFailureTask("%s_FAILURE"%(row['sensor_name']))
				self.DAGfile.write("%s.set_downstream(%s_FAILURE)\n"%(self.sensorStartTask, row['sensor_name']))
				self.DAGfile.write("%s_FAILURE.set_downstream(%s)\n"%(row['sensor_name'], self.sensorStopTask))
				self.DAGfile.write("\n")
				continue
#				self.DAGfile.close()
#				self.common_config.remove_temporary_files()
#				sys.exit(1)

			if mainDagMatchHourMin != None:
				# Time is in HH:MM format for both DAG's. So now we can calculate the diff in time between them
				noon = datetime.strptime('12:00', '%H:%M')
				mainDagScheduleDateTime = datetime.strptime(mainDagSchedule, '%H:%M')
				waitDagScheduleDateTime = datetime.strptime(waitDagSchedule, '%H:%M')

				# Add or remove half a day so we dont calculate over midnight.
				if mainDagScheduleDateTime > noon:
					mainDagScheduleDateTime = mainDagScheduleDateTime - timedelta(seconds=43200)
				else:
					mainDagScheduleDateTime = mainDagScheduleDateTime + timedelta(seconds=43200)

				if waitDagScheduleDateTime > noon:
					waitDagScheduleDateTime = waitDagScheduleDateTime - timedelta(seconds=43200)
				else:
					waitDagScheduleDateTime = waitDagScheduleDateTime + timedelta(seconds=43200)

#				print (mainDagScheduleDateTime)
#				print (waitDagScheduleDateTime)

				if mainDagScheduleDateTime > waitDagScheduleDateTime:
					timeDiff = mainDagScheduleDateTime - waitDagScheduleDateTime
					minusText = ''
				else:
					timeDiff = waitDagScheduleDateTime - mainDagScheduleDateTime
					minusText = '-'

				timeDiff = str(minusText + str(timeDiff.seconds))
#				print (timeDiff)
			else:
				if mainDagSchedule != waitDagSchedule:
					logging.warning("Issue on DAG '%s'"%(dagName))
					logging.warning("When using cron or cron alias schedules for DAG sensors, the schedule time in both DAG's must match")
					logging.warning("The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute")
					self.addFailureTask("%s_FAILURE"%(row['sensor_name']))
					self.DAGfile.write("%s.set_downstream(%s_FAILURE)\n"%(self.sensorStartTask, row['sensor_name']))
					self.DAGfile.write("%s_FAILURE.set_downstream(%s)\n"%(row['sensor_name'], self.sensorStopTask))
					self.DAGfile.write("\n")
					continue
#					self.DAGfile.close()
#					self.common_config.remove_temporary_files()
#					sys.exit(1)

			self.DAGfile.write("%s = ExternalTaskSensor(\n"%(row['sensor_name']))
			self.DAGfile.write("    task_id='%s',\n"%(row['sensor_name']))
			self.DAGfile.write("    external_dag_id='%s',\n"%(row['wait_for_dag']))
			self.DAGfile.write("    external_task_id='%s',\n"%(waitForTask))
			self.DAGfile.write("    retries=0,\n")
#			self.DAGfile.write("    execution_timeout=timedelta(seconds=%s),\n"%(timeoutSeconds))
			self.DAGfile.write("    execution_delta=timedelta(seconds=%s),\n"%(timeDiff))
			self.DAGfile.write("    timeout=%s,\n"%(int(timeoutSeconds)))
			self.DAGfile.write("    poke_interval=300,\n")
			self.DAGfile.write("    mode='reschedule',\n")
			self.DAGfile.write("    dag=dag)\n")
			self.DAGfile.write("\n")
			self.DAGfile.write("%s.set_downstream(%s)\n"%(self.sensorStartTask, row['sensor_name']))
			self.DAGfile.write("%s.set_downstream(%s)\n"%(row['sensor_name'], self.sensorStopTask))
			self.DAGfile.write("\n")

		session.close()
