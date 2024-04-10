import json
from datetime import datetime, timedelta, time
from pydantic import BaseModel
from typing import Union, NewType, Literal
from typing_extensions import Annotated
from enum import Enum

class Token(BaseModel):
	access_token: str
	token_type: str

class TokenData(BaseModel):
	username: Union[str, None] = None

class User(BaseModel):
	username: str
	password: str
	disabled: bool
	fullname: Union[str, None] = None
	department: Union[str, None] = None
	email: Union[str, None] = None

class changeUser(BaseModel):
	disabled: Union[bool, None] = None
	fullname: Union[str, None] = None
	department: Union[str, None] = None
	email: Union[str, None] = None

class changePassword(BaseModel):
	old_password: Union[str, None] = None
	new_password: str

class status(BaseModel):
	status: str
	version: str

class dbs(BaseModel):
	name: str
	tables: int
	lastImport: Union[str, None] = None
	lastSize: Union[int, None] = None
	lastRows: Union[int, None] = None

class connectionsRead(BaseModel):
	name: str
	connectionString: str
	source: Union[str, None] = None
	forceString: Union[bool, None] = None
	maxSessions: Union[int, None] = None
	createDatalakeImport: Union[bool, None] = None
	timeWindowStart: Union[time, None] = None
	timeWindowStop: Union[time, None] = None
	timeWindowTimezone: Union[str, None] = None
	operatorNotes: Union[str, None] = None
	contactInformation: Union[str, None] = None
	description: Union[str, None] = None
	owner: Union[str, None] = None
	environment: Union[str, None] = None
	seedFile: Union[str, None] = None
	createForeignKey: Union[bool, None] = None
	atlasDiscovery: Union[bool, None] = None
	atlasIncludeFilter: Union[str, None] = None
	atlasExcludeFilter: Union[str, None] = None
	atlasLastDiscovery: Union[datetime, None] = None

class jdbcDriver(BaseModel):
	databaseType: str
	version: str
	driver: str
	classpath: str

class configuration(BaseModel):
	airflow_aws_instanceids: Union[str, None] = None
	airflow_aws_pool_to_instanceid: Union[bool, None] = None
	airflow_create_pool_with_task: Union[bool, None] = None
	airflow_dag_directory: Union[str, None] = None
	airflow_dag_file_group: Union[str, None] = None
	airflow_dag_file_permission: Union[str, None] = None
	airflow_dag_staging_directory: Union[str, None] = None
	airflow_dbimport_commandpath: Union[str, None] = None
	airflow_default_pool_size: Union[int, None] = None
	airflow_disable: Union[bool, None] = None
	airflow_dummy_task_queue: Union[str, None] = None
	airflow_major_version: Union[int, None] = None
	airflow_sudo_user: Union[str, None] = None
	atlas_discovery_interval: Union[int, None] = None
	cluster_name: Union[str, None] = None
	export_default_sessions: Union[int, None] = None
	export_max_sessions: Union[int, None] = None
	export_stage_disable: Union[bool, None] = None
	export_staging_database: Union[str, None] = None
	export_start_disable: Union[bool, None] = None
	hdfs_address: Union[str, None] = None
	hdfs_basedir: Union[str, None] = None
	hdfs_blocksize: Union[str, None] = None
	hive_acid_with_clusteredby: Union[bool, None] = None
	hive_insert_only_tables: Union[bool, None] = None
	hive_major_compact_after_merge: Union[bool, None] = None
	hive_print_messages: Union[bool, None] = None
	hive_remove_locks_by_force: Union[bool, None] = None
	hive_validate_before_execution: Union[bool, None] = None
	hive_validate_table: Union[str, None] = None
	impala_invalidate_metadata: Union[bool, None] = None
	import_columnname_delete: Union[str, None] = None
	import_columnname_histtime: Union[str, None] = None
	import_columnname_import: Union[str, None] = None
	import_columnname_insert: Union[str, None] = None
	import_columnname_iud: Union[str, None] = None
	import_columnname_source: Union[str, None] = None
	import_columnname_update: Union[str, None] = None
	import_default_sessions: Union[int, None] = None
	import_history_database: Union[str, None] = None
	import_history_table: Union[str, None] = None
	import_max_sessions: Union[int, None] = None
	import_process_empty: Union[bool, None] = None
	import_stage_disable: Union[bool, None] = None
	import_staging_database: Union[str, None] = None
	import_staging_table: Union[str, None] = None
	import_start_disable: Union[bool, None] = None
	import_work_database: Union[str, None] = None
	import_work_table: Union[str, None] = None
	kafka_brokers: Union[str, None] = None
	kafka_saslmechanism: Union[str, None] = None
	kafka_securityprotocol: Union[str, None] = None
	kafka_topic: Union[str, None] = None
	kafka_trustcafile: Union[str, None] = None
	post_airflow_dag_operations: Union[bool, None] = None
	post_data_to_kafka_extended: Union[bool, None] = None
	post_data_to_kafka: Union[bool, None] = None
	post_data_to_rest_extended: Union[bool, None] = None
	post_data_to_rest: Union[bool, None] = None
	restserver_admin_user: Union[str, None] = None
	restserver_authentication_method: Union[str, None] = None
#	restserver_secret_key: Union[str, None] = None
	restserver_token_ttl: Union[int, None] = None
	rest_timeout: Union[int, None] = None
	rest_trustcafile: Union[str, None] = None
	rest_url: Union[str, None] = None
	rest_verifyssl: Union[bool, None] = None
	spark_max_executors: Union[int, None] = None
	timezone: Union[str, None] = None

class tableRead(BaseModel):
	database: str
	table: str
	connection: str
	sourceSchema: str
	sourceTable: str
	importPhaseType: str
	etlPhaseType: str
	importTool: str
	etlEngine: str
	lastUpdateFromSource: Union[str, None] = None
	sqlWhereAddition: Union[str, None] = None
	nomergeIngestionSqlAddition: Union[str, None] = None
	includeInAirflow: bool
	airflowPriority: Union[str, None] = None
	validateImport: bool
	validationMethod: str
	validateSource: str
	validateDiffAllowed: int
	validationCustomQuerySourceSQL: Union[str, None] = None
	validationCustomQueryHiveSQL: Union[str, None] = None
	validationCustomQueryValidateImportTable: bool
	truncateTable: bool
	mappers: int
	softDeleteDuringMerge: bool
	sourceRowcount: Union[int, None] = None
	sourceRowcountIncr: Union[int, None] = None
	targetRowcount: Union[int, None] = None
	validationCustomQuerySourceValue: Union[str, None] = None
	validationCustomQueryHiveValue: Union[str, None] = None
	incrMode: Union[str, None] = None
	incrColumn: Union[str, None] = None
	incrValidationMethod: str
	incrMinvalue: Union[str, None] = None
	incrMaxvalue: Union[str, None] = None
	incrMinvaluePending: Union[str, None] = None
	incrMaxvaluePending: Union[str, None] = None
	pkColumnOverride: Union[str, None] = None
	pkColumnOverrideMergeonly: Union[str, None] = None
	mergeHeap: Union[int, None] = None
	splitCount: Union[int, None] = None
	sparkExecutorMemory: Union[str, None] = None
	sparkExecutors: Union[int, None] = None
	splitByColumn: Union[str, None] = None
	customQuery: Union[str, None] = None
	sqoopOptions: Union[str, None] = None
	lastSize: Union[int, None] = None
	lastRows: Union[int, None] = None
	lastMappers: Union[int, None] = None
	lastExecution: Union[int, None] = None
	useGeneratedSql: bool
	allowTextSplitter: bool
	forceString: int
	comment: Union[str, None] = None
	generatedHiveColumnDefinition: Union[str, None] = None
	generatedSqoopQuery: Union[str, None] = None
	generatedSqoopOptions: Union[str, None] = None
	generatedPkColumns: Union[str, None] = None
	generatedForeignKeys: Union[str, None] = None
	datalakeSource: Union[str, None] = None
	operatorNotes: Union[str, None] = None
	copyFinished: Union[str, None] = None
	copySlave: bool
	createForeignKeys: int
	invalidateImpala: int
	customMaxQuery: Union[str, None] = None
	mergeCompactionMethod: str
	sourceTableType: str
	importDatabase: Union[str, None] = None
	importTable: Union[str, None] = None
	historyDatabase: Union[str, None] = None
	historyTable: Union[str, None] = None

