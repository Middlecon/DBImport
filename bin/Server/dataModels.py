import json
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import Union
from typing import NewType
from typing_extensions import Annotated

class Item(BaseModel):
	name: str
	description: Union[str, None] = None
	price: float
	tax: Union[str, None] = None

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
	rest_timeout: Union[int, None] = None
	rest_trustcafile: Union[str, None] = None
	rest_url: Union[str, None] = None
	rest_verifyssl: Union[bool, None] = None
	spark_max_executors: Union[int, None] = None
	timezone: Union[str, None] = None




