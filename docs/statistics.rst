Statistics
==========

There are three different ways to get statistics out of DBImport. Two of them saves the finished exports and imports statistics in a JSON format and sends them to a REST or Kafka endpoint and the third option is the statistics tables inside the DBImport configuration database. The JSON data is created only if configured in the configuration table. The statistics in the database tables are always created as DBImport is using that internally aswell.

Database Tables
^^^^^^^^^^^^^^^

There are statistical information in four different tables

  - import_statistics
  - import_statistics_last
  - export_statistics
  - export_statistics_last

The reason to keep the _last tables is because many time you only want to see data for the last export/import for a specific table. So instead of having to go through a lot of data, you can just get the last information from these tables. This will reduce the load on the MySQL database server for the cost of a little storage. 

JSON
^^^^

JSON default and extended format
--------------------------------

There are two different JSON formats. The default and the extended format. The main difference is that the extended format includes statistics for all internal DBImport stages where the default one includes the statistics for the entire import or export of the table. For external usage, where third-party application uses the statistics to start a job or run an application when a specific import/export is finished, the default format is the most common one.

You can select what format should be sent to what endpoint individually. So it's supported to send the default JSON format to the Kafka endpoint and the extended format to a REST endpoint if that is required.

The configuration for what service and format to use is located in the configurations_ table in the config database, and have the prefix post_.

default format for imports

.. code-block:: json
    {
        "type": "import", 
        "hive_db": "defaul", 
        "hive_table": "table1", 
        "import_phase": "full", 
        "etl_phase": "truncate_insert", 
        "incremental": false, 
        "source_database": "test", 
        "source_schema": "dbo", 
        "source_table": "table1", 
        "size": 41671452, 
        "rows": 2989699, 
        "start": "2021-11-08 06:59:47", 
        "stop": "2021-11-08 07:08:21", 
        "duration": 514
    }

The extended format depends on what import or export method you are using. The internal stages in an full import with truncate is not the same as an incremental merge with history import. The following example is from a full import with truncate import

.. code-block:: json

    {
        "type": "import", 
        "hive_db": "default", 
        "hive_table": "table2", 
        "import_phase": "full", 
        "copy_phase": "none", 
        "etl_phase": "truncate_insert", 
        "incremental": false, 
        "source_database": "test", 
        "source_schema": "dbo", 
        "source_table": "table2", 
        "size": "686", 
        "rows": 5, 
        "sessions": 1, 
        "get_source_tableschema_start": "2021-11-08 07:51:30", 
        "get_source_tableschema_stop": "2021-11-08 07:51:31", 
        "get_source_tableschema_duration": 2, 
        "clear_table_rowcount_start": "2021-11-08 07:51:31", 
        "clear_table_rowcount_stop": "2021-11-08 07:51:31", 
        "clear_table_rowcount_duration": 0, 
        "get_source_rowcount_start": "2021-11-08 07:51:31", 
        "get_source_rowcount_stop": "2021-11-08 07:51:31", 
        "get_source_rowcount_duration": 0, 
        "spark_start": "2021-11-08 07:51:31", 
        "spark_stop": "2021-11-08 07:52:21", 
        "spark_duration": 50, 
        "validate_sqoop_import_start": "2021-11-08 07:52:21", 
        "validate_sqoop_import_stop": "2021-11-08 07:52:21", 
        "validate_sqoop_import_duration": 0, 
        "atlas_schema_start": "2021-11-08 07:52:21", 
        "atlas_schema_stop": "2021-11-08 07:52:22", 
        "atlas_schema_duration": 1, 
        "copy_data_start": "2021-11-08 07:52:22", 
        "copy_data_stop": "2021-11-08 07:52:28", 
        "copy_data_duration": 6, 
        "copy_schema_start": "2021-11-08 07:52:28", 
        "copy_schema_stop": "2021-11-08 07:52:29", 
        "copy_schema_duration": 1, 
        "connect_to_hive_start": "2021-11-08 07:52:29", 
        "connect_to_hive_stop": "2021-11-08 07:52:31", 
        "connect_to_hive_duration": 1, 
        "create_import_table_start": "2021-11-08 07:52:31", 
        "create_import_table_stop": "2021-11-08 07:52:31", 
        "create_import_table_duration": 0, 
        "get_import_rowcount_start": "2021-11-08 07:52:31", 
        "get_import_rowcount_stop": "2021-11-08 07:52:42", 
        "get_import_rowcount_duration": 11, 
        "validate_import_table_start": "2021-11-08 07:52:42",
        "validate_import_table_stop": "2021-11-08 07:52:42", 
        "validate_import_table_duration": 0, 
        "clear_hive_locks_start": "2021-11-08 07:52:42", 
        "clear_hive_locks_stop": "2021-11-08 07:52:42", 
        "clear_hive_locks_duration": 0, 
        "create_target_table_start": "2021-11-08 07:52:42", 
        "create_target_table_stop": "2021-11-08 07:52:43", 
        "create_target_table_duration": 0, 
        "truncate_target_table_start": "2021-11-08 07:52:43", 
        "truncate_target_table_stop": "2021-11-08 07:52:43", 
        "truncate_target_table_duration": 1, 
        "hive_import_start": "2021-11-08 07:52:43", 
        "hive_import_stop": "2021-11-08 07:52:57", 
        "hive_import_duration": 14, 
        "update_statistics_start": "2021-11-08 07:52:57", 
        "update_statistics_stop": "2021-11-08 07:53:02", 
        "update_statistics_duration": 4, 
        "get_target_rowcount_start": "2021-11-08 07:53:02", 
        "get_target_rowcount_stop": "2021-11-08 07:53:09", 
        "get_target_rowcount_duration": 7, 
        "validate_target_table_start": "2021-11-08 07:53:09", 
        "validate_target_table_stop": "2021-11-08 07:53:09", 
        "validate_target_table_duration": 0, 
        "start": "2021-11-08 07:51:30", 
        "stop": "2021-11-08 07:53:09", 
        "duration": 99
    }

JSON information about Airflow DAG executions
---------------------------------------------

It's possible to send a JSON when an Airflow DAG is started and stopped. To enable this feature, set valueInt_ to 1 in configuration_ table where the configKey is post_airflow_dag_operations_. The result will be that the start task, after passsing if it's ok to start, will send a JSON saying that the DAG started. The stop task will also send a JSON saying that the DAG is finished. The JSON have the following format.

Start JSON
.. code-block:: json

    {
        "type": "airflow_dag", 
        "status": "started", 
        "dag": "<name of dag>", 
    }


Stop JSON
.. code-block:: json

    {
        "type": "airflow_dag", 
        "status": "finished", 
        "dag": "<name of dag>", 
    }


Setting up a REST service
-------------------------

Setting up the actual endpoint that is receiving the JSON data is out-of-scope for this documentation. Current users of DBImport is using Nifi together with the `HandleHttpRequest <https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.9.2/org.apache.nifi.processors.standard.HandleHttpRequest/>`_ and the `HandleHttpResponse <https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.9.2/org.apache.nifi.processors.standard.HandleHttpResponse/>`_ processors. Thats a very easy way to get started with a REST interface and using the JSON statistics from DBImport.
