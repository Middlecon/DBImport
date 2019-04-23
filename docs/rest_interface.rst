Statistics
==========

DBImport have the option to send JSON data to a REST interface with statistic and configuration data. The endpoint together with the option of what to send is controlled from the dbimport.cfg file.


JSON Example
------------

At the end of every import, all statistics about how long each stage took together with source and target tables, sqoop statistics and more are collected and saved in a JSON document. This document is then POST'ed to a REST interface. The following is an example of such a JSON document

.. code-block:: json

    {
        "clear_hive_locks_duration": 0,
        "clear_hive_locks_start": "2019-04-20 09:09:14",
        "clear_hive_locks_stop": "2019-04-20 09:09:14",
        "clear_table_rowcount_duration": 0,
        "clear_table_rowcount_start": "2019-04-20 09:08:38",
        "clear_table_rowcount_stop": "2019-04-20 09:08:38",
        "connect_to_hive_duration": 0,
        "connect_to_hive_start": "2019-04-20 09:09:12",
        "connect_to_hive_stop": "2019-04-20 09:09:12",
        "create_import_table_duration": 0,
        "create_import_table_start": "2019-04-20 09:09:12",
        "create_import_table_stop": "2019-04-20 09:09:13",
        "create_target_table_duration": 0,
        "create_target_table_start": "2019-04-20 09:09:14",
        "create_target_table_stop": "2019-04-20 09:09:15",
        "duration": 43,
        "get_import_rowcount_duration": 2,
        "get_import_rowcount_start": "2019-04-20 09:09:13",
        "get_import_rowcount_stop": "2019-04-20 09:09:14",
        "get_source_rowcount_duration": 0,
        "get_source_rowcount_start": "2019-04-20 09:08:38",
        "get_source_rowcount_stop": "2019-04-20 09:08:38",
        "get_source_tableschema_duration": 3,
        "get_source_tableschema_start": "2019-04-20 09:08:35",
        "get_source_tableschema_stop": "2019-04-20 09:08:38",
        "get_target_rowcount_duration": 0,
        "get_target_rowcount_start": "2019-04-20 09:09:17",
        "get_target_rowcount_stop": "2019-04-20 09:09:18",
        "hive_db": "hive_test_database",
        "hive_import_duration": 2,
        "hive_import_start": "2019-04-20 09:09:15",
        "hive_import_stop": "2019-04-20 09:09:17",
        "hive_table": "tbl_test",
        "importtype": "full",
        "incremental": false,
        "source_database": "the_source_database",
        "source_schema": "-",
        "source_table": "tbl_test",
        "sqoop_duration": 34,
        "sqoop_rows": 2,
        "sqoop_size": 5144,
        "sqoop_start": "2019-04-20 09:08:38",
        "sqoop_stop": "2019-04-20 09:09:12",
        "start": "2019-04-20 09:08:35",
        "stop": "2019-04-20 09:09:18",
        "truncate_target_table_duration": 1,
        "truncate_target_table_start": "2019-04-20 09:09:15",
        "truncate_target_table_stop": "2019-04-20 09:09:15",
        "type": "import",
        "update_statistics_duration": 1,
        "update_statistics_start": "2019-04-20 09:09:17",
        "update_statistics_stop": "2019-04-20 09:09:17",
        "validate_import_table_duration": 0,
        "validate_import_table_start": "2019-04-20 09:09:14",
        "validate_import_table_stop": "2019-04-20 09:09:14",
        "validate_sqoop_import_duration": 0,
        "validate_sqoop_import_start": "2019-04-20 09:09:12",
        "validate_sqoop_import_stop": "2019-04-20 09:09:12",
        "validate_target_table_duration": 0,
        "validate_target_table_start": "2019-04-20 09:09:18",
        "validate_target_table_stop": "2019-04-20 09:09:18"
    }

During *getSourceTableSchema*, all columns that are read from the source table are storde in the configuration database. At the same time, a JSON document is created and are also uploaded to the REST interface. The following is an example of a colum configuration JSON

.. code-block:: json

    {
        "column": "test_col_02",
        "column_type": "char(50)",
        "date": "2019-04-20 13:36:48.036947",
        "hive_db": "hive_test_database",
        "hive_table": "tbl_test",
        "source_column": "test_col_02",
        "source_column_type": "char(50)",
        "source_database": "the_source_database",
        "source_database_server": "server1.domain",
        "source_database_server_type": "mysql",
        "source_schema": "-",
        "source_table": "tbl_test",
        "type": "column_data"
    }

Setting up a REST service
-------------------------

Setting up the actual endpoint that is receiving the JSON data is out-of-scope for this documentation. Current users of DBImport is using Nifi together with the `HandleHttpRequest <https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.9.2/org.apache.nifi.processors.standard.HandleHttpRequest/>`_ and the `HandleHttpResponse <https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.9.2/org.apache.nifi.processors.standard.HandleHttpResponse/>`_ processors. Thats a very easy way to get started with a REST interface and using the JSON statistics from DBImport.
