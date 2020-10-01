Configuration
=============

.. note:: The current version of DBImport dont have the webUI available. That means that the most of the configuration that needs to be done have to be directly in the MySQL database. Current users of DBImport use HeidiSQL_ as the client tool to change the configuration in the database

.. _HeidiSQL: https://www.heidisql.com/

Database Connections
--------------------

All communications against a source or target system goes against a Database Connection. This connection is configured in the jdbc_connections table. 

Username and Password
^^^^^^^^^^^^^^^^^^^^^

The username and password is encrypted and stored in the jdbc_connection table together with JDBC connection string and other information. To encrypt and save the username and password, you need to run the *manage* command tool::

    manage --encryptCredentials

You will first get a question about what Database Connection that the username and password should be used on, and then the username and password itself. Once all three items are entered, the username and password will be encrypted and saved in the *credentials* column in *jdbc_connections* table. 


JDBC Connection String
^^^^^^^^^^^^^^^^^^^^^^

The JDBC string needs to be entered manually into the *jdbc_url* column in the *jdbc_connections* table. Common for all JDBC connection strings is that you can add additional settings that is separated by a ; after the JDBC string that is documentat at each database type. 


**DB2 AS400**::

    jdbc:as400://<HOSTNAME>:<PORT>/<DATABASE>

**DB2 UDB**::

    jdbc:db2://<HOSTNAME>:<PORT>/<DATABASE>

**Microsoft SQL Server**::

There are two different ways to enter the JDBC URL for MSSQL. Default Microsoft JDBC or jTDS JDBC. jTDS is used when you are autenticating with a user that is in AD and the standard Microsoft JDBC is used when the SQL Server have local users that you connect with::

    jdbc:sqlserver://<HOSTNAME>:<PORT>;database=<DATBASE NAME>
    jdbc:jtds:sqlserver://<HOSTNAME>:<PORT>;useNTLMv2=true;domain=<DOMAIN>;databaseName=<DATBASE NAME>

**MySQL**::

    jdbc:mysql://<HOSTNAME>:<PORT>/<DATABASE>

**Oracle**::

    jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=<HOSTNAME>)(PORT=<PORT>)))(CONNECT_DATA=(SERVICE_NAME=<SERVICE NAME>)))

**PostgreSQL**::

    jdbc:postgresql://<HOSTNAME>:<PORT>/<DATABASE>

**Progress**::

    jdbc:datadirect:openedge://<HOSTNAME>:<PORT>;databaseName=<DATABASE>


Testing connection
^^^^^^^^^^^^^^^^^^

After the Database Connection is created, JDBC string is entered and username/password is encrypted and saved, you are ready to test the connection to make sure that DBImport can connect to the remote database.:: 

    ./manage --testConnection -a <DATABASE CONNECTION>


Adding tables to Import
-----------------------

There are two ways to add tables from sources that we are going to import. Manually direct in the database or by running the search tool and add the dicsovered tables to the import_tables table. This documentation is about the search tool

The most simple way to search for tables to import is by running the following::

./manage --addImportTable -a <DATABASE CONNECTION> -h <HIVE DB>

This will add all tables and view that the tool can discover on the source database specified by <DATABASE CONNECTION> and add them to the <HIVE DB>.

In some cases, you dont want to add all the tables that the tool discovers. Maybe the tool discovers system tables, temp tables or other unwanted stuff that is not needed. To handle that, you can add filters for the schema and the table on the source system. The is done by adding the following to the **manage** command.

== ===================================================
-S Filter the schema name. * as wildcard is supported
-T Filter the table name. * as wildcard is supported
== ===================================================

You also have the ability to controll what the table in Hive should be called. The following options are available for you to change the table name

===========================  ===================================================================================================================
\\-\\-addCounterToTable      Adds a number to the table name. Starts from 1 if not \\-\\-counterStart is supplied
\\-\\-counterStart=<NUMBER>  Forces \\-\\-addCounterToTable to start from a specific number. Both with or without 0 in the beginning is supported
\\-\\-addSchemaToTable       Adds the schema from the source system to the Hive table
\\-\\-addCustomText          Adds a custom text to the Hive table
===========================  ===================================================================================================================


Adding tables to Export
-----------------------

There are two ways to add tables from Hive that we are going to export. Manually direct in the database or by running the search tool and add the dicsovered tables to the export_tables table. This documentation is about the search tool

The most simple way to search for tables to export is by running the following::

./manage --addExportTable -a <DATABASE CONNECTION> -S <SCHEMA>

This will add all tables and view that the tool can discover in Hive as exports to the connection specified by <DATABASE CONNECTION> and in the schema specified in <SCHEMA>.

In most cases, you dont want to export all tables in Hive to a specific database. To handle that, you can add filters for the Hive database and/or table. The is done by adding the following to the **manage** command.

== ======================================================
-h Filter the Hive Database. * as wildcard is supported
-t Filter the Hive Table. * as wildcard is supported
== ======================================================

You also have the ability to controll what the table in the remote database should be called. The following options are available for you to change the table name

===========================  ====================================================================================================================
\\-\\-addCounterToTable      Adds a number to the table name. Starts from 1 if not \\-\\-counterStart is supplied
\\-\\-counterStart=<NUMBER>  Forces \\-\\-addCounterToTable to start from a specific number. Both with or without 0 in the beginning is supported
\\-\\-addDBToTable           Adds the schema from the source system to the Hive table
\\-\\-addCustomText          Adds a custom text to the Hive table
===========================  ====================================================================================================================


Validation
----------

There are two validation methods available for DBImport. Row count and custom SQL. Row count is doing exactly what it says it's doing. Count the number of rows available in the source/target database and count the number of rows in Hive. If these match, validation succeeded. There is a certain amonut of missmatch allowed and this can be configured to allow a certain amount of missmatched rows. The other option is to use a custom SQL code. There is one SQL for the source/target database and one SQL for the Hive database. These SQL codes will be executed and the result will be converted to a json document with only the values. Columnnames and such are not part of the json document. When both SQL queries have been executed, the two json documents must match. So if you want to use a sum() on the primary key and compare that result, it would work. Or if you want a max() on a timestamp column, that works as well. Or maybe just the last 10 rows inserted, that is also possible. There is a limit on 512 bytes for the json file, but nothing else.


Row count validation
^^^^^^^^^^^^^^^^^^^^

**Imports**

For imports, these are the configuration properties in import_tables that are used to configure row count validation

  validate_import        | Should the import be validated at all. 0 for no validation and 1 for validation. 
  validationMethod       | Validation method to use. For row count validation, you select, believe it or not, 'rowCount'
  validate_source        | Where should the source row count come from. There are two option. DBImport can execute a "select count(*) from ..." or just take the number of rows that spark or sqoop imported and use that as the number of rows in the source system. 
                         | Both have it's advantages. Running the select count(*) statement will return the actual rows on the source systemen, regardless of how many rows sqoop or spark imported. But lets say it's a log table and the table is filled with new data all the time. Then the number of rows that was added between the select statement and the time for spark or sqoop to execute will most likely exceed the allowed number of difference in row count between source and Hive. In this case, it's better to use the 'sqoop' method. Then the number of rows in the source system will be what spark or sqoop imported. 
                         | **Note**: Even if the setting is 'sqoop', it also works for spark. This is a legacy setting that was created when only sqoop was supported by DBImport.
  validate_diff_allowed  | The default setting is -1. That means that the number of rows that are allowed to diff is handled automaticly. If it's a large table with many rows, the allowed diff is larger than a small table. 
                         | Setting this to a fixed value will only allow these many rows in diff. 
                         | **Note**: Formula for auto settings is the following. *rowcount*(50/(100*math.sqrt(rowcount)))*
  incr_validation_method | If the import is an incremental import, then you have the option to choose if you are going to validate against the full number of rows or only validate the incremental rows that you are importing. There are cases when for example the source system only keeps a X number of days data in their tables. Then after X number of days of incremental imports, there will be more data in Hive compared to the source system. Then the 'full' ince_validation_method will fail as the total number of rows will be different. In this case, the 'incr' method should be used. What it basically does is to add the min and max values for the incremental load to the select count statement. So only the incrementally loaded rows are counted.


**Exports**

For exports, these are the configuration properties in export_tables that are used to configure row count validation

  validate_export        | Should the export be validated at all. 0 for no validation and 1 for validation. 
  validationMethod       | Validation method to use. For row count validation, you select, believe it or not, 'rowCount'
  incr_validation_method | If the export is an incremental export, then you have the option to choose if you are going to validate against the full number of rows or only validate the incremental rows that you are exporting. 


custom SQL validation
^^^^^^^^^^^^^^^^^^^^^

**Variables**

There are certain variables that can be used in the queries. These will during runtime be replaced with the real values. It makes it faster to configure the same custom SQL queries on multiple tables when only for example the tablename is different.

These are the available variables

================== ================
${HIVE_DB}         Replaced with the Hive Database configured in both imports and exports 
${HIVE_TABLE}      Replaced with the Hive Table configured in both imports and exports
${SOURCE_SCHEMA}   Replaced with source database schema in imports
${SOURCE_TABLE}    Replaced with source database table in imports
${TARGET_SCHEMA}   Replaced with target database schema in exports
${TARGET_TABLE}    Replaced with target database table in exports
================== ================


**Imports**

These are the configuration properties in import_tables that are used to configure custom SQL validation

  validate_import                           | Should the import be validated at all. 0 for no validation and 1 for validation.
  validationMethod                          | Validation method to use. For custom SQL validation, you select 'customQuery'
  validationCustomQuerySourceSQL            | The SQL query that will be executed in the source database
  validationCustomQueryHiveSQL              | The SQL query that will be executed in Hive. ${HIVE_DB} and ${HIVE_TABLE} variable must be used as the query will be executed on both the *Import Table* and *Target Table*
  validationCustomQueryValidateImportTable  | For certain imports, like incremental imports, running the custom sql against the *import table* have a large risk of returning the incorrect result. So for custom SQL imports, it's possible to disable the validation on the *import table* and only do the validation on the *target table*. Putting 0 in this column will disable validation on the *import table*


**Exports**

These are the configuration properties in export_tables that are used to configure custom SQL validation

  validate_export                           | Should the export be validated at all. -1 for no validation and 1 for validation.
  validationMethod                          | Validation method to use. For custom SQL validation, you select 'customQuery'
  validationCustomQueryHiveSQL              | The SQL query that will be executed in Hive. 
  validationCustomQueryTargetSQL            | The SQL query that will be executed in the target database

