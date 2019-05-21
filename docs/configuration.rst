Configuration
=============

.. note:: The current version of DBImport dont have the webUI available. That means that the most of the configuration that needs to be done have to be directly in the MySQL database. Current users of DBImport use HeidiSQL_ as the client tool to change the configuration in the database

.. _HeidiSQL: https://www.heidisql.com/

Database Connections
--------------------

All communications against a source or target system goes against a Database Connection. This connection is configured in the jdbc_connections table. Common for all JDBC connection strings is that you can add additional settings that is separated by a ; after the JDBC string that is documentat at each database type.


**DB2 AS400**

jdbc:as400://<HOSTNAME>:<PORT>/<DATABASE>

**DB2 UDB**

jdbc:db2://<HOSTNAME>:<PORT>/<DATABASE>

**Microsoft SQL Server**

There are two different ways to enter the JDBC URL for MSSQL. Default Microsoft JDBC or jTDS JDBC. jTDS is used when you are autenticating with a user that is in AD and the standard Microsoft JDBC is used when the SQL Server have local users that you connect with

jdbc:sqlserver://<HOSTNAME>:<PORT>;database=<DATBASE NAME>
jdbc:jtds:sqlserver://<HOSTNAME>:<PORT>;useNTLMv2=true;domain=<DOMAIN>;databaseName=<DATBASE NAME>

**MySQL**

jdbc:mysql://<HOSTNAME>:<PORT>/<DATABASE>

**Oracle**

jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=<HOSTNAME>)(PORT=<PORT>)))(CONNECT_DATA=(SERVICE_NAME=<SERVICE NAME>)))

**PostgreSQL**

jdbc:postgresql://<HOSTNAME>:<PORT>/<DATABASE>

**Progress**

jdbc:datadirect:openedge://<HOSTNAME>:<PORT>;databaseName=<DATABASE>


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


