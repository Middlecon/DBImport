Configuration
=============

.. note:: The current version of DBImport dont have the webUI available. That means that the most of the configuration that needs to be done have to be directly in the MySQL database. Current users of DBImport use `HeidiSQL < https://www.heidisql.com/>`_ as the client tool to change the configuration in the database

Database Connections
--------------------

All communications against a source or target system goes against a Database Connection. This connection is configured in the jdbc_connections table.

*JDBC String formats*




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

=======================  ===============================================================================================================
--addCounterToTable      Adds a number to the table name. Starts from 1 if not --counterStart is supplied
--counterStart=<NUMBER>  Forces --addCounterToTable to start from a specific number. Both with or without 0 in the beginning is supported
--addSchemaToTable       Adds the schema from the source system to the Hive table
--addCustomText          Adds a custom text to the Hive table
=======================  ===============================================================================================================


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

=======================  ===============================================================================================================
--addCounterToTable      Adds a number to the table name. Starts from 1 if not --counterStart is supplied
--counterStart=<NUMBER>  Forces --addCounterToTable to start from a specific number. Both with or without 0 in the beginning is supported
--addDBToTable           Adds the schema from the source system to the Hive table
--addCustomText          Adds a custom text to the Hive table
=======================  ===============================================================================================================


