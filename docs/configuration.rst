Configuration
=============

.. note:: The current version of DBImport dont have the webUI available. That means that the most of the configuration that needs to be done have to be directly in the MySQL database. Current users of DBImport use `HeidiSQL < https://www.heidisql.com/>`_ as the client tool to change the configuration in the database

Adding tables to Import
-----------------------

There are two ways to add tables from sources that we are going to import. Manually direct in the database or by running the search tool and add the dicsovered tables to the import_tables table. This documentation is about the search tool

The most simple way to search for tables to import is by running the following::

./manage --addImportTable -a v10 -h user_boszkk -T pur* --addCounterToTable --counterStart=002 --addSchemaToTable --addCustomText=TEST
