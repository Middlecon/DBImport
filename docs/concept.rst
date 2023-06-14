Concept
=======

Full import
-----------

There are two different ways to import tables with DBImport. Full import or incremental import. Full import does exactly what is says. It imports an entire table from the source system to Hive. Normal scenario is that a sqoop job starts and reads the source into Parquet files on HDFS. From there, an external table is created over these files and then a *select into* is executed from the external table to the target table. The target table is using ORC format and is a standard managed table in Hive. The import is normally truncating the target table before the insert and there is also validation happening that count the number of rows for both the sqoop job and in the external table to make sure that we have a full copy of the source table before the truncate is executed. To get a better understanding of what exactly is happening for a full import, please check the :doc:`import_method` page.

Incremental import
------------------

An incremental import reads just the changed values from the previous import and add them to the target table. This can be done in two different ways. You can look at an integer based column and read all values that is larger than the previous value you read. This is usually for log/change tables where the Primary Key is an auto incremented column. The other way is to use a *timestamp*. When using the *timestamp* alternative, you specify a column in the source system that contains a column with information about when the row was last changed. DBImport then imports only those rows that was changed from the last import execution. Most common way is to use this alternative together with a *merge* import that apply the changes to a table that already have the rows and update the values in the columns.

Full export
-----------

Full export makes a complete copy of the Hive table to the Target table. The data in the Target table will be truncated before the export is started.

Incremental export
------------------

An incremental export reads just the changed values from the previous export and add them to the target table. 

History Audit Table
-------------------

This is basically a log of the changes that have happened in the table between the two last imports. If it is an incremental load, we already know that the rows have been changed and it's a simple insert into the *History Audit table*. But sometimes you need log only the changes but there is no column that allows you to identify if the row was updated or not. The Full History Audit import is the solution for that. What it does is that it reads the entire table from the source system and then compare based on the Primary Key if any of the columns for that row was changed. If it was, it updates the row with new data but also log the changes in the History Audit Table. This way, you can use the History Audit Table as an audit function to see what have been changed from one day to another in each table that uses this import function.

Table Changes
-------------

The tables in the source systems changes, and usually without telling the Hadoop environment about it. DBImport handles this automatically for you. Each time an import is running, DBImport connects to the source system and reads the table schema and saves it in it's configuration database. Based on this data, the column in Hive is changed so it contains the same rows with the same column type and comment as the source system. So if the source system adds a column, DBImport adds it in Hive. If the column type changes or a column comment changes, DBImport will do the same in Hive. And this is for both the Import, Target and History table.

Different Table types
---------------------

DBImport uses different tables in Hive depending on the operation it's performing. 

- | *Import table*
  | These tables exists in the ETL database in Hive and are external tables pointing towards the Parquet file sqoop is loading. Usually not accessable for the end users.
- | *Target table*
  | The real table that you want the data to end up in. It's a managed Hive table based on ORC
- | *History Audit table*
  | History information based on the changes in the *Target table*. The table exists in the same Hive database as the *Target table* and have the prefix '_history' at the end of the table name. It's a managed Hive table based on ORC
- | *Delete table*
  | To handle deletes and log them into the *History table*, we need to save the rows that are being deleted into a separate table. This table only contains the columns that is part of the Primary Key. This table exists in the same database as the *Import table* and is usually not accessable for the end users. It's a managed Hive table based on ORC
- | *Temporary Export table*
  | Sqoop export is very limited compared to the imports. To overcome some of these limitation, it's sometimes required to create a staging table that we load from the real Hive table and then the sqoop export will export the data from the *Temporary Export table* instead. This is also the way incremental exports are handled. We load the *Temporary Export table* with the changed rows and then make a full export from there to the *Target table*. It's a managed Hive table based on ORC

Import Validation
-----------------

During import, both *full* and *incremental*, there will be three validations for each imported table. Validation in this context is either a rowcount and comparing the number of rows between the two tables or a custom SQL query executed on both source and target table. Depending on what method for validation is used, the process is a bit different.

Row count valdation 

  1. The first validation happens in the *import stage* and is done after sqoop/spark is executed. It compares the number of rows in the source system against the number of rows that sqoop or spark reported that it read. If this is an incremental import, then only the incremental values are compared. This means that the select statement against the source system includes the min and max values in a where statement
  2. Second validation is done after the import table is created. This validates the number of rows in the Parquet or Orc files against the source system. If it is an incremental import, then only the imported rows are validates. Same way as the first validation.
  3. The last validation occurs after the target table is loaded. This might be a full or incremental validation based on the configuration for that specific table. Incremental validation works the same way as the other two validations and the Full validation is comparing the total amount of rows between source and target table regardless of how many rows was imported.

custom SQL validation

  1. Validation of sqoop or spark imported data is not possible with custom SQL validation. So this will be skipped here
  2. Validation of *Import table* is done by running the custom SQL on the source system and another custom SQL on the *Import table*. The result is saved in a json and these two json documents are compared to each other.
  3. The last validation is of the *Target table*. The custom SQL will be executed aginst *Target table* and the same query that was executed in step 2 against the source table will be used. The result is saved in a json and the two json documents are compared to each other.

Export Validation
-----------------

For exports, there will be only one validation and that is at the end of the export. There is also two different validation methods available for exports, same as for imports. It's a row count or executing a custom SQL on both tables and compare the result. 

Sqoop and Spark
-----------------

DBImport support both sqoop and spark. This is selectable on table level and you can run with different tools on different import/exports on the same installation. 

If you are running Hive 3.x, all tables in Hive are transactional tables. Sqoop cant export tables that are transactional tables. So if you are running Hive 3.x, you are forced to use spark for the export tool.

ETL Engine
----------

After Import is completed there will be an ETL step. This is where data is being loaded from the initial save on hdfs and gets loaded in the target table accessible through Hive. This is also where History tables are created. Default ETL Engine is Hive. This means that after Spark or Sqoop have loaded the data, a session to Hive is established and the load from the data saved on HDFS too Hive will start.

From version 0.8, there is now also support to use Spark as the ETL engine. The benefit for this is that it will already have most of the data loaded in memory and by that, reduce the amount of I/O required. It also removes the need to start a Hive session and running the queries through that. Running with Spark as the ETL engine will also force the usage of Iceberg as the table format. At the release date of DBImport V0.8, CDP Private Cloud v7.1.8 does not support Iceberg out of the box. So additional steps needs to be taken in order to get Iceberg support in the current version of CDP. For more information on how to do this, please read under Installation.

AWS S3
------

Exporting data from Hive to AWS S3 have never been simpler. DBImport supports writing data to S3 in the same way as a normal export works. Support is only available for spark and not with sqoop. This feature is still under tech-preview

