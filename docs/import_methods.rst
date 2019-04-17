Import methods
==============

When specifying a table to import, one of the most important setting is to select an import method. It basically defines if it’s a full import or an incremental import together with what will happen afterwards. Maybe a history table should be created or rows in the Hive table should be deleted even if it’s an incremental import. This mayor settings are what we call an *import method*.

.. note:: Some of these import methods are not available in the latest version of DBImport. But as some of these are used by an early version of DBImport that was not Python based and not opensource, we still keep the documentation for those. Once the unavailable import methods are implemented, they will work in the same way.
 
 
Full import
-----------

This is the most basic import method. It does exactly what it says. It reads the entire table from the source database and replace the data in the Hive table with the information.

Stages
^^^^^^

There are multiple stages in the full import. 

#. Getting source tableschema
   This stage connects to the source database and reads all columns, columntypes, primary keys, foreign keys and comments and saves the to the configuration database. 
#. Clear table rowcount
   Removes the number of rows that was import in the previous import of the table
#. Get source table rowcount
   Run a * select count(1) ... * on the source table to the number of rows
#. Sqoop import
   Executes the sqoop import and saves the source table in Parquet files
#. Stage1 Completed
   This is just a mark saying that the stage 1 is completed. If you selected to run only a stage 1 import, this is where the import will end.
#. Connecting to Hive
   Connects to Hive and runs a test to verify that Hive is working properly
#. Creating the import table in the staging database
   The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here. 
#. Get Hive table rowcount
   Run a * select count(1) ... * on the import table in Hive to get the number of rows
#. Validate import
   Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
#. Removing Hive locks by force
   Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
#. Creating the target table
   The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here. 
#. Truncate target table
   Clears the Hive target table
#. Copy rows from import to target table
   Insert all rows from the import table to the target table
#. Update Hive statistics on target table
   Updates all the statistcs in Hive for the table

