Import methods
==============

When specifying a table to import, one of the most important setting is to select an import method. It basically defines if it’s a full import or an incremental import together with what will happen afterwards. Maybe a history table should be created or rows in the Hive table should be deleted even if it’s an incremental import. This mayor settings are what we call an *import method*.

.. note:: Some of these import methods are not available in the latest version of DBImport. But as some of these are used by an early version of DBImport that was not Python based and not opensource, we still keep the documentation for those. Once the unavailable import methods are implemented, they will work in the same way.
 
.. note:: The stege number seen in the documentation are the internal stage id that is used by DBImport. This number is also used in the *import_stage* and the *import_retries_log* table.
 
 
Full import
-----------

This is the most basic import method. It does exactly what it says. It reads the entire table from the source database and replace the data in the Hive table with the information.

Stages
^^^^^^

  1010. | *Getting source tableschema*
        | This stage connects to the source database and reads all columns, columntypes, primary keys, foreign keys and comments and saves the to the configuration database.
  1011. | *Clear table rowcount*
        | Removes the number of rows that was import in the previous import of the table
  1012. | *Get source table rowcount*
        | Run a ``select count(1) from ...`` on the source table to the number of rows
  1013. | *Sqoop import*
        | Executes the sqoop import and saves the source table in Parquet files
  1014. | *Validate sqoop import*
        | Validates that sqoop read the same amount of rows that exists in the source system. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1011
  1049. | *Stage1 Completed*
        | This is just a mark saying that the stage 1 is completed. If you selected to run only a stage 1 import, this is where the import will end.
  1050. | *Connecting to Hive*
        | Connects to Hive and runs a test to verify that Hive is working properly
  1051. | *Creating the import table in the staging database*
        | The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  1052. | *Get Import table rowcount*
        | Run a ``select count(1) from ...`` on the Import table in Hive to get the number of rows
  1053. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1050
  1054. | *Removing Hive locks by force*
        | Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
  1055. | *Creating the target table*
        | The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  1056. | *Truncate target table*
        | Clears the Hive target table
  1057. | *Copy rows from import to target table*
        | Insert all rows from the import table to the target table
  1058. | *Update Hive statistics on target table*
        | Updates all the statistcs in Hive for the table
  1059. | *Get Target table rowcount*
        | Run a ``select count(1) from ...`` on the Target table in Hive to get the number of rows
  1060. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1054


Incremental import
------------------

An incremental imports keeps track of how much data have been read from the source table and only imports the new data. There are two different ways to do this

**Append**
If data is added to the source table and there is an integer based column that increases for every new row (AUTO_INCREMENT), then *Append* mode is the way to go. 

**Last Modified**
If there is a column with the type of date or a timestamp, and it gets a new data/timestamp for every new row, then *Last Modified* the correct option. 


Stages
^^^^^^

  1110. | *Getting source tableschema*
        | This stage connects to the source database and reads all columns, columntypes, primary keys, foreign keys and comments and saves the to the configuration database.
  1111. | *Clear table rowcount*
        | Removes the number of rows that was import in the previous import of the table
  1112. | *Sqoop import*
        | Executes the sqoop import and saves the source table in Parquet files
  1113. | *Get source table rowcount*
        | Run a ``select count(1) from ... where incr_column > min_value and incr_column > max_value`` on the source table to get the number of rows. Due to the where statement, it only validaes the incremental rows
        | If the incremental validation method is 'full', then a ``select count(1) from ...`` without any where statement is also executed against the source table.
  1114. | *Validate sqoop import*
        | Validates that sqoop read the same amount of rows that exists in the source system. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1111
  1149. | *Stage1 Completed*
        | This is just a mark saying that the stage 1 is completed. If you selected to run only a stage 1 import, this is where the import will end.
  1150. | *Connecting to Hive*
        | Connects to Hive and runs a test to verify that Hive is working properly
  1151. | *Creating the import table in the staging database*
        | The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  1152. | *Get Import table rowcount*
        | Run a ``select count(1) ...`` on the Import table in Hive to get the number of rows
  1153. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table based on the min and max values that was used for sqoop. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1150
  1154. | *Removing Hive locks by force*
        | Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
  1155. | *Creating the target table*
        | The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  1156. | *Copy rows from import to target table*
        | Insert all rows from the import table to the target table
  1157. | *Update Hive statistics on target table*
        | Updates all the statistcs in Hive for the table
  1158. | *Get Target table rowcount*
        | If the incremental validation method is 'incr', then a ``select count(1) from ... where incr_column > min_value and incr_column > max_value`` on the target table to get the number of rows. If it is 'full', then a normal ``select count(1) from ...`` without any where statement will be executed instead
  1159. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table based on the min and max values that was used for sqoop. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.



