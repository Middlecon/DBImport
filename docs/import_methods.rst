Import methods
==============

When specifying a table to import, one of the most important setting is to select an import method. It basically defines if it’s a full import or an incremental import together with what will happen afterwards. Maybe a history table should be created or rows in the Hive table should be deleted even if it’s an incremental import. This mayor settings are what we call an *import method*.

.. note:: The stage number seen in the documentation are the internal stage id that is used by DBImport. This number is also used in the *import_stage* and the *import_retries_log* table.
 
.. note:: The default Hive table type is *managed table* using *ORC* if nothing else is specified.
 
 
Full import
-----------

Full imports reads the entire source table and makes it available in Hive. Depending on what ETL phase is used, the Target table can be created/updated in different ways. Most simple and most common way is to do a full import, but there are more complex imports that compare all values in all columns to create audit information.  

Truncate and insert
^^^^^^^^^^^^^^^^^^^

This is the most common way to import the data. The entire table is loaded by DBImport and then the target table in Hive is truncated and all rows are inserted again. 

+---------------------+-----------------------------------------------------+
| Setting             | Configuration                                       |
+=====================+=====================================================+
| Import Type (old)   | full                                                |
+---------------------+-----------------------------------------------------+
| Import Phase        | full                                                |
+---------------------+-----------------------------------------------------+
| ETL Phase           | truncate_insert                                     |
+---------------------+-----------------------------------------------------+


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
  3050. | *Connecting to Hive*
        | Connects to Hive and runs a test to verify that Hive is working properly
  3051. | *Creating the import table in the staging database*
        | The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3052. | *Get Import table rowcount*
        | Run a ``select count(1) from ...`` on the Import table in Hive to get the number of rows
  3053. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1050
  3054. | *Removing Hive locks by force*
        | Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
  3055. | *Creating the target table*
        | The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3056. | *Truncate target table*
        | Clears the Hive target table
  3057. | *Copy rows from import to target table*
        | Insert all rows from the import table to the target table
  3058. | *Update Hive statistics on target table*
        | Updates all the statistcs in Hive for the table
  3059. | *Get Target table rowcount*
        | Run a ``select count(1) from ...`` on the Target table in Hive to get the number of rows
  3060. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1054


Insert
^^^^^^

Full import with Insert will just add data to the target table without truncating. This will most likely create duplicates in the target database. As we still need to validate the appended data, the *create_datalake_import* must be set to 1 in *jdbc_connections* table for the connection the appended table is using.

+---------------------+-----------------------------------------------------+
| Setting             | Configuration                                       |
+=====================+=====================================================+
| Import Type (old)   | full_insert                                         |
+---------------------+-----------------------------------------------------+
| Import Phase        | full                                                |
+---------------------+-----------------------------------------------------+
| ETL Phase           | insert                                              |
+---------------------+-----------------------------------------------------+


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
  3100. | *Connecting to Hive*
        | Connects to Hive and runs a test to verify that Hive is working properly
  3101. | *Creating the import table in the staging database*
        | The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3102. | *Get Import table rowcount*
        | Run a ``select count(1) from ...`` on the Import table in Hive to get the number of rows
  3103. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1050
  3104. | *Removing Hive locks by force*
        | Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
  3105. | *Creating the target table*
        | The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3106. | *Copy rows from import to target table*
        | Insert all rows from the import table to the target table
  3107. | *Update Hive statistics on target table*
        | Updates all the statistcs in Hive for the table
  3108. | *Get Target table rowcount*
        | Run a ``select count(1) from ...`` on the Target table in Hive to get the number of rows
  3109. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1054


Full Merge
^^^^^^^^^^

Doing a Full Merge operation instead of a normal full import gives you one additional thing. It will create a number of new columns that will contain information about when was the last time the row was changed. This is a great way to get only changed data from a table that have no way to identify if the data in the row was changed or not. Will create a fairly large job in Hive during the merge, and depending on the cluster size, might take all resources available in the cluster.

+---------------------+-----------------------------------------------------+
| Setting             | Configuration                                       |
+=====================+=====================================================+
| Import Type (old)   | full_merge_direct                                   |
+---------------------+-----------------------------------------------------+
| Import Phase        | full                                                |
+---------------------+-----------------------------------------------------+
| ETL Phase           | merge                                               |
+---------------------+-----------------------------------------------------+


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
  3250. | *Connecting to Hive*
        | Connects to Hive and runs a test to verify that Hive is working properly
  3251. | *Creating the import table in the staging database*
        | The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3252. | *Get Import table rowcount*
        | Run a ``select count(1) from ...`` on the Import table in Hive to get the number of rows
  3253. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 3250
  3254. | *Removing Hive locks by force*
        | Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
  3255. | *Creating the Target table*
        | The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3256. | *Creating the Delete table*
        | The Delete table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3257. | *Merge Import table with Target table*
        | Merge all data in the Import table into the Target table based on PK and if any values is changed in any of the columns. 
  3258. | *Update Hive statistics on target table*
        | Updates all the statistcs in Hive for the table
  3259. | *Get Target table rowcount*
        | Run a ``select count(1) from ...`` on the Target table in Hive to get the number of rows
  3260. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1054


Full Merge with History Audit 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is one of the largest import method you can use. It will fetch all rows from the source system and once available in the Import Table, the data will be merge into the Target table. Do know what rows have been changed, all columns will be compared between the Import and the Target table. When that is done, a new merge will run that will find out what rows exists in the Target table and not in the Import table. These are the rows that was deleted in the source system. Once they are identified, they will be inserted into the History Audit table and then deleted from the Target table. 
Depending on the size of the table, this can be a very large job in Hive during the different merge commands. Keep that in mind when you select a timeslot to run the job.


+---------------------+-----------------------------------------------------+
| Setting             | Configuration                                       |
+=====================+=====================================================+
| Import Type (old)   | full_merge_direct_history                           |
+---------------------+-----------------------------------------------------+
| Import Phase        | full                                                |
+---------------------+-----------------------------------------------------+
| ETL Phase           | merge_history_audit                                 |
+---------------------+-----------------------------------------------------+


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
  3200. | *Connecting to Hive*
        | Connects to Hive and runs a test to verify that Hive is working properly
  3201. | *Creating the import table in the staging database*
        | The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3202. | *Get Import table rowcount*
        | Run a ``select count(1) from ...`` on the Import table in Hive to get the number of rows
  3203. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 3250
  3204. | *Removing Hive locks by force*
        | Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
  3205. | *Creating the Target table*
        | The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3206. | *Creating the History table*
        | The History table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3207. | *Creating the Delete table*
        | The Delete table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3208. | *Merge Import table with Target table*
        | Merge all data in the Import table into the Target table based on PK and if any values is changed in any of the columns. 
  3209. | *Update Hive statistics on target table*
        | Updates all the statistcs in Hive for the table
  3210. | *Get Target table rowcount*
        | Run a ``select count(1) from ...`` on the Target table in Hive to get the number of rows
  3211. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1054


Incremental import
------------------

An incremental imports keeps track of how much data have been read from the source table and only imports the new data. There are two different ways to do this

**Append**
If data is added to the source table and there is an integer based column that increases for every new row (AUTO_INCREMENT), then *Append* mode is the way to go. 

**Last Modified**
If there is a column with the type of date or a timestamp, and it gets a new data/timestamp for every new row, then *Last Modified* the correct option. 


Insert
^^^^^^

The changed data is read from the source and once it's avalable in the Import table, an insert operation will be triggered in Hive to insert the newly fetched rows into the Target table. 

+---------------------+-----------------------------------------------------+
| Setting             | Configuration                                       |
+=====================+=====================================================+
| Import Type (old)   | incr                                                |
+---------------------+-----------------------------------------------------+
| Import Phase        | incr                                                |
+---------------------+-----------------------------------------------------+
| ETL Phase           | insert                                              |
+---------------------+-----------------------------------------------------+


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
  3150. | *Connecting to Hive*
        | Connects to Hive and runs a test to verify that Hive is working properly
  3151. | *Creating the import table in the staging database*
        | The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3152. | *Get Import table rowcount*
        | Run a ``select count(1) ...`` on the Import table in Hive to get the number of rows
  3153. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table based on the min and max values that was used for sqoop. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1150
  3154. | *Removing Hive locks by force*
        | Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
  3155. | *Creating the target table*
        | The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3156. | *Copy rows from import to target table*
        | Insert all rows from the import table to the target table
  3157. | *Update Hive statistics on target table*
        | Updates all the statistcs in Hive for the table
  3158. | *Get Target table rowcount*
        | If the incremental validation method is 'incr', then a ``select count(1) from ... where incr_column > min_value and incr_column > max_value`` on the target table to get the number of rows. If it is 'full', then a normal ``select count(1) from ...`` without any where statement will be executed instead
  3159. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table based on the min and max values that was used for sqoop. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
  3160. | *Saving pending incremental values*
        | In order to start the next incremental import from the last entry that the current import read, we are saving the min and max values into the import_tables table. The next import will then start to read from the next record after the max we read this time.



Merge
^^^^^^

The changed data is read from the source and once it's avalable in the Import table, a merge operation will be executed in Hive. The merge will be based on the Primary Keys and will update the information in the Target table if it already exists and insert it if it's missing. Keep in mind that if the source table deletes rows, we wont fetch them with this import. 

+---------------------+-----------------------------------------------------+
| Setting             | Configuration                                       |
+=====================+=====================================================+
| Import Type (old)   | incr_merge_direct                                   |
+---------------------+-----------------------------------------------------+
| Import Phase        | incr                                                |
+---------------------+-----------------------------------------------------+
| ETL Phase           | merge                                               |
+---------------------+-----------------------------------------------------+

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
  3300. | *Connecting to Hive*
        | Connects to Hive and runs a test to verify that Hive is working properly
  3301. | *Creating the import table in the staging database*
        | The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3302. | *Get Import table rowcount*
        | Run a ``select count(1) from ...`` on the Import table in Hive to get the number of rows
  3303. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 3301
  3304. | *Removing Hive locks by force*
        | Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
  3305. | *Creating the Target table*
        | The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3306. | *Merge Import table with Target table*
        | Merge all data in the Import table into the Target table based on PK. 
  3307. | *Update Hive statistics on target table*
        | Updates all the statistcs in Hive for the table
  3308. | *Get Target table rowcount*
        | Run a ``select count(1) from ...`` on the Target table in Hive to get the number of rows
  3309. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 3304
  3310. | *Saving pending incremental values*
        | In order to start the next incremental import from the last entry that the current import read, we are saving the min and max values into the import_tables table. The next import will then start to read from the next record after the max we read this time.


Merge with History Audit 
^^^^^^^^^^^^^^^^^^^^^^^^

The changed data is read from the source and once it's avalable in the Import table, a merge operation will be executed in Hive. The merge will be based on the Primary Keys and will update the information in the Target table if it already exists and insert it if it's missing. Keep in mind that if the source table deletes rows, we wont fetch them with this import. After the merge is completed, it will also insert all new and changed rows into the History Audit Table so it's possible to track the changed in the table over time 

+---------------------+-----------------------------------------------------+
| Setting             | Configuration                                       |
+=====================+=====================================================+
| Import Type (old)   | incr_merge_direct_history                           |
+---------------------+-----------------------------------------------------+
| Import Phase        | incr                                                |
+---------------------+-----------------------------------------------------+
| ETL Phase           | merge_history_audit                                 |
+---------------------+-----------------------------------------------------+

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
  3350. | *Connecting to Hive*
        | Connects to Hive and runs a test to verify that Hive is working properly
  3351. | *Creating the import table in the staging database*
        | The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3352. | *Get Import table rowcount*
        | Run a ``select count(1) from ...`` on the Import table in Hive to get the number of rows
  3353. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 3301
  3354. | *Removing Hive locks by force*
        | Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
  3355. | *Creating the Target table*
        | The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3356. | *Creating the History table*
        | The History table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3357. | *Merge Import table with Target table*
        | Merge all data in the Import table into the Target table based on PK. 
  3358. | *Update Hive statistics on target table*
        | Updates all the statistcs in Hive for the table
  3359. | *Get Target table rowcount*
        | Run a ``select count(1) from ...`` on the Target table in Hive to get the number of rows
  3360. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 3304
  3361. | *Saving pending incremental values*
        | In order to start the next incremental import from the last entry that the current import read, we are saving the min and max values into the import_tables table. The next import will then start to read from the next record after the max we read this time.


Oracle Flashback
^^^^^^^^^^^^^^^^

This import method uses the Oracle Flashback Version Query to fetch only the changed rows from the last import. Comparing this to a standard incremental import, the main differences is that we detect *deletes* as well and that we dont require a timestamp or an integer based column with increasing values. The downside is that the table must support Oracle Flashback Version Query and that the undo area is large enough to keep changes between imports. Once the data is avalable in the Import table, a merge operation will be executed in Hive. The merge will be based on the Primary Keys and will update the information in the Target table if it already exists, delete the data if that happend in the source system and insert it if it's missing.

+---------------------+-----------------------------------------------------+
| Setting             | Configuration                                       |
+=====================+=====================================================+
| Import Type (old)   | oracle_flashback_merge                              |
+---------------------+-----------------------------------------------------+
| Import Phase        | oracle_flashback                                    |
+---------------------+-----------------------------------------------------+
| ETL Phase           | merge                                               |
+---------------------+-----------------------------------------------------+

  1210. | *Getting source tableschema*
        | This stage connects to the source database and reads all columns, columntypes, primary keys, foreign keys and comments and saves the to the configuration database.
  1211. | *Clear table rowcount*
        | Removes the number of rows that was import in the previous import of the table
  1212. | *Sqoop import*
        | Executes the sqoop import and saves the source table in Parquet files. This is where the Oracle Flashback *VERSION BETWEEN* query is executed against the source system.
  1213. | *Get source table rowcount*
        | Run a ``select count(1) from ... VERSIONS BETWEEN SCN <min_value> AND <max_value> WHERE VERSIONS_OPERATION IS NOT NULL AND VERSIONS_ENDTIME IS NULL`` on the source table to get the number of rows. Due to the where statement, it only validates the incremental rows
        | If the incremental validation method is 'full', then a ``select count(1) from ... VERSIONS BETWEEN SCN <min_value> AND <max_value> WHERE VERSIONS_ENDTIME IS NULL AND (VERSIONS_OPERATION != 'D' OR VERSIONS_OPERATION IS NULL)`` is also executed against the source table.
  1214. | *Validate sqoop import*
        | Validates that sqoop read the same amount of rows that exists in the source system. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 1211
  1249. | *Stage1 Completed*
        | This is just a mark saying that the stage 1 is completed. If you selected to run only a stage 1 import, this is where the import will end.
  3400. | *Connecting to Hive*
        | Connects to Hive and runs a test to verify that Hive is working properly
  3401. | *Creating the import table in the staging database*
        | The import table is created. This is an external table based on the Parquet files that sqoop wrote. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3402. | *Get Import table rowcount*
        | Run a ``select count(1) from ...`` on the Import table in Hive to get the number of rows
  3403. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 3301
  3404. | *Removing Hive locks by force*
        | Due to a bug in Hive, we need to remove the locks by force. This connects to the metadatabase and removes them from there
  3405. | *Creating the Target table*
        | The target table is created. Any changes on the exiting table compared the the information that was received in the *Getting source tableschema* stage is applied here.
  3406. | *Merge Import table with Target table*
        | Merge all data in the Import table into the Target table based on PK. 
  3407. | *Update Hive statistics on target table*
        | Updates all the statistcs in Hive for the table
  3408. | *Get Target table rowcount*
        | Run a ``select count(1) from ...`` on the Target table in Hive to get the number of rows
  3409. | *Validate import table*
        | Compare the number of rows from the source table with the number of rows in the import table. These dont have to match 100% and is based on the configuration in the import_tables.validate_diff_allowed column.
        | If the validation fails, the next import will restart from stage 3304
  3410. | *Saving pending incremental values*
        | In order to start the next incremental import from the last entry that the current import read, we are saving the min and max values into the import_tables table. The next import will then start to read from the next record after the max we read this time.

