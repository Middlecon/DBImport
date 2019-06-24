Export methods
==============

There are two different ways to export data. Full or Incrementally. 
 
.. note:: The stage number seen in the documentation are the internal stage id that is used by DBImport. This number is also used in the *export_stage* and the *export_retries_log* table.
 
 
Full export
-----------

Full imports reads the entire Hive table and makes a copy of it available in the Target database. 

+---------------------+-----------------------------------------------------+
| Setting             | Configuration                                       |
+=====================+=====================================================+
| Export Type         | full                                                |
+---------------------+-----------------------------------------------------+


  100. | *Getting Hive tableschema*
       | This stage connects to Hive and reads all columns, columntypes and comments and saves the to the configuration database.
  101. | *Clear table rowcount*
       | Removes the number of rows that was import in the previous import of the table
  102. | *Update Hive statistics on exported table*
       | Updates all the statistcs in Hive for the table that is exported. This is needed for correct row count
  103. | *Create Export Temp table*
       | If required, the export will create an Export Temp Table.
  104. | *Truncate Export Temp table*
       | If required, will truncate the Export Temp Table
  105. | *Insert data into Export Temp table*
       | If required, will insert data from the exported Hive table into the Export Temp Table
  106. | *Create Target table*
       | The Target table will be created on the system we are exporting data to. It will also update the table definition if there is a change in Hive. 
  107. | *Truncate Target table*
       | Truncates the table we will export to
  108. | *Sqoop Export*
       | Executes the sqoop export 
       | If the sqoop command fails, the next export will restart from stage 106
  109. | *Validations*
       | Compare the number of rows in Hive table with the number of rows in the target table.
       | If the validation fails, the next import will restart from stage 101


Incremental export
------------------

An incremental export keeps track of how much data have been read from the Hive table and only exports the new data. Incremental exports always requires a Temporary Export Table. 

+---------------------+-----------------------------------------------------+
| Setting             | Configuration                                       |
+=====================+=====================================================+
| Export Type         | incr                                                |
+---------------------+-----------------------------------------------------+

  150. | *Getting Hive tableschema*
       | This stage connects to Hive and reads all columns, columntypes and comments and saves the to the configuration database.
  151. | *Clear table rowcount*
       | Removes the number of rows that was import in the previous import of the table
  152. | *Update Hive statistics on exported table*
       | Updates all the statistcs in Hive for the table that is exported. This is needed for correct row count
  153. | *Create Export Temp table*
       | The Export Temp Table will be created.
  154. | *Truncate Export Temp table*
       | Truncate the Export Temp Table
  155. | *Fetching the Max value from Hive*
       | The max value for the incremental columns are read from Hive. This is used to get the incremental delta that we will load into the Target table.
  156. | *Insert data into Export Temp table*
       | Data will be inserted into the Export Temp Table based on the min and max values
  157. | *Create Target table*
       | The Target table will be created on the system we are exporting data to. It will also update the table definition if there is a change in Hive. 
  158. | *Sqoop Export*
       | Executes the sqoop export 
       | If the sqoop command fails, the next export will restart from stage 106
  159. | *Validations*
       | Compare the number of rows in Hive table with the number of rows in the target table.
       | If the validation fails, the next import will restart from stage 101
  160. | *Saving pending incremental values*
       | In order to start the next incremental export from the last entry that the current export read, we are saving the min and max values into the export_tables table. The next export will then start to read from the next record after the max we read this time.


