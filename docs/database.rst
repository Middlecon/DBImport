Database Tables
===============

As this version of DBImport dont have the web admin tool available, the documentation will be agains each column in the configuration database tables. The admin tool will later use the same fields so whats said in here will later be applicable on the admin tool aswell.

Table - jdbc_connections
------------------------

This table is used for storing all the connection details against the different source databases. 
+------------------+----------------------------------------------------+
| Column           | Description                                        |
+==================+====================================================+
| dbalias          | Name of the JDBC connection                        |
+------------------+----------------------------------------------------+
| private_key_path | NOT USED                                           |
+------------------+----------------------------------------------------+

