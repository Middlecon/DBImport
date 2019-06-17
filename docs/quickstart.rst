Quickstart
==========

The following guild will make a full import of the DBImport MySQL database into Hive under a database called dbimport. Pre-req for this Quickstart guide is that the installation described in :doc:`install` is executed and completed. The goal of the Quickstart is to show the basic setup of a complete database import including the schedule from Airflow. 

.. note:: In order to manipulate the DBImport configuration database, you will need a SQL tool that makes it easy to work with. A very common tool for this is HeidiSQL and that is what the author used when creating this Quickstart Guide 

Database Connections
--------------------

The first step for all imports is to create a connection to the source database that we will read the data from. This is done in the jdbc connections table. So create a new row in the *jdbc_connection* table with the following information

========== ============================================================
dbalias    dbimport
jdbc_url   jdbc:mysql://<HOSTNAME>/DBImport
========== ============================================================

Replace <HOSTNAME> with the host that you are using for the DBImport database. Localhost is not valid here, as you dont know what node the sqoop command will be running on in Yarn.

Username and Password
^^^^^^^^^^^^^^^^^^^^^

The username and password is encrypted and stored in the jdbc_connection table together with JDBC connection string and other information. To encrypt and save the username and password, you need to run the *manage* command tool::

./manage --encryptCredentials

You will first get a question about what Database Connection that the username and password should be used on, and then the username and password itself. Use the following information for the questions

===================== ====================================================
DataBase Connection   dbimport
Username              Username to connect to the DBImport config database
Password              Password to connect to the DBImport config database
===================== ====================================================

Once all three items are entered, the username and password will be encrypted and saved in the *credentials* column in *jdbc_connections* table. You can verify this by refreshing the table in the SQL tool to make sure that you got a cryptic string in the *credentials* column.

Testing connection
^^^^^^^^^^^^^^^^^^

After the Database Connection is created, JDBC string is entered and username/password is encrypted and saved, you are ready to test the connection to make sure that DBImport can connect to the remote database.::

./manage --testConnection -a dbimport

Make sure that you get a *Connected successfully* message before you continue on this quickstart guide


Adding tables to import
-----------------------

Time to add tables that we are going to import. We will use the table search function for this. That means that we will connect to the source system using the JDBC connection we just configured and list what tables are available there and import them as tables to import. This is done by executing the following command::

./manage --addImportTable -a dbimport -h dbimport

The command options we use here is the following.

== ============================================================
-a The connection alias we configured in jdbc_connections table
-h Name of the Hive database we will import to
== ============================================================

Running the manage command will give you a list of all tables DBImport can find on the source database. As we are importing the DBImports own tables into Hive, this will be a list of all tables in DBImport. Answer 'Yes' on the validation question and DBImpoirt have now created definitions for the import of all tables in *import*tables* table.

Update import configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are many options to configure for each and every table that we are importing. In order for this quickstart guide to work, we need to change one of the options to handle Primary Keys in the source system that are based on text columns. As this is small tables with just a couple of rows, the simplest way is to force sqoop to only use one mapper. By doing that, there will be no split and it doesnt mather if the Primary Key is a text columns or not. In order to force sqoop to use only one mapper, please update the following rows and columns. Without this, the imports will fail.

.. note:: As this quickstart guide assumes that there is no other data in import_tables than what we just imported, another simple option is to force all *mappers* columns into 1 with "update import_tables set mappers = 1". Doing this on a full production system is very very far from acceptable. But here and now, it's a simple way to get forward without changing every row below individual

+---------------------------+-----------+
| hive_table                | mappers   |
+===========================+===========+
| airflow_custom_dags       | 1         |
+---------------------------+-----------+
| airflow_dag_sensors       | 1         |
+---------------------------+-----------+
| airflow_etl_dags          | 1         |
+---------------------------+-----------+
| airflow_export_dags       | 1         |
+---------------------------+-----------+
| airflow_import_dags       | 1         |
+---------------------------+-----------+
| airflow_tasks             | 1         |
+---------------------------+-----------+
| alembic_version           | 1         |
+---------------------------+-----------+
| auto_discovered_tables    | 1         |
+---------------------------+-----------+
| configuration             | 1         |
+---------------------------+-----------+
| etl_jobs                  | 1         |
+---------------------------+-----------+
| export_retries_log        | 1         |
+---------------------------+-----------+
| export_stage              | 1         |
+---------------------------+-----------+
| export_stage_statistics   | 1         |
+---------------------------+-----------+
| export_statistics_last    | 1         |
+---------------------------+-----------+
| export_tables             | 1         |
+---------------------------+-----------+
| import_failure_log        | 1         |
+---------------------------+-----------+
| import_retries_log        | 1         |
+---------------------------+-----------+
| import_stage              | 1         |
+---------------------------+-----------+
| import_stage_statistics   | 1         |
+---------------------------+-----------+
| import_statistics_last    | 1         |
+---------------------------+-----------+
| import_tables             | 1         |
+---------------------------+-----------+
| jdbc_connections          | 1         |
+---------------------------+-----------+
| jdbc_connections_drivers  | 1         |
+---------------------------+-----------+
| jdbc_table_change_history | 1         |
+---------------------------+-----------+
| table_change_history      | 1         |
+---------------------------+-----------+

Running the first import
------------------------

Connection to the source database is in place and we have added the tables to import to the *import_tables* table. We are now ready to test our first DBImport import. For this test, we will use the *configuration* table. To start the Import from command line, run the following::

./import -h dbimport -t configuration

The command options we use here is the following.

== ============================================================
-h Name of the Hive database 
-t Name of the Hive table
== ============================================================
