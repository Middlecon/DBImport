Quickstart
==========

The following guild will make a full import of the DBImport MySQL database into Hive under a database called dbimport. Pre-req for this Quickstart guide is that the installation described in :doc:`install` is executed and completed. The goal of the Quickstart is to show the basic setup of a complete database import including the schedule from Airflow. 

.. note:: In order to manipulate the DBImport configuration database, you will need a SQL tool that makes it easy to work with. A very common tool for this is HeidiSQL and that is what the author used when creating this Quickstart Guide 

Database Connections
--------------------

The first step for all imports is to create a connection to the source database that we will read the data from. This is done in the jdbc connections table. So create a new row in the *jdbc_connection* table with the following information

dbalias:    dbimport
jdbc_url:   jdbc:mysql://<HOSTNAME>/DBImport

Replace <HOSTNAME> with the host that you are using for the DBImport database. Localhost is not valid here, as you dont know what node the sqoop command will be running on in Yarn.

Username and Password
^^^^^^^^^^^^^^^^^^^^^

The username and password is encrypted and stored in the jdbc_connection table together with JDBC connection string and other information. To encrypt and save the username and password, you need to run the *manage* command tool::

./manage --encryptCredentials

You will first get a question about what Database Connection that the username and password should be used on, and then the username and password itself. Once all three items are entered, the username and password will be encrypted and saved in the *credentials* column in *jdbc_connections* table. You can verify this by refreshing the table in the SQL tool to make sure that you got a cryptic string in the *credentials* column.

Testing connection
^^^^^^^^^^^^^^^^^^

After the Database Connection is created, JDBC string is entered and username/password is encrypted and saved, you are ready to test the connection to make sure that DBImport can connect to the remote database.::

./manage --testConnection -a dbimport

Make sure that you get a *Connected successfully* message before you continue on this quickstart guide


Adding tables to Import
-----------------------

Time to add tables that we are going to import. We will use the table search function for this. That means that we will connect to the source system using the JDBC connection we just configured and list what tables are available there and import them as tables to import. This is done by executing the following command::

./manage --addImportTable -a dbimport -h dbimport

The command options we use here is the following.

== ============================================================
-a The connection alias we configured in jdbc_connections table
-h Name of the Hive database we will import to
== ============================================================

