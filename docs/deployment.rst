Deployment
==========

To handle proper development in a test/development environment and then push the changes to a production system, DBImport have built-in functions for deployment. The deploymnets are based around the Airflow DAG configuration. That means that the entity that is deployed is the DAG together with all the Airflow tasks, import/export tables (including column configuration) and JDBC Connection details. The deployment itself only copies configuration between two different systems. No actions or data in Hive or HDFS is changed when doing this.

Configuration
-------------

The first thing to configure is the connection to the remote DBImport instance. This is done in the *dbimport_instances* table. The following information must be entered

================= ==============================================================================================================================================================================
name              Name of the connection. Usually the remote system cluster name defined in HDFS
db_hostname       Host where the MySQL database is running
db_port           Port where the MySQL database is running
db_database       Name of the DBImport database in MySQL
db_credentials    Leave empty right now. Will use the *manage* command to encrypt the data
sync_credentials  If set to 1, the encrypted username and password on the JDBC Connection configuration will be synced aswell. Only used when public and private key is the same on both systems
================= ==============================================================================================================================================================================

Once all that is in the table, you can use the *manage* command with the --encryptInstance option to encrypt the username and password for a user with select/insert/update/delete permissions in the remove MySQL database::

      ./manage --encryptInstance


Deploying changes
-----------------

On the cluster where you configured the remote connection, you can now execute the deploy command. Depending on if it's an import or export DAG, the command looks a little bit different

**Deploy Import DAG**
::

      ./deploy --deployAirflowImportDAG --deployTarget=<Remote DBImport Instance> --airflowDAG=<DAG NAME>
     
**Deploy Export DAG**
::

      ./deploy --deployAirflowExportDAG --deployTarget=<Remote DBImport Instance> --airflowDAG=<DAG NAME>
     
