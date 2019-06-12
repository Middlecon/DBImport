Installing DBImport
===================

Installation
------------

To install DBImport, please follow the steps on this page. That will setup DBImport and make you able to import your first database in notime.

 #. Download the source package from the latest release from the `GitHub Release Page <https://github.com/BerryOsterlund/DBImport/releases>`_. Create a directory and unpack the file::

        mkdir /usr/local/dbimport
        cd /usr/local/dbimport
        tar -xf dbimport-VERSION.tar
   

 #. There is a number of Python dependencies that needs to be in place before DBImport can start. As of python itself, DBImport is developed on Python 3.6. Other versions may work but it's not tested. It's also preferable to use a virtual environment for Python, but creating a virtual environment is not part of this documentation. Please install these python packages::

        pip3.6 install --upgrade pip cryptography crypto pycrypto docutils numpy pandas mysql mysqlclient mysql-connector mysql-connector-python jaydebeapi pyhive PyHive reprint requests requests_kerberos thrift_sasl sqlalchemy pymysql 


Base configuration
------------------

There is a file inside the *conf* directory called *dbimport.cfg*. This file contains configuration that is required to connect databases, HDFS and such. Before running any DBImport commands, this file needs to be configured. The important settings that you must configure is the following

  - Section [Database], all settings starting with *mysql*. This is the connection to the DBImport configuration database.
  - Section [REST_statistics]. Put *post_column_data* and *post_column_data* to *false* or configure a valid REST interface on *rest_endpoint*
  - Section [HDFS]. *hdfs_address* should be a valid URL to HDFS
  - Section [Hive]. *hive_metastore_alchemy_conn* must have a valid Alchemy connection string to the Hive Metastore SQL database
  - Section [Hive]. *hostname*, *port* and *kerberos_service_name* must all be set against a Hive server with binary transport mode.

.. note:: If the *hive_remove_locks_by_force* setting (in the configuration table) is set to 1, the user configured for the *hive_metastore_alchemy_conn* must have delete permissions to the *HIVE_LOCKS* table. Other than that, the user only requires select permissions. 

SSL keys
--------

As DBImport is saving connection details to all databases its importing from, this data needs to be encrypted and stored in a secure way. To do that, we have a privat/public keypair that encrypts and decrypts the credentials to the source system. These keys needs to be configured. Default configuration is to have them under the conf directory. This can be changed in the *dbimport.cfg* if if needed. To create the keys with the default path, run the following command::

        openssl genrsa -out conf/dbimport_private_key.pem 8192
        openssl rsa -in conf/dbimport_private_key.pem -out conf/dbimport_public_key.pem -outform PEM -pubout

Once created, make sure that the unix permission only enabled the correct users to read these files.


Database Setup
--------------

 #. As the configuration is all located in a MariaDB database, you need to create this database and then create the required tables. We assume that you have a working MariaDB server as the setup of that is not part of this documentation.
    Create the configuration database running these commands::

        bin/manage --createDB
   
Database Configuration
----------------------

There is also global configuration stored inside the newly created database. These settings are stored in the *configuration* table. There is one value that is important to configure correct as it requires creation of a table in Hive if enabled. And that is *hive_validate_before_execution*. If this is set to 1 in *valueInt* column, it will do a *select ... group by ....* from the table configured by the *hive_validate_table* setting. Default value is *dbimport.validate_table*. To create this table, please run the following in Hive::

        create database dbimport
        create table dbimport.validate_table ( id integer, value string )
        insert into dbimport.validate_table (id, value) values (1, 'Hive'), (2, 'LLAP'), (3, 'is'), (4, 'working'), (5, 'fine')

DBImport uses staging databases for creating of Import Tables and Temporary Export Tables. These databases needs to exist before running import and/or exports. The name of the databases are configured by the *import_staging_database* and the *export_staging_database*. To create the default databases, please run the following in Hive::

        create database etl_import_staging
        create database etl_export_staging

Thats it. DBImport is now installed and ready for usage. If this is your first time working with DBImport, the :doc:`quickstart` guide can help you to get up to speed fast.
