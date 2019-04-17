Installing DBImport
===================

Installation
------------

To install DBImport, please follow the steps on this page. That will setup DBImport and make you able to import your first database in notime.

 #. Download the source package from the latest release from the `GitHub Release Page <https://github.com/BerryOsterlund/DBImport/releases>`_. Create a directory and unpack the file::

        mkdir /usr/local/dbimport
        cd /usr/local/dbimport
        tar -xf dbimport-VERSION.tar
   

 #. There is a number of Python dependencies that needs to be in place before DBImport can start. As of python itself, DBImport is developed on Python 3.6. Other versions may work but it's not tested. Please install these packages::

        pip3.6 install --upgrade pip cryptography crypto pycrypto docutils numpy pandas mysql mysqlclient mysql-connector mysql-connector-python jaydebeapi pyhive PyHive reprint requests requests_kerberos thrift_sasl 


Database Setup
^^^^^^^^^^^^^^

 #. As the configuration is all located in a MariaDB database, you need to create this database and then create the required tables. We assume that you have a working MariaDB server as the setup of that is not part of this documentation.
    Create the configuration database running these commands::

        create database dbimport;
        source conf/dbschema.sql;
   

Thats it. DBImport is now installed and ready for usage. If this is your first time working with DBImport, the :doc:`quickstart` guide can help you to get up to speed fast.
