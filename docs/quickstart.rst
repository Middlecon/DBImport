Quickstart
==========

The following guild will make a full import of the DBImport MySQL database into Hive under a database called dbimport. Pre-req for this Quickstart guide is that the installation described in :doc:`install` is executed and completed. The goal of the Quickstart is to show the basic setup of a complete database import including the schedule from Airflow. 

.. [#] The first step for all imports is to create a connection to the source database that we will read the data from. This is done in the jdbc connections table.  

.. [#] As the username and password are encrypted, we need to use the *manage* tool to encrypt the username and passwords. So start the following command to encrypt your username and password.


