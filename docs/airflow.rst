Airflow Integration
===================

As the number of databases and tables that DBImport is importing, the need to automate the scheduling of these imports grows aswell. This is where the integration with Airflow is so useful. The basic of it is that DBImport generate the DAG to Airflow including all the steps that is required to import the tables defined in DBImport. The result is that no large intevention is needed to schedule and operate an environment with with 10000's of tables running daily imports.

As described in the :doc:`quickstart` guide, the command to generate the Airflow DAG is the following::

./manage --airflowGenerate --airflowDAG=<DAG NAME>

**Example used on this page**

In order to show how a DAG is generated, a demo environment have been setup with one table imported for each *import_type* with the name having the prefix of *tbl_*. So a *full_append* import is called *tbl_full_append* and they are all in the Hive database *demo*. 

A DAG have also been created in the *airflow_import_dags* table with the following settings

=================== ============================================================
dag_name            DBImport_DEMO
schedule_interval   None
filter_hive         dbimport.*
=================== ============================================================

Generate a DAG
--------------

In order to generate the DAG as described under Examples above, run the following command::

./manage --airflowGenerate --airflowDAG=DBImport_DEMO

This will create a DAG file in a staging table as configued in *airflow_dag_staging_directory*. The staging directory is not used by Airflow and the file will never be activated from there. But it's a good place to check the file to verify that it looks correct. Only needed for very complex DAG's with many independent tasks and dependencies. But as this is a simple DAG, we add the -w  or the --airflowWriteDAG to the command and write the DAG directy to Airflow::

./manage --airflowGenerate --airflowDAG=DBImport_DEMO --airflowWriteDAG

The DAG is now available in Airflow and in paused mode. As soon as it's un-paused, it can be viewed and started from Airflow.


Default DAG layout
------------------

With the DAG just specified with default settings like what is described earlier on this page, a DAG will be created in Airflow and it will look something like this. 

.. image:: img/airflow_dag_default.jpg



