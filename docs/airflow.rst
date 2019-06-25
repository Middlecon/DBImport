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
filter_hive         demo.*
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


clearStage Tasks
^^^^^^^^^^^^^^^^

As you can see in the picture above there are a couple of *clearStage* tasks before the actual import Task. They are created for full imports and will reset whatever stage the previous import was in. This will force the Task to start from the beginning. As it's a full import, there is no risk of losing data as we read everything anyway. And that's the reason why there are no *clearStage* task before the incremental imports. If the previous incremental import failed, we might be in a position where we read the data from the source but unable to insert it into Hive. Forcing a restart of the stage in that situation might result in loss of data.

start and stop Tasks
^^^^^^^^^^^^^^^^^^^^

The *start* and *stop* task is always there. Stop is just a dummy Task that is not doing anything. But it's very useful if another DAG wants a sensor so it continues after this DAG is completed. As stop is always at the end, it's safe (and usually default) to look at the status of the *stop* task.

The *start* task is not a dummy Task. It's actually running the following command at the start of the DAG::

./manage --checkAirflowExecution

This will check if the *airflow_disable* configuration is set to 1 or not. If it's set to 1, the *start* task will fail and the rest of the Tasks will not run. This is one of the options the operators have to temporary disable all DBImport executions. Useful during for example upgrade of DBImport or system components in order to make sure that no imports are starting.

Run Import and ETL separate
---------------------------

In the *airflow_import_dags* there is a column called *run_import_and_etl_separate*. If this is set to 1 and the DAG is regenerated, the result will look something like this.

.. image:: img/airflow_import_etl_separate.jpg

As you can see, the DBImport task is now split into two. One for the actuall import where the communication with the source system is taking place and one for ETL work where the Hive operations are happening. As the Import and ETL Tasks are running as default in separate pools, this is useful in order to minimize the number of sessions against the source system but at the same time have a larger number of sessions in Hive.


Finish all Imports first
------------------------

In the *airflow_import_dags* there is a column called *finish_all_stage1_first*. If this is set to 1 and the DAG is regenerated, the result will look something like this.

.. note:: In the Legacy version of DBImport the *Import* and *ETL* phase was called *stage1* and *stage2*. That's why the column is called what it's called. This will most likely change name as soon as the Legacy version is decommissioned.

.. image:: img/airflow_finish_import_first.jpg


