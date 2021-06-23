Source Table Information
========================

It's very common when importing data that you need additional information about the source table. This is usually needed as soon as you will attempt an incremental import or an import that uses merge in some kind. Because you then need to know what indexes exists on the source table. You might also need to know if columns are allowed to have null values. In order to get this information without having to talk the the source table DBA's all the time, DBImport fetches this information and stores it just in order to help the persons doing the imports. The information itself is not used by DBImport during operations.

Table or View
-------------

Sometimes, the table you are importing is not behaiving as you would expect. Most common is that the source system says that they have primary keys and indexes on all tables, but you get no such information when reading the table. Then there is a big chance that you are only allowed to read views and not the tables directy. In order to know this, there is a column in *import_tables* called *sourceTableType* that will contains the table type. 

Indexes
-------

When fetching metadata from the source system, indexes are also read and stored in a table called *import_tables_indexes*. More information on this table can be found on the :doc:`database` page.

