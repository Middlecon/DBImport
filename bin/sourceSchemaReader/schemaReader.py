# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
import re
import logging
import json
import math
from ConfigReader import configuration
import mysql.connector
from common import constants as constant
from mysql.connector import errorcode
from datetime import datetime
import pandas as pd
import jaydebeapi

class source(object):
	def __init__(self):
		logging.debug("Initiating schemaReader.source()")

	def removeNewLine(self, _data):
		if _data == None: 
			return None
		else:
			return _data
		

	def readTableColumns(self, JDBCCursor, serverType = None, database = None, schema = None, table = None):
		logging.debug("Executing schemaReader.readTableColumns()")
		query = None
		result_df = pd.DataFrame()

		if serverType == constant.MSSQL:
			query  = "select "
			query += "	SchemaName = CAST((TBL.TABLE_SCHEMA) AS NVARCHAR(4000)), "
			query += "	TableName =  CAST((TBL.TABLE_NAME) AS NVARCHAR(4000)), "
			query += "	TableDescription = CAST((tableProp.value) AS NVARCHAR(4000)), "
			query += "	ColumnName = CAST((COL.COLUMN_NAME) AS NVARCHAR(4000)), "
			query += "	ColumnDataType = CAST((COL.DATA_TYPE) AS NVARCHAR(4000)), " 
			query += "	ColumnLength = COL.CHARACTER_MAXIMUM_LENGTH, "
			query += "	ColumnDescription = CAST((colDesc.ColumnDescription) AS NVARCHAR(4000)), " 
			query += "	ColumnPrecision = CAST((COL.numeric_precision) AS NVARCHAR(128)), "
			query += "	ColumnScale = COL.numeric_scale, "
			query += "	IsNullable =  CAST((COL.Is_Nullable) AS NVARCHAR(128)), "
			query += "	TableType =  CAST((TBL.TABLE_TYPE) AS NVARCHAR(4000)), "
			query += "  CreateDate = sysTables.create_date "
			query += "FROM INFORMATION_SCHEMA.TABLES TBL " 
			query += "INNER JOIN INFORMATION_SCHEMA.COLUMNS COL " 
			query += "	ON COL.TABLE_NAME = TBL.TABLE_NAME "
			query += "	AND COL.TABLE_SCHEMA = TBL.TABLE_SCHEMA " 
			query += "LEFT JOIN sys.tables sysTables "
			query += "	ON sysTables.object_id = object_id(TBL.TABLE_SCHEMA + '.' + TBL.TABLE_NAME) " 
			query += "LEFT JOIN sys.extended_properties tableProp "
			query += "	ON tableProp.major_id = object_id(TBL.TABLE_SCHEMA + '.' + TBL.TABLE_NAME) " 
			query += "	AND tableProp.minor_id = 0 "
			query += "	AND tableProp.name = 'MS_Description' " 
			query += "LEFT JOIN ( "
			query += "	SELECT "
			query += "		sc.object_id, " 
			query += "		sc.column_id, "
			query += "		sc.name, "
			query += "		colProp.[value] AS ColumnDescription " 
			query += "	FROM sys.columns sc "
			query += "	INNER JOIN sys.extended_properties colProp " 
			query += "		ON colProp.major_id = sc.object_id "
			query += "		AND colProp.minor_id = sc.column_id "
			query += "		AND colProp.name = 'MS_Description' "
			query += "	) colDesc "
			query += "	ON colDesc.object_id = object_id(TBL.TABLE_SCHEMA + '.' + TBL.TABLE_NAME) " 
			query += "	AND colDesc.name = COL.COLUMN_NAME " 
			query += "WHERE lower(TBL.TABLE_TYPE) in ('base table','view') "
			query += "	AND COL.TABLE_SCHEMA = '%s' "%(schema)
			if table != None:
				query += "	AND COL.TABLE_NAME = '%s' "%(table)
			query += "ORDER BY TBL.TABLE_SCHEMA, TBL.TABLE_NAME,COL.ordinal_position"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = self.removeNewLine(row[0])
					line_dict["TABLE_NAME"] = self.removeNewLine(row[1])
				if row[2] == "" or row[2] == None:
					line_dict["TABLE_COMMENT"] = None
				else:
					line_dict["TABLE_COMMENT"] = self.removeNewLine(row[2]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
				line_dict["SOURCE_COLUMN_NAME"] = self.removeNewLine(row[3])

				if row[4] in ("numeric", "decimal"):
					if row[5] == None:
						line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s,%s)"%(row[4],row[7], row[8] )
					else:
						line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(row[4], row[5])

				elif row[4] in ("geometry", "image", "ntext", "text", "xml"):
					line_dict["SOURCE_COLUMN_TYPE"] = "%s"%(row[4])

				elif row[4] == "varbinary":
					if row[7] != None and row[7] > -1:
						line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s,%s)"%(row[4],row[7], row[8] )
					else:
						line_dict["SOURCE_COLUMN_TYPE"] = row[4]
				else:
					if row[5] == None:
						line_dict["SOURCE_COLUMN_TYPE"] = row[4]
					else:
						line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(row[4], row[5])

				line_dict["SOURCE_COLUMN_LENGTH"] = row[5]

				if row[6] == "" or row[6] == None:
					line_dict["SOURCE_COLUMN_COMMENT"] = None
				else:
					line_dict["SOURCE_COLUMN_COMMENT"] = self.removeNewLine(row[6]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
				line_dict["IS_NULLABLE"] = row[9]

				line_dict["TABLE_TYPE"] = row[10]

				try:
					line_dict["TABLE_CREATE_TIME"] = datetime.strptime(row[11], '%Y-%m-%d %H:%M:%S.%f')
				except:
					line_dict["TABLE_CREATE_TIME"] = None

				line_dict["DEFAULT_VALUE"] = None
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.ORACLE:
			# First determine if column ORIGIN_CON_ID exists in ALL_TAB_COMMENTS. If it does, we need to take that into consideration
			oracle_OriginConId_exists = True
			oracle_OriginConId = None
			# query = "SELECT ORIGIN_CON_ID FROM ALL_TAB_COMMENTS WHERE 1 = 0"
			query = "SELECT ORIGIN_CON_ID FROM ALL_TAB_COMMENTS "
			query += "WHERE OWNER = '%s' "%(schema)
			if table != None:
				query += "  AND TABLE_NAME = '%s' "%(table)
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				if "invalid identifier" in str(errMsg):
					oracle_OriginConId_exists = False
				else:
					logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
					return result_df

			if oracle_OriginConId_exists == True:
				rowCount = 0
				for row in JDBCCursor.fetchall():
					oracle_OriginConId = row[0]
					rowCount += 1

				if rowCount != 1:
					# If there are more than one originConId, it's impossible to determine what we will use. So then we go to default
					oracle_OriginConId = None

			query  = "SELECT "
			query += "  ALL_TAB_COLUMNS.OWNER SCHEMA_NAME, "
			query += "  ALL_TAB_COLUMNS.TABLE_NAME, "
			query += "  ALL_TAB_COMMENTS.COMMENTS TABLE_COMMENT, " 
			query += "  ALL_TAB_COLUMNS.COLUMN_NAME, "
			query += "  ALL_TAB_COLUMNS.DATA_TYPE, "
			query += "  ALL_TAB_COLUMNS.DATA_LENGTH, "
			query += "  ALL_COL_COMMENTS.COMMENTS COLUMN_COMMENT, "
			query += "  ALL_TAB_COLUMNS.CHAR_LENGTH, "
			query += "  ALL_TAB_COLUMNS.DATA_PRECISION, " 
			query += "  ALL_TAB_COLUMNS.DATA_SCALE, " 
			query += "  ALL_TAB_COLUMNS.NULLABLE, " 
			query += "  ALL_OBJECTS.OBJECT_TYPE, " 
			query += "  ALL_OBJECTS.CREATED " 
			query += "FROM ALL_TAB_COLUMNS ALL_TAB_COLUMNS " 
			query += "LEFT JOIN ALL_TAB_COMMENTS ALL_TAB_COMMENTS " 
			query += "  ON  ALL_TAB_COLUMNS.OWNER = ALL_TAB_COMMENTS.OWNER " 
			query += "  AND ALL_TAB_COLUMNS.TABLE_NAME = ALL_TAB_COMMENTS.TABLE_NAME " 
			if oracle_OriginConId_exists == True:
				if oracle_OriginConId == None:
					query += "  AND ALL_TAB_COMMENTS.ORIGIN_CON_ID <= 1 "
				else:
					query += "  AND ALL_TAB_COMMENTS.ORIGIN_CON_ID = %s "%(oracle_OriginConId)
			query += "LEFT JOIN ALL_COL_COMMENTS ALL_COL_COMMENTS " 
			query += "  ON  ALL_TAB_COLUMNS.OWNER = ALL_COL_COMMENTS.OWNER " 
			query += "  AND ALL_TAB_COLUMNS.TABLE_NAME = ALL_COL_COMMENTS.TABLE_NAME " 
			query += "  AND ALL_TAB_COLUMNS.COLUMN_NAME = ALL_COL_COMMENTS.COLUMN_NAME " 
			if oracle_OriginConId_exists == True:
				if oracle_OriginConId == None:
					query += "  AND ALL_COL_COMMENTS.ORIGIN_CON_ID <= 1 "
				else:
					query += "  AND ALL_COL_COMMENTS.ORIGIN_CON_ID = %s "%(oracle_OriginConId)
			query += "LEFT JOIN ALL_OBJECTS ALL_OBJECTS " 
			query += "  ON  ALL_TAB_COLUMNS.OWNER = ALL_OBJECTS.OWNER " 
			query += "  AND ALL_TAB_COLUMNS.TABLE_NAME = ALL_OBJECTS.OBJECT_NAME " 
			query += "  AND ALL_OBJECTS.OBJECT_TYPE IN ('TABLE', 'VIEW') " 
			query += "WHERE ALL_TAB_COLUMNS.OWNER = '%s' "%(schema)
			if table != None:
				query += "  AND ALL_TAB_COLUMNS.TABLE_NAME = '%s' "%(table)
			query += "ORDER BY SCHEMA_NAME, ALL_TAB_COLUMNS.TABLE_NAME, ALL_TAB_COLUMNS.COLUMN_ID"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = self.removeNewLine(row[0])
					line_dict["TABLE_NAME"] = self.removeNewLine(row[1])
				if row[2] == "" or row[2] == None:
					line_dict["TABLE_COMMENT"] = None
				else:
					line_dict["TABLE_COMMENT"] = self.removeNewLine(row[2]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
				line_dict["SOURCE_COLUMN_NAME"] = self.removeNewLine(row[3])

				if row[5] == None:
					line_dict["SOURCE_COLUMN_TYPE"] = row[4]
				else:
					if re.search('TIMESTAMP', row[4]) or row[4] in ("CLOB", "DATE", "LONG", "BLOB", "NCLOB", "LONG RAW"):
						line_dict["SOURCE_COLUMN_TYPE"] = row[4]
					elif row[4] in ("VARCHAR", "VARCHAR2", "CHAR", "NCHAR", "NVARCHAR2"):
						line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(row[4], int(row[7]))
					elif row[4] in ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
						if row[8] == None:
							line_dict["SOURCE_COLUMN_TYPE"] = row[4]
						elif row[8] == 0:	#("DATA_PRECISION") == 0) then use char_length
							line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(row[4], int(row[7]))
						elif row[9]== None or row[9] == 0:
							line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(row[4], int(row[8]))
						else:
							line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s,%s)"%(row[4], int(row[8]), int(row[9]))
					else:
						line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(row[4], int(row[5]))

				line_dict["SOURCE_COLUMN_LENGTH"] = row[5]

				if row[6] == "" or row[6] == None:
					line_dict["SOURCE_COLUMN_COMMENT"] = None
				else:
					line_dict["SOURCE_COLUMN_COMMENT"] = self.removeNewLine(row[6]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
				line_dict["IS_NULLABLE"] = row[10]

				line_dict["TABLE_TYPE"] = row[11]

				try:
					line_dict["TABLE_CREATE_TIME"] = datetime.strptime(row[12], '%Y-%m-%d %H:%M:%S')
				except:
					line_dict["TABLE_CREATE_TIME"] = None

				line_dict["DEFAULT_VALUE"] = None
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)


		if serverType == constant.MYSQL:
			query =  "select "
			query += "   c.table_schema as table_schema, "
			query += "   c.table_name, "
			query += "   t.table_comment, " 
			query += "   c.column_name, "
			query += "   c.data_type, "
			query += "   c.character_maximum_length, "
			query += "   c.column_comment, "
			query += "   c.is_nullable, " 
			query += "   c.numeric_precision, " 
			query += "   c.numeric_scale, " 
			query += "   t.table_type, " 
			query += "   t.create_time " 
			query += "from information_schema.columns c "
			query += "left join information_schema.tables t " 
			query += "   on c.table_schema = t.table_schema and c.table_name = t.table_name "
			query += "where c.table_schema = '%s' "%(database)
			if table != None:
				query += "   and c.table_name = '%s' "%(table)
			query += "order by c.table_schema,c.table_name, c.ordinal_position "

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = self.removeNewLine(row[0])
					line_dict["TABLE_NAME"] = self.removeNewLine(row[1])

				if row[2] == "" or row[2] == None:
					line_dict["TABLE_COMMENT"] = None
				else:
					line_dict["TABLE_COMMENT"] = self.removeNewLine(row[2]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
				line_dict["SOURCE_COLUMN_NAME"] = self.removeNewLine(row[3])

				if row[4] == "decimal":
					line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s,%s)"%(self.removeNewLine(row[4]), row[8], row[9])
				elif row[5] == None:
					line_dict["SOURCE_COLUMN_TYPE"] = self.removeNewLine(row[4])
				else:
					line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(self.removeNewLine(row[4]), row[5])

				line_dict["SOURCE_COLUMN_LENGTH"] = row[5]

				if row[6] == None or row[6] == "":
					line_dict["SOURCE_COLUMN_COMMENT"] = None
				else:
					line_dict["SOURCE_COLUMN_COMMENT"] = self.removeNewLine(row[6]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
				line_dict["IS_NULLABLE"] = row[7]

				line_dict["TABLE_TYPE"] = row[10]

				try:
					line_dict["TABLE_CREATE_TIME"] = datetime.strptime(row[11], '%Y-%m-%d %H:%M:%S')
				except:
					line_dict["TABLE_CREATE_TIME"] = None

				line_dict["DEFAULT_VALUE"] = None
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.DB2_UDB:

			query  = "SELECT "
			query += "	TRIM(ST.CREATOR) as SCHEMA_NAME, "
			query += "	TRIM(ST.NAME) as TABLE_NAME, "
			query += "	TRIM(ST.REMARKS) as TABLE_COMMENT, "
			query += "	TRIM(SC.NAME) as SOURCE_COLUMN_NAME, " 
			query += "	TRIM(SC.COLTYPE) SOURCE_COLUMN_TYPE, "
			query += "	SC.LENGTH as SOURCE_COLUMN_LENGTH, "
			query += "	SC.SCALE as SOURCE_COLUMN_SCALE, " 
			query += "	TRIM(SC.REMARKS) as SOURCE_COLUMN_COMMENT, "
			query += "	SC.NULLS as IS_NULLABLE, "
			query += "	ST.TYPE as TABLE_TYPE, " 
			query += "	ST.CTIME as CREATE_TIME "
			query += "FROM SYSIBM.SYSTABLES ST "
			query += "LEFT JOIN SYSIBM.SYSCOLUMNS SC " 
			query += "	ON ST.NAME = SC.TBNAME "
			query += "	AND ST.CREATOR = SC.TBCREATOR "
			query += "WHERE "
			query += "	ST.CREATOR = '%s' "%(schema)
			if table != None:
				query += "	AND ST.NAME = '%s' "%(table)
			query += "ORDER BY ST.CREATOR, ST.NAME"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df
				

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = self.removeNewLine(row[0])
					line_dict["TABLE_NAME"] = self.removeNewLine(row[1])
				if row[2] == "" or row[2] == None:
					line_dict["TABLE_COMMENT"] = None
				else:
					line_dict["TABLE_COMMENT"] = self.removeNewLine(row[2]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
				line_dict["SOURCE_COLUMN_NAME"] = self.removeNewLine(row[3])

				if row[4] == "DECIMAL":
					line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s,%s)"%(row[4], row[5], row[6])
				elif row[4] in ("DOUBLE", "REAL", "SMALLINT", "DATE", "BLOB", "INTEGER", "TIMESTMP", "BIGINT", "CLOB"):
					line_dict["SOURCE_COLUMN_TYPE"] = self.removeNewLine(row[4])
				else:
					line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(self.removeNewLine(row[4]), row[5])

				line_dict["SOURCE_COLUMN_LENGTH"] = row[5]

				if row[7] == ""  or row[7] == None:
					line_dict["SOURCE_COLUMN_COMMENT"] = None
				else:
					line_dict["SOURCE_COLUMN_COMMENT"] = self.removeNewLine(row[7]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')

				line_dict["IS_NULLABLE"] = row[8]

				line_dict["TABLE_TYPE"] = row[9]

				try:
					line_dict["TABLE_CREATE_TIME"] = datetime.strptime(row[10], '%Y-%m-%d %H:%M:%S.%f')
				except:
					line_dict["TABLE_CREATE_TIME"] = None

				line_dict["DEFAULT_VALUE"] = None
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.DB2_AS400:
			query  = "SELECT "
			query += "	TRIM(ST.TABLE_SCHEMA) as SCHEMA_NAME, "
			query += "	TRIM(ST.TABLE_NAME) as TABLE_NAME, "
			query += "	ST.LONG_COMMENT as TABLE_COMMENT, "
			query += "	TRIM(SC.COLUMN_NAME) as SOURCE_COLUMN_NAME, " 
			query += "	SC.TYPE_NAME as SOURCE_COLUMN_TYPE, "
			query += "	SC.COLUMN_SIZE as SOURCE_COLUMN_LENGTH, "
			query += "	SC.DECIMAL_DIGITS as SOURCE_COLUMN_SCALE, " 
			query += "	SC.REMARKS as SOURCE_COLUMN_COMMENT, "
			query += "	SC.IS_NULLABLE, "
			query += "	ST.TABLE_TYPE, "
						# ST.LAST_ALTERED_TIMESTAMP is not really correct, but it's the best we got
						# https://www.ibm.com/support/knowledgecenter/SSAE4W_9.6.0/db2/rbafzcatsystbls.htm
			query += "	ST.LAST_ALTERED_TIMESTAMP "
			query += "FROM QSYS2.SYSTABLES ST "
			query += "LEFT JOIN SYSIBM.SQLCOLUMNS SC " 
			query += "	ON ST.TABLE_SCHEMA = SC.TABLE_SCHEM "
			query += "	AND ST.TABLE_NAME= SC.TABLE_NAME "
			query += "WHERE "
			query += "	ST.TABLE_SCHEMA = '%s' "%(schema)
			if table != None:
				query += "	AND SC.TABLE_NAME = '%s' "%(table)
			query += "ORDER BY ST.TABLE_SCHEMA, SC.TABLE_NAME, SC.ORDINAL_POSITION"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = self.removeNewLine(row[0])
					line_dict["TABLE_NAME"] = self.removeNewLine(row[1])

				if row[2] == "" or row[2] == None:
					line_dict["TABLE_COMMENT"] = None
				else:
					line_dict["TABLE_COMMENT"] = self.removeNewLine(row[2]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
				line_dict["SOURCE_COLUMN_NAME"] = self.removeNewLine(row[3])

				if row[4] == "DECIMAL":
					line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s,%s)"%(row[4], row[5], row[6])
				elif row[4] in ("DOUBLE", "REAL", "SMALLINT", "DATE", "BLOB", "INTEGER", "TIMESTMP", "BIGINT", "CLOB"):
					line_dict["SOURCE_COLUMN_TYPE"] = self.removeNewLine(row[4])
				else:
					line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(self.removeNewLine(row[4]), row[5])

				line_dict["SOURCE_COLUMN_LENGTH"] = row[5]

				if self.removeNewLine(row[7]) == "" or row[7] == None:
					line_dict["SOURCE_COLUMN_COMMENT"] = None
				else:
					line_dict["SOURCE_COLUMN_COMMENT"] = self.removeNewLine(row[7]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')

				line_dict["IS_NULLABLE"] = row[8]

				line_dict["TABLE_TYPE"] = row[9]

				try:
					line_dict["TABLE_CREATE_TIME"] = datetime.strptime(row[10], '%Y-%m-%d %H:%M:%S.%f')
				except:
					line_dict["TABLE_CREATE_TIME"] = None

				line_dict["DEFAULT_VALUE"] = None
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.POSTGRESQL:
			query  = "SELECT "
			query += "	tab_columns.table_schema, "
			query += "	tab_columns.table_name, "
			query += "	pg_catalog.col_description(c.oid, 0::int) as table_comment, "
			query += "	tab_columns.column_name, "
			query += "	data_type, "
			query += "	character_maximum_length, " 
			query += "	pg_catalog.col_description(c.oid, tab_columns.ordinal_position::int) as column_comment, "
			query += "	is_nullable, " 
			query += "	tab_tables.table_type " 
			query += "FROM information_schema.columns AS tab_columns " 
			query += "LEFT JOIN pg_catalog.pg_class c "
			query += "	ON c.relname = tab_columns.table_name "
			query += "LEFT JOIN information_schema.tables AS tab_tables "
			query += "	ON tab_tables.table_catalog = tab_columns.table_catalog "
			query += "	AND tab_tables.table_schema = tab_columns.table_schema "
			query += "	AND tab_tables.table_name = tab_columns.table_name "
			query += "WHERE tab_columns.table_catalog = '%s' "%(database)
			query += "	AND tab_columns.table_schema ='%s' "%(schema)
			if table != None:
				query += "	AND tab_columns.table_name = '%s' "%(table)
			query += "ORDER BY table_schema, table_name"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = self.removeNewLine(row[0])
					line_dict["TABLE_NAME"] = self.removeNewLine(row[1])

				if row[2] == "" or row[2] == None:
					line_dict["TABLE_COMMENT"] = None
				else:
					line_dict["TABLE_COMMENT"] = self.removeNewLine(row[2]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
				line_dict["SOURCE_COLUMN_NAME"] = self.removeNewLine(row[3])

				if row[5] == None:
					line_dict["SOURCE_COLUMN_TYPE"] = self.removeNewLine(row[4])
				else:
					line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(self.removeNewLine(row[4]), row[5])

				line_dict["SOURCE_COLUMN_LENGTH"] = row[5]

				if row[6] == "" or row[6] == None:
					line_dict["SOURCE_COLUMN_COMMENT"] = None
				else:
					line_dict["SOURCE_COLUMN_COMMENT"] = self.removeNewLine(row[6]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')

				line_dict["IS_NULLABLE"] = row[7]

				line_dict["TABLE_TYPE"] = row[8]
				line_dict["TABLE_CREATE_TIME"] = None
				line_dict["DEFAULT_VALUE"] = None
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.PROGRESS:
			query  = "SELECT "
			query += "	tab_tables.OWNER, "
			query += "	tab_tables.TBL, "
			query += "	tab_tables.DESCRIPTION AS TBL_Commnets, "
			query += "	COL, "
			query += "	COLTYPE, "
			query += "	WIDTH, "
			query += "	SCALE, "
			query += "	tab_columns.DESCRIPTION, "
			query += "	tab_columns.NULLFLAG, "
			query += "	tab_tables.TBLTYPE "
			query += "FROM sysprogress.SYSCOLUMNS_FULL tab_columns "
			query += "LEFT JOIN SYSPROGRESS.SYSTABLES_FULL tab_tables " 
			query += "	ON tab_tables.TBL = tab_columns.TBL "
			query += "	AND tab_tables.OWNER = tab_columns.OWNER  "
			query += "WHERE "
			query += "	tab_columns.OWNER = '%s' "%(schema)
			if table != None:
				query += "	AND tab_columns.TBL = '%s' "%(table)
			query += "ORDER BY tab_tables.OWNER, tab_tables.TBL"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = self.removeNewLine(row[0])
					line_dict["TABLE_NAME"] = self.removeNewLine(row[1])

				if row[2] == "" or row[2] == None:
					line_dict["TABLE_COMMENT"] = None
				else:
					line_dict["TABLE_COMMENT"] = self.removeNewLine(row[2]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
				line_dict["SOURCE_COLUMN_NAME"] = self.removeNewLine(row[3])

				if row[4] in ("decimal", "numeric"):
					if row[5] == None:
						line_dict["SOURCE_COLUMN_TYPE"] = "%s"%(row[4])
					else:
						line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s,%s)"%(row[4], row[5], row[6])
				else:
					if row[5] == None:
						line_dict["SOURCE_COLUMN_TYPE"] = "%s"%(row[4])
					else:
						line_dict["SOURCE_COLUMN_TYPE"] = "%s(%s)"%(row[4], row[5])

				line_dict["SOURCE_COLUMN_LENGTH"] = row[5]

				if self.removeNewLine(row[7]) == "" or row[7] == None:
					line_dict["SOURCE_COLUMN_COMMENT"] = None
				else:
					try:
						line_dict["SOURCE_COLUMN_COMMENT"] = self.removeNewLine(row[7]).encode('ascii', 'ignore').decode('unicode_escape', 'ignore')
					except UnicodeDecodeError:
						line_dict["SOURCE_COLUMN_COMMENT"] = self.removeNewLine(row[7])

				line_dict["IS_NULLABLE"] = row[8]

				line_dict["TABLE_TYPE"] = row[9]
				line_dict["TABLE_CREATE_TIME"] = None
				line_dict["DEFAULT_VALUE"] = None
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		logging.debug(result_df)
		logging.debug("Executing schemaReader.readTable() - Finished")
		return result_df

	def readTableKeys(self, JDBCCursor, serverType = None, database = None, schema = None, table = None):
		logging.debug("Executing schemaReader.readTableKeys()")
		query = None
		result_df = pd.DataFrame()

		if serverType == constant.MSSQL:
			query  = "SELECT "
			query += "	CAST(oParentColDtl.TABLE_SCHEMA AS VARCHAR(4000)) as SCHEMA_NAME, " 
			query += "	CAST(PKnUTable.name AS VARCHAR(4000)) as TABLE_NAME, " 
			query += "	CAST(PKnUKEY.name AS VARCHAR(4000)) as CONSTRAINT_NAME, " 
#			query += "	CAST(PKnUKEY.type_desc AS VARCHAR(4000)) as CONSTRAINT_TYPE, " 
			query += "  '%s' AS CONSTRAINT_TYPE, "%(constant.PRIMARY_KEY)
			query += "	CAST(PKnUKEYCol.name AS VARCHAR(4000)) as COL_NAME, " 
			query += "	oParentColDtl.DATA_TYPE as COL_DATA_TYPE, " 
			query += "	oParentColDtl.CHARACTER_MAXIMUM_LENGTH as COL_LENGTH, "
			query += "	'' as REFERENCE_SCHEMA_NAME, " 
			query += "	'' as REFERENCE_TABLE_NAME, " 
			query += "	'' as REFERENCE_COL_NAME, " 
			query += "	PKnUColIdx.key_ordinal as ORDINAL_POSITION " 
			query += "FROM sys.key_constraints as PKnUKEY " 
			query += "INNER JOIN sys.tables as PKnUTable " 
			query += "	ON PKnUTable.object_id = PKnUKEY.parent_object_id " 
			query += "INNER JOIN sys.index_columns as PKnUColIdx " 
			query += "	ON PKnUColIdx.object_id = PKnUTable.object_id " 
			query += "	AND PKnUColIdx.index_id = PKnUKEY.unique_index_id " 
			query += "INNER JOIN sys.columns as PKnUKEYCol " 
			query += "	ON PKnUKEYCol.object_id = PKnUTable.object_id " 
			query += "	AND PKnUKEYCol.column_id = PKnUColIdx.column_id " 
			query += "INNER JOIN INFORMATION_SCHEMA.COLUMNS oParentColDtl " 
			query += "	ON oParentColDtl.TABLE_NAME=PKnUTable.name " 
			query += "	AND oParentColDtl.COLUMN_NAME=PKnUKEYCol.name " 
			query += "WHERE oParentColDtl.TABLE_SCHEMA = '%s' "%(schema)
			if table != None:
				query += "	and PKnUTable.name = '%s' "%(table)
			query += "	and PKnUKEY.type_desc = 'PRIMARY_KEY_CONSTRAINT' "

			query += "UNION ALL " 

			query += "SELECT "
			query += "	CAST(oParentColDtl.TABLE_SCHEMA AS VARCHAR(4000)) as SCHEMA_NAME, " 
			query += "	CAST(oParent.name AS VARCHAR(4000)) as TABLE_NAME, " 
			query += "	CAST(oConstraint.name AS VARCHAR(4000)) as CONSTRAINT_NAME, " 
#			query += "	CONSTRAINT_TYPE = 'FK', " 
			query += "  '%s' AS CONSTRAINT_TYPE, "%(constant.FOREIGN_KEY)
			query += "	CAST(oParentCol.name AS VARCHAR(4000)) as COL_NAME, " 
			query += "	oParentColDtl.DATA_TYPE as COL_NAME_DATA_TYPE, " 
			query += "	oParentColDtl.CHARACTER_MAXIMUM_LENGTH as COL_LENGTH, " 
			query += "	CAST(OBJECT_SCHEMA_NAME(T.[object_id],DB_ID()) AS VARCHAR(4000)) as REFERENCE_SCHEMA_NAME, " 
			query += "	CAST(oReference.name AS VARCHAR(4000)) as REFERENCE_TABLE_NAME, " 
			query += "	CAST(oReferenceCol.name AS VARCHAR(4000)) as REFERENCE_COL_NAME, " 
			query += "	'' as ORDINAL_POSITION "
			query += "FROM sys.foreign_key_columns FKC " 
			query += "INNER JOIN sys.sysobjects oConstraint " 
			query += "	ON FKC.constraint_object_id=oConstraint.id " 
			query += "INNER JOIN sys.sysobjects oParent " 
			query += "	ON FKC.parent_object_id=oParent.id " 
			query += "INNER JOIN sys.all_columns oParentCol " 
			query += "	ON FKC.parent_object_id=oParentCol.object_id " 
			query += "	AND FKC.parent_column_id=oParentCol.column_id " 
			query += "INNER JOIN sys.sysobjects oReference " 
			query += "	ON FKC.referenced_object_id=oReference.id " 
			query += "INNER JOIN INFORMATION_SCHEMA.COLUMNS oParentColDtl " 
			query += "	ON oParentColDtl.TABLE_NAME=oParent.name " 
			query += "	AND oParentColDtl.COLUMN_NAME=oParentCol.name " 
			query += "INNER JOIN sys.all_columns oReferenceCol " 
			query += "	ON FKC.referenced_object_id=oReferenceCol.object_id " 
			query += "	AND FKC.referenced_column_id=oReferenceCol.column_id " 
			query += "INNER JOIN  sys.[tables] AS T  ON T.[object_id] = oReferenceCol.[object_id] "
			query += "WHERE oParentColDtl.TABLE_SCHEMA = '%s' "%(schema)
			if table != None:
				query += "	and oParent.name = '%s' "%(table)
			query += "ORDER BY SCHEMA_NAME, TABLE_NAME, CONSTRAINT_TYPE, ORDINAL_POSITION"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = row[0]
					line_dict["TABLE_NAME"] = row[1]
				line_dict["CONSTRAINT_NAME"] = row[2]
				line_dict["CONSTRAINT_TYPE"] = row[3]
				line_dict["COL_NAME"] = row[4]
#				line_dict["COL_DATA_TYPE"] = line.split('|')[5]
				line_dict["REFERENCE_SCHEMA_NAME"] = row[7]
				line_dict["REFERENCE_TABLE_NAME"] = row[8]
				line_dict["REFERENCE_COL_NAME"] = row[9]
				line_dict["COL_KEY_POSITION"] = row[10]
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

	
		if serverType == constant.ORACLE:
			query  = "SELECT "
			query += "  DISTINCT  CAST (acc.OWNER AS VARCHAR(4000)) AS SCHEMA_NAME, "
			query += "  CAST (acc.TABLE_NAME AS VARCHAR(4000)) AS  TABLE_NAME, " 
			query += "  CAST(ac.CONSTRAINT_NAME AS VARCHAR(4000)) AS CONSTRAINT_NAME, " 
			query += "  '%s' AS CONSTRAINT_TYPE, "%(constant.PRIMARY_KEY)
			query += "  CAST ( acc.COLUMN_NAME AS VARCHAR(4000)) AS COL_NAME, " 
			query += "  CAST(atc.data_type AS VARCHAR(4000)) AS COL_NAME_DATA_TYPE, "
			query += "  atc.DATA_LENGTH, " 
			query += "  '' AS REFERENCE_OWNER_NAME, " 
			query += "  '' AS REFERENCE_TABLE_NAME, "
			query += "  '' AS REFERENCE_COL_NAME, "
			query += "  acc.POSITION AS COL_KEY_POSITION, "
			query += "  atc.DATA_PRECISION, "
			query += "  atc.CHAR_LENGTH "
			query += "FROM ALL_CONSTRAINTS ac " 
			query += "JOIN ALL_CONS_COLUMNS acc " 
			query += "  ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME " 
			query += "JOIN all_tab_cols atc "
			query += "  ON ac.owner = atc.owner "
			query += "  AND ac.table_name = atc.TABLE_NAME "
			query += "  AND acc.COLUMN_NAME = atc.COLUMN_NAME "
			query += "WHERE ac.CONSTRAINT_TYPE = 'P' "
			query += "  AND acc.OWNER = '%s' "%(schema)
			if table != None:
				query += "  AND acc.TABLE_NAME = '%s' "%(table)
			query += "UNION ALL " 
			query += "select "
			query += "  b.owner AS SCHEMA_NAME, " 
			query += "  b.table_name AS  TABLE_NAME, " 
			query += "  a.constraint_name AS CONSTRAINT_NAME, " 
			query += "  '%s' AS CONSTRAINT_TYPE, "%(constant.FOREIGN_KEY)
			query += "  b.column_name AS COL_NAME , " 
			query += "  atc.data_type AS COL_NAME_DATA_TYPE, " 
			query += "  atc.DATA_LENGTH, " 
			query += "  c.owner AS REFERENCE_SCHEMA_NAME, " 
			query += "  c.table_name AS REFERENCE_TABLE_NAME, " 
			query += "  c.column_name AS REFERENCE_COL_NAME, " 
			query += "  b.position AS COL_KEY_POSITION, " 
			query += "  atc.DATA_PRECISION, "
			query += "  atc.CHAR_LENGTH  " 
			query += "from all_cons_columns b "
			query += "left join all_cons_columns c " 
			query += "  on b.position = c.position "
			query += "left join all_constraints a "
			query += "  on b.constraint_name = a.constraint_name "
			query += "  AND a.owner = b.owner "
			query += "  AND c.constraint_name = a.r_constraint_name "
			query += "  AND c.owner = a.r_owner "
			query += "left join all_tab_cols atc "
			query += "  on b.owner = atc.owner "
			query += "  AND b.table_name = atc.table_name "
			query += "  AND b.column_name = atc.column_name "
			query += "where "
			query += "  a.constraint_type = 'R' "
			query += "  AND b.OWNER = '%s' "%(schema)
			if table != None:
				query += "  AND b.TABLE_NAME = '%s' "%(table)
			query += "ORDER BY SCHEMA_NAME, TABLE_NAME,CONSTRAINT_TYPE,CONSTRAINT_NAME,COL_KEY_POSITION"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = row[0]
					line_dict["TABLE_NAME"] = row[1]
				line_dict["CONSTRAINT_NAME"] = row[2]
				line_dict["CONSTRAINT_TYPE"] = row[3]
				line_dict["COL_NAME"] = row[4]
#				line_dict["COL_DATA_TYPE"] = line.split('|')[5]
				line_dict["REFERENCE_SCHEMA_NAME"] = row[7]
				line_dict["REFERENCE_TABLE_NAME"] = row[8]
				line_dict["REFERENCE_COL_NAME"] = row[9]
				line_dict["COL_KEY_POSITION"] = int(row[10])
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.MYSQL:
			query  = "SELECT kcu.CONSTRAINT_SCHEMA AS SCHEMA_NAME, "
			query += "	kcu.table_name AS TABLE_NAME, "
			query += "	kcu.constraint_name AS CONSTRAINT_NAME, "
			query += "	'%s' AS CONSTRAINT_TYPE, "%(constant.PRIMARY_KEY)
			query += "	kcu.column_name AS COL_NAME, "
			query += "	cols.data_type AS COL_DATA_TYPE, " 
			query += "	cols.character_maximum_length AS COL_MAX_LENGTH, "
			query += "	kcu.referenced_table_schema AS REFERENCE_TABLE_SCHEMA, "
			query += "	kcu.referenced_table_name AS REFERENCE_TABLE_NAME, "
			query += "	kcu.referenced_column_name AS REFERENCE_COL_NAME, "
			query += "	kcu.ORDINAL_POSITION AS COL_KEY_POSITION "
			query += "FROM information_schema.key_column_usage kcu "
			query += "left join information_schema.columns cols "
			query += "	on kcu.table_name = cols.table_name and kcu.column_name = cols.column_name "
			query += "WHERE "
			query += "	kcu.referenced_table_name IS NULL " 
			query += "	AND (CONSTRAINT_NAME='PRIMARY' OR CONSTRAINT_NAME='UNIQUE') "
			query += "	AND kcu.CONSTRAINT_SCHEMA = '%s' "%(database)
			if table != None:
				query += "	AND kcu.table_name = '%s' "%(table)

			query += "UNION "

			query += "SELECT " 
			query += "	kcu.CONSTRAINT_SCHEMA AS SCHEMA_NAME, "
			query += "	kcu.table_name AS TABLE_NAME, "
			query += "	kcu.constraint_name AS CONSTRAINT_NAME, "
			query += "	'%s' AS CONSTRAINT_TYPE, "%(constant.FOREIGN_KEY)
			query += "	kcu.column_name AS COL_NAME, "
			query += "	cols.data_type AS COL_DATA_TYPE, "
			query += "	cols.character_maximum_length AS COL_MAX_LENGTH, "
			query += "	kcu.referenced_table_schema AS REFERENCE_TABLE_SCHEMA, "
			query += "	kcu.referenced_table_name AS REFERENCE_TABLE_NAME, "
			query += "	kcu.referenced_column_name AS REFERENCE_COL_NAME, "
			query += "	kcu.ORDINAL_POSITION AS COL_KEY_POSITION "
			query += "FROM information_schema.key_column_usage kcu "
			query += "left join information_schema.columns cols "
			query += "	on kcu.referenced_table_name = cols.table_name and referenced_column_name = cols.column_name "
			query += "WHERE "
			query += "	kcu.referenced_table_name IS NOT NULL " 
			query += "	AND kcu.CONSTRAINT_SCHEMA = '%s' "%(database)
			if table != None:
				query += "	AND kcu.table_name = '%s' "%(table)
			query += "order by schema_name, table_name, CONSTRAINT_TYPE, COL_KEY_POSITION"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = row[0]
					line_dict["TABLE_NAME"] = row[1]
				line_dict["CONSTRAINT_NAME"] = row[2]
				line_dict["CONSTRAINT_TYPE"] = row[3]
				line_dict["COL_NAME"] = row[4]
#				line_dict["COL_DATA_TYPE"] = line.split('|')[5]
				line_dict["REFERENCE_SCHEMA_NAME"] = row[7]
				line_dict["REFERENCE_TABLE_NAME"] = row[8]
				line_dict["REFERENCE_COL_NAME"] = row[9]
				line_dict["COL_KEY_POSITION"] = row[10]
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.DB2_UDB:
			query  = "select "
			query += "	TRIM(SI.TBCREATOR) as SCHEMA_NAME, " 
			query += "	TRIM(SI.TBNAME) as TABLE_NAME, " 
			query += "	TRIM(SI.NAME) as CONSTRAINT_NAME, " 
			query += "	'%s' AS CONSTRAINT_TYPE, "%(constant.PRIMARY_KEY)
			query += "	TRIM(SC.NAME) as COL_NAME, " 
			query += "	TRIM(SC.COLTYPE) as COL_DATA_TYPE, " 
			query += "	SC.LENGTH as COL_DATA_LENGTH, " 
			query += "	SC.SCALE as COL_DATA_SCALE,  " 
			query += "	'' as REFERENCE_SCHEMA_NAME, " 
			query += "	'' as REFERENCE_TABLE_NAME, " 
			query += "	'' as REFERENCE_COL_NAME, " 
			query += "	SI.COLCOUNT as ORDINAL_POSITION  " 
			query += "FROM SYSIBM.SYSINDEXES SI "
			query += "LEFT JOIN SYSIBM.SYSCOLUMNS SC " 
			query += "	ON SI.TBCREATOR = SC.TBCREATOR "
			query += "	AND SI.TBNAME = SC.TBNAME "
			query += "WHERE "
			query += "	SI.COLNAMES = CONCAT('+',SC.NAME) "
			query += "	AND SI.uniquerule = 'P'"
			query += "	AND SI.TBCREATOR = '%s' "%(schema)
			if table != None:
				query += "	AND SI.TBNAME = '%s' "%(table)

			query += "UNION ALL " 

			query =  "SELECT "
			query += "  TRIM(R.tabschema) as SCHEMA_NAME, "
			query += "  TRIM(R.tabname) as TABLE_NAME, "
			query += "  TRIM(R.constname) as CONSTRAINT_NAME, "
			query += "  'F' AS CONSTRAINT_TYPE, "
			query += "  TRIM(C.COLNAME) as COL_NAME, "
			query += "  SC.COLTYPE as COL_DATA_TYPE, "
			query += "  SC.LENGTH as COL_DATA_LENGTH, "
			query += "  SC.SCALE as COL_DATA_SCALE, "
			query += "  TRIM(R.reftabschema) as REFERENCE_SCHEMA_NAME, "
			query += "  TRIM(R.reftabname) as REFERENCE_TABLE_NAME, "
			query += "  TRIM(Cref.COLNAME) as REFERENCE_COL_NAME, "
			query += "  C.COLSEQ as ORDINAL_POSITION "
			query += "FROM syscat.references R "
			query += "LEFT JOIN syscat.keycoluse C "
			query += "  ON R.constname = C.constname "
			query += "LEFT JOIN syscat.keycoluse Cref "
			query += "  ON R.refkeyname = Cref.constname "
			query += "  AND C.COLSEQ = Cref.COLSEQ "
			query += "LEFT JOIN SYSIBM.SYSCOLUMNS SC "
			query += "  ON R.tabschema = SC.TBCREATOR "
			query += "  AND R.tabname = SC.TBNAME "
			query += "  AND TRIM(SC.NAME)= TRIM(R.FK_COLNAMES) "
			query += "WHERE "
			query += "	R.tabschema = '%s' "%(schema)
			if table != None:
				query += "	AND R.tabname = '%s' "%(table)
			query += "ORDER BY SCHEMA_NAME, TABLE_NAME, CONSTRAINT_TYPE, ORDINAL_POSITION "

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = row[0]
					line_dict["TABLE_NAME"] = row[1]
				line_dict["CONSTRAINT_NAME"] = row[2]
				line_dict["CONSTRAINT_TYPE"] = row[3]
				line_dict["COL_NAME"] = row[4]
#				line_dict["COL_DATA_TYPE"] = line.split('|')[5]
				line_dict["REFERENCE_SCHEMA_NAME"] = row[8]
				line_dict["REFERENCE_TABLE_NAME"] = row[9]
				line_dict["REFERENCE_COL_NAME"] = row[10]
				line_dict["COL_KEY_POSITION"] = int(row[11])
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.DB2_AS400:
			query  = "SELECT "
			query += "	TRIM(SPK.TABLE_SCHEM) as SCHEMA_NAME, "
			query += "	TRIM(SPK.TABLE_NAME) as TABLE_NAME, " 
			query += "	TRIM(SPK.PK_NAME) as CONSTRAINT_NAME, "
			query += "	'%s' AS CONSTRAINT_TYPE, "%(constant.PRIMARY_KEY)
			query += "	TRIM(SC.COLUMN_NAME) as COL_NAME, "
			query += "	SC.TYPE_NAME as COL_DATA_TYPE, "
			query += "	SC.COLUMN_SIZE as COL_DATA_LENGTH, " 
			query += "	SC.DECIMAL_DIGITS as COL_DATA_SCALE, " 
			query += "	'' as REFERENCE_SCHEMA_NAME, "
			query += "	'' as REFERENCE_TABLE_NAME, "
			query += "	'' as REFERENCE_COL_NAME, " 
			query += "	SPK.KEY_SEQ as ORDINAL_POSITION " 
			query += "FROM SYSIBM.SQLPRIMARYKEYS SPK "
			query += "LEFT JOIN SYSIBM.SQLCOLUMNS SC " 
			query += "	ON SPK.TABLE_CAT = SC.TABLE_CAT "
			query += "	AND SPK.TABLE_SCHEM = SC.TABLE_SCHEM " 
			query += "	AND SPK.TABLE_NAME = SC.TABLE_NAME "
			query += "	AND SPK.COLUMN_NAME=SC.COLUMN_NAME "
			query += "WHERE " 
			query += "	SPK.TABLE_SCHEM = '%s' "%(schema)
			if table != None:
				query += "	AND SPK.TABLE_NAME = '%s' "%(table)

			query += "UNION ALL " 

			query += "SELECT "
			query += "	TRIM(SFK.FKTABLE_SCHEM) as SCHEMA_NAME, "
			query += "	TRIM(SFK.FKTABLE_NAME) as TABLE_NAME, " 
			query += "	TRIM(SFK.FK_NAME) as CONSTRAINT_NAME, "
			query += "	'%s' AS CONSTRAINT_TYPE, "%(constant.FOREIGN_KEY)
			query += "	TRIM(SFK.FKCOLUMN_NAME) as COL_NAME, "
			query += "	SC.TYPE_NAME as COL_DATA_TYPE, "
			query += "	SC.COLUMN_SIZE as COL_DATA_LENGTH, " 
			query += "	SC.DECIMAL_DIGITS as COL_DATA_SCALE, " 
			query += "	SFK.PKTABLE_SCHEM  as REFERENCE_SCHEMA_NAME, "
			query += "	SFK.PKTABLE_NAME  as REFERENCE_TABLE_NAME, "
			query += "	SFK.PKCOLUMN_NAME as REFERENCE_COL_NAME, " 
			query += "	SFK.KEY_SEQ as ORDINAL_POSITION " 
			query += "FROM SYSIBM.SQLFOREIGNKEYS SFK "
			query += "LEFT JOIN SYSIBM.SQLCOLUMNS SC " 
			query += "	ON SFK.FKTABLE_CAT = SC.TABLE_CAT "
			query += "	AND SFK.FKTABLE_SCHEM = SC.TABLE_SCHEM " 
			query += "	AND SFK.FKTABLE_NAME = SC.TABLE_NAME "
			query += "	AND SFK.FKCOLUMN_NAME = SC.COLUMN_NAME "
			query += "WHERE " 
			query += "	SFK.FKTABLE_SCHEM = '%s' "%(schema)
			if table != None:
				query += "	AND SFK.FKTABLE_NAME = '%s' "%(table)
			query += "ORDER BY SCHEMA_NAME, TABLE_NAME, CONSTRAINT_TYPE, ORDINAL_POSITION"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				if table == None:
					line_dict["SCHEMA_NAME"] = row[0]
					line_dict["TABLE_NAME"] = row[1]
				line_dict["CONSTRAINT_NAME"] = row[2]
				line_dict["CONSTRAINT_TYPE"] = row[3]
				line_dict["COL_NAME"] = row[4]
#				line_dict["COL_DATA_TYPE"] = line.split('|')[5]
				line_dict["REFERENCE_SCHEMA_NAME"] = row[8]
				line_dict["REFERENCE_TABLE_NAME"] = row[9]
				line_dict["REFERENCE_COL_NAME"] = row[10]
				line_dict["COL_KEY_POSITION"] = int(row[11])
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.POSTGRESQL:
			query  = "SELECT "
			query += "	distinct kcu.constraint_schema AS SCHEMA_NAME, " 
			query += "	kcu.table_name AS TABLE_NAME, " 
			query += "	c.conname AS CONSTRAINT_NAME, " 
			query += "	'%s' AS CONSTRAINT_TYPE, "%(constant.PRIMARY_KEY)
			query += "	CASE WHEN pg_get_constraintdef(c.oid) LIKE 'PRIMARY KEY %' "
			query += "		THEN substring(pg_get_constraintdef(c.oid), 14, position(')' in pg_get_constraintdef(c.oid))-14) "
			query += "	END AS COL_NAME, " 
			query += "	'' AS REFERENCE_SCHEMA_NAME, " 
			query += "	'' AS REFERENCE_TABLE_NAME, " 
			query += "	'' AS REFERENCE_COL_NAME " 
			query += "FROM pg_catalog.pg_constraint c "
			query += "LEFT JOIN information_schema.key_column_usage kcu "
			query += "	ON c.conname = kcu.constraint_name "
			query += "LEFT JOIN information_schema.tables ist " 
			query += "	ON ist.table_schema = kcu.constraint_schema "
			query += "	AND ist.table_name = kcu.table_name "
			query += "WHERE "
			query += "	c.contype = 'p' "
			query += "	AND pg_get_constraintdef(c.oid) LIKE 'PRIMARY KEY %' "
			query += "	AND ist.table_catalog = '%s' "%(database)
			query += "	AND kcu.constraint_schema ='%s' "%(schema)
			if table != None:
				query += "	AND kcu.table_name = '%s' "%(table)

			query += "UNION " 

			query += "SELECT "
			query += "	kcu.constraint_schema AS SCHEMA_NAME, " 
			query += "	kcu.table_name AS TABLE_NAME, " 
			query += "	c.conname AS CONSTRAINT_NAME, " 
			query += "	'%s' AS CONSTRAINT_TYPE, "%(constant.FOREIGN_KEY)
			query += "	CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' "
			query += "		THEN substring(pg_get_constraintdef(c.oid), 14, position(')' in pg_get_constraintdef(c.oid))-14) "
			query += "	END AS COL_NAME, " 
			query += "	'' AS REFERENCE_SCHEMA_NAME," 
			query += "	CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' "
			query += "		THEN substring(pg_get_constraintdef(c.oid), position(' REFERENCES ' in pg_get_constraintdef(c.oid))+12, position('(' in substring(pg_get_constraintdef(c.oid), 14))-position(' REFERENCES ' in pg_get_constraintdef(c.oid))+1) "
			query += "	END AS REFERENCE_TABLE_NAME, " 
			query += "	CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' "
			query += "		THEN substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14, position(')' in substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14))-1) " 
			query += "	END AS REFERENCE_COL_NAME " 
			query += "FROM pg_catalog.pg_constraint c "
			query += "LEFT JOIN information_schema.key_column_usage kcu "
			query += "	ON c.conname = kcu.constraint_name "
			query += "LEFT JOIN information_schema.tables ist " 
			query += "	ON ist.table_schema=kcu.constraint_schema "
			query += "	AND ist.table_name=kcu.table_name "
			query += "WHERE "
			query += "	c.contype = 'f' AND contype IN ('f', 'p') " 
			query += "	AND pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' "
			query += "	AND ist.table_catalog = '%s' "%(database)
			query += "	AND kcu.constraint_schema ='%s' "%(schema)
			if table != None:
				query += "	AND kcu.table_name = '%s' "%(table)
			query += "ORDER BY SCHEMA_NAME, TABLE_NAME,CONSTRAINT_TYPE "

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				schemaName = row[0]
				tableName = row[1]
				constraintName = row[2]
				constraintType = row[3]
				colName = row[4].strip('"')
				refSchemaName = row[5]
				refTableName = row[6].strip('"')
				refColName = row[7].strip('"')
				colKeyPosition = 1

				if constraintType == constant.FOREIGN_KEY:
					if refSchemaName == "" and "." in refTableName:
						refArray = refTableName.split(".")
						refSchemaName = refArray[0]
						refTableName = refArray[1]
	
					if refSchemaName == "":
						refSchemaName = "public"

				colNameList = colName.split(",")
				refColNameList = refColName.split(",")

				for i, column in enumerate(colNameList):
					colName = colNameList[i]
					refColName = refColNameList[i]

					if table == None:
						line_dict["SCHEMA_NAME"] = schemaName
						line_dict["TABLE_NAME"] = tableName
					line_dict["CONSTRAINT_NAME"] = constraintName
					line_dict["CONSTRAINT_TYPE"] = constraintType
					line_dict["COL_NAME"] = colName
#					line_dict["COL_DATA_TYPE"] = line.split('|')[5]
					line_dict["REFERENCE_SCHEMA_NAME"] = refSchemaName
					line_dict["REFERENCE_TABLE_NAME"] = refTableName
					line_dict["REFERENCE_COL_NAME"] = refColName
					line_dict["COL_KEY_POSITION"] = colKeyPosition
					rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		# In some cases, we get duplicate Foreign Keys. This removes all duplicate entries
		result_df.drop_duplicates(keep="first", inplace=True)

		logging.debug(result_df)
		logging.debug("Executing schemaReader.readKeys() - Finished")
		return result_df

	def readTableIndex(self, JDBCCursor, serverType = None, database = None, schema = None, table = None):
		logging.debug("Executing schemaReader.readTableColumns()")
		query = None
		result_df = pd.DataFrame()

		if serverType == constant.MSSQL:
			query = ""
			query += "select i.name,"
			query += "    i.type, "
			query += "    i.is_unique, "
			query += "    col.name, "
			query += "    ic.index_column_id, "
			query += "    col.is_nullable "
			query += "from sys.objects t "
			query += "    inner join sys.indexes i "
			query += "        on t.object_id = i.object_id "
			query += "    inner join sys.index_columns ic "
			query += "        on ic.object_id = t.object_id "
			query += "        and ic.index_id = i.index_id "
			query += "    inner join sys.columns col "
			query += "        on col.object_id = t.object_id "
			query += "        and col.column_id = ic.column_id "
			query += "where schema_name(t.schema_id) = '%s' "%(schema)
			query += "and t.name = '%s' "%(table)
			query += "order by i.object_id, i.index_id"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			uniqueDict = { 0: "Not unique", 1: "Unique" }
			indexTypeDict = { 
				1: "Clustered index",
				2: "Nonclustered unique index",
				3: "XML index",
				4: "Spatial index",
				5: "Clustered columnstore index",
				6: "Nonclustered columnstore index",
				7: "Nonclustered hash index"
			}

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				line_dict["Name"] = row[0]
				line_dict["Type"] = indexTypeDict.get(row[1], row[1])
				line_dict["Unique"] = uniqueDict.get(int(row[2]), int(row[2]))
				line_dict["Column"] = row[3]
				line_dict["ColumnOrder"] = row[4]
				line_dict["IsNullable"] = row[5]
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.ORACLE:
			query = ""
			query += "SELECT "
			query += "   ai.index_name, " 
			query += "   ai.index_type, "
			query += "   ai.uniqueness, "
			query += "   aic.column_name, "
			query += "   aic.column_position, "
			query += "   atc.nullable "
			query += "FROM all_indexes ai "
			query += "INNER JOIN all_ind_columns aic "
			query += "     ON ai.owner = aic.index_owner "
			query += "     AND ai.index_name = aic.index_name "
			query += "INNER JOIN all_tab_columns atc "
			query += "     ON ai.owner = atc.owner "
			query += "     AND ai.table_name = atc.table_name "
			query += "     AND aic.column_name = atc.column_name "
			query += "WHERE ai.owner = UPPER('%s') "%(schema)
			query += "  AND ai.table_name = UPPER('%s') "%(table)
			query += "ORDER BY aic.column_position"

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				line_dict["Name"] = row[0]
				line_dict["Type"] = row[1].capitalize() 
				if row[2] == "NONUNIQUE":
					line_dict["Unique"] = "Not unique"
				else:
					line_dict["Unique"] = row[2].capitalize()
				line_dict["Column"] = row[3]
				line_dict["ColumnOrder"] = row[4]
				if row[5] == "N":
					line_dict["IsNullable"] = 0
				else:
					line_dict["IsNullable"] = 1
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.MYSQL:
			query = "SHOW INDEX FROM `%s`.`%s`"%(database, table)

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				# Order of columns from "SHOW INDEX" is fixed. If mysql change the standard, we need to change here aswell
				line_dict["Name"] = row[2]
				line_dict["Type"] = row[10].capitalize() 
				if row[1] == "1":
					line_dict["Unique"] = "Not unique"
				else:
					line_dict["Unique"] = "Unique"
				line_dict["Column"] = row[4]
				line_dict["ColumnOrder"] = row[3]
				if row[9] == "YES":
					line_dict["IsNullable"] = 1
				else:
					line_dict["IsNullable"] = 0
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.DB2_UDB:
			query =  "select I.INDNAME, I.INDEXTYPE, I.UNIQUERULE, IC.COLNAME, IC.COLSEQ, C.NULLS "
			query += "from SYSCAT.INDEXES I "
			query += "left join SYSCAT.INDEXCOLUSE IC "
			query += "   on I.INDSCHEMA = IC.INDSCHEMA "
			query += "   and I.INDNAME = IC.INDNAME "
			query += "left join SYSCAT.COLUMNS C "
			query += "   on I.TABNAME = C.TABNAME "
			query += "   and I.TABSCHEMA = C.TABSCHEMA "
			query += "   and IC.COLNAME = C.COLNAME "
			query += "where I.TABNAME = '%s' "%(table)
			query += "   and I.TABSCHEMA = '%s' "%(schema)

			logging.debug("SQL Statement executed: %s" % (query) )
			try:
				JDBCCursor.execute(query)
			except jaydebeapi.DatabaseError as errMsg:
				logging.error("Failure when communicating with JDBC database. %s"%(errMsg))
				return result_df

			uniqueDict = { "D": "Not unique", "U": "Unique", "P": "Unique - PrimaryKey" }
			indexTypeDict = { 
				"BLOK": "Block index",
				"CLUS": "Clustering index",
				"DIM": "Dimension block index",
				"REG": "Regular index",
				"XPTH": "XML path index",
				"XRGN": "XML region index",
				"XVIL": "Index over XML column (logical)",
				"XVIP": "Index over XML column (physical)"
			}

			rows_list = []
			for row in JDBCCursor.fetchall():
				logging.debug(row)
				line_dict = {}
				line_dict["Name"] = row[0]
				line_dict["Type"] = indexTypeDict.get(row[1].strip(), row[1].strip())
				line_dict["Unique"] = uniqueDict.get(row[2].strip(), row[2].strip())
				line_dict["Column"] = row[3]
				line_dict["ColumnOrder"] = row[4]
				if row[5] == "N":
					line_dict["IsNullable"] = 0
				else:
					line_dict["IsNullable"] = 1
				rows_list.append(line_dict)
			result_df = pd.DataFrame(rows_list)

		if serverType == constant.DB2_AS400:
			logging.warning("Reading Index information from DB2AS400 connections is not supported. Please contact developer if this is required")

		if serverType == constant.POSTGRESQL:
			logging.warning("Reading Index information from PostgreSQL connections is not supported. Please contact developer if this is required")

		return result_df

	def getJDBCtablesAndViews(self, JDBCCursor, serverType, database=None, schemaFilter=None, tableFilter=None):
		logging.debug("Executing schemaReader.getJDBCtablesAndViews()")

		if schemaFilter != None:
			schemaFilter = schemaFilter.replace('*', '%')

		if tableFilter != None:
			tableFilter = tableFilter.replace('*', '%')

		if serverType == constant.MSSQL:
			query = "select TABLE_SCHEMA, TABLE_NAME from INFORMATION_SCHEMA.TABLES "
			if schemaFilter != None:
				query += "where TABLE_SCHEMA like '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "and TABLE_NAME like '%s' "%(tableFilter)
				else:
					query += "where TABLE_NAME like '%s' "%(tableFilter)
			query += "order by TABLE_SCHEMA, TABLE_NAME"

		if serverType == constant.ORACLE:
			query  = "select OWNER, TABLE_NAME as NAME from all_tables "
			if schemaFilter != None:
				query += "where OWNER like '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "and TABLE_NAME like '%s' "%(tableFilter)
				else:
					query += "where TABLE_NAME like '%s' "%(tableFilter)

			query += "union all "
			query += "select OWNER, VIEW_NAME as NAME from all_views "
			if schemaFilter != None:
				query += "where OWNER like '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "and VIEW_NAME like '%s' "%(tableFilter)
				else:
					query += "where VIEW_NAME like '%s' "%(tableFilter)
			query += "order by OWNER, NAME "

		if serverType == constant.MYSQL:
#			query = "select '-', table_name from INFORMATION_SCHEMA.tables where table_schema = '%s' "%(self.jdbc_database)
			query = "select '-', table_name from INFORMATION_SCHEMA.tables where table_schema = '%s' "%(database)
			if tableFilter != None:
				query += "and table_name like '%s' "%(tableFilter)
			query += "order by table_name"

		if serverType == constant.POSTGRESQL:
			query = "select TABLE_SCHEMA, TABLE_NAME from INFORMATION_SCHEMA.TABLES "
			if schemaFilter != None:
				query += "where TABLE_SCHEMA like '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "and TABLE_NAME like '%s' "%(tableFilter)
				else:
					query += "where TABLE_NAME like '%s' "%(tableFilter)
			query += "order by TABLE_SCHEMA, TABLE_NAME"

		if serverType == constant.PROGRESS:
			query  = "select \"_Owner\", \"_File-Name\" from PUB.\"_File\" "
			if schemaFilter != None:
				query += "WHERE \"_Owner\" LIKE '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "AND \"_File-Name\" LIKE '%s' "%(tableFilter)
				else:
					query += "WHERE \"_File-Name\" LIKE '%s' "%(tableFilter)
			query += "ORDER BY \"_Owner\", \"_File-Name\""

		if serverType == constant.DB2_UDB:
			query  = "SELECT CREATOR, NAME FROM SYSIBM.SYSTABLES "
			if schemaFilter != None:
				query += "WHERE CREATOR LIKE '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "AND NAME LIKE '%s' "%(tableFilter)
				else:
					query += "WHERE NAME LIKE '%s' "%(tableFilter)
			query += "ORDER BY CREATOR, NAME"

		if serverType == constant.DB2_AS400:
			query  = "SELECT TABLE_SCHEM, TABLE_NAME FROM SYSIBM.SQLTABLES "
			if schemaFilter != None:
				query += "WHERE TABLE_SCHEM LIKE '%s' "%(schemaFilter)
			if tableFilter != None:
				if schemaFilter != None:
					query += "AND TABLE_NAME LIKE '%s' "%(tableFilter)
				else:
					query += "WHERE TABLE_NAME LIKE '%s' "%(tableFilter)
			query += "ORDER BY TABLE_SCHEM, TABLE_NAME"

		logging.debug("SQL Statement executed: %s" % (query) )
		JDBCCursor.execute(query)

		result_df = pd.DataFrame(JDBCCursor.fetchall())
		if len(result_df) > 0:
			result_df.columns = ['schema', 'table']
		else:
			result_df = pd.DataFrame(columns=['schema', 'table'])

		logging.debug("Executing schemaReader.getJDBCtablesAndViews() - Finished")
		return result_df



	def getJdbcTableType(self, serverType, tableTypeFromSource):
		""" Returns the table type of the table """
		logging.debug("Executing schemaReader.getJdbcTableType()")

#		if self.source_columns_df.empty == True:
		if tableTypeFromSource == None:
			logging.warning("No metadata for tableType sent to getJdbcTableType()")
			return None

#		tableTypeFromSource = self.source_columns_df.iloc[0]["TABLE_TYPE"]
		tableType = None


		if serverType == constant.MSSQL:
			# BASE TABLE, VIEW
			if tableTypeFromSource == "VIEW":	tableType = "view"
			else: tableType = "table"

		elif serverType == constant.ORACLE:
			# TABLE, VIEW
			if tableTypeFromSource == "VIEW":	tableType = "view"
			else: tableType = "table"

		elif serverType == constant.MYSQL:
			# BASE TABLE, VIEW, SYSTEM VIEW (for an INFORMATION_SCHEMA table)
			if tableTypeFromSource == "VIEW":	tableType = "view"
			else: tableType = "table"

		elif serverType == constant.POSTGRESQL:
			# BASE TABLE, VIEW, FOREIGN TABLE, LOCAL TEMPORARY
			if tableTypeFromSource == "VIEW":	tableType = "view"
			if tableTypeFromSource == "LOCAL TEMPORARY":	tableType = "temporary"
			else: tableType = "table"

		elif serverType == constant.PROGRESS:
			# Unsure. Cant find documentation. 
			# Verified	T=Table
			# We assume	V=View
			if tableTypeFromSource == "V":	tableType = "view"
			else: tableType = "table"

		elif serverType == constant.DB2_UDB or serverType == constant.DB2_AS400:
			# A = Alias
			# C = Clone Table
			# D = Accelerator-only table
			# G = Global temporary table
			# H = History Table
			# M = Materialized query table
			# P = Table that was implicitly created for XML columns
			# R = Archive table 
			# T = Table
			# V = View
			# X = Auxiliary table
			if tableTypeFromSource == "A":	tableType = "view"
			if tableTypeFromSource == "V":	tableType = "view"
			else: tableType = "table"


		logging.debug("Executing schemaReader.getJdbcTableType() - Finished")
		return tableType

