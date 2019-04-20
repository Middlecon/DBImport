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

import os
import io
import sys
import logging
import base64
import json
import requests
from ConfigReader import configuration
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, time, timedelta
import pandas as pd
from DBImportConfig import constants as constant

class postSQLData(object):
	def __init__(self):
		logging.debug("Executing rest.__init__()")

		self.mysql_conn = None
		self.mysql_cursor_01 = None
		self.mysql_cursor_02 = None

		self.RESTendpoint = configuration.get("REST_statistics", "rest_endpoint")
		if self.RESTendpoint == "":
			logging.error("Cant find the REST endpoint. Please check configuration file")
			sys.exit(1)

		# Fetch configuration about MySQL database and how to connect to it
		mysql_hostname = configuration.get("Database", "mysql_hostname")
		mysql_port =     configuration.get("Database", "mysql_port")
		mysql_database = configuration.get("Database", "mysql_database")
		mysql_username = configuration.get("Database", "mysql_username")
		mysql_password = configuration.get("Database", "mysql_password")

		# Esablish a connection to the DBImport database in MySQL
		try:
			self.mysql_conn = mysql.connector.connect(host=mysql_hostname, 
												 port=mysql_port, 
												 database=mysql_database, 
												 user=mysql_username, 
												 password=mysql_password)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				logging.error("Something is wrong with your user name or password")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				logging.error("Database does not exist")
			else:
				logging.error("%s"%err)
			logging.error("Error: There was a problem connecting to the MySQL database. Please check configuration and serverstatus and try again")
			self.remove_temporary_files()
			sys.exit(1)
		else:
			self.mysql_cursor_01 = self.mysql_conn.cursor(buffered=False)
			self.mysql_cursor_02 = self.mysql_conn.cursor(buffered=False)

		rest = restInterface()

		query = "select id, jsondata from json_to_rest"
		self.mysql_cursor_01.execute(query)
		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor_01.statement) )

		successCounter = 0
		errorCounter = 0

		for row in self.mysql_cursor_01.fetchall():
			jsonID = row[0]
			jsonData = row[1]
			response_code = -1

			response_code = rest.sendData(jsonData)

			if response_code == 200:
				query = "delete from json_to_rest where id = %s"
				self.mysql_cursor_02.execute(query, (jsonID, ))
				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor_02.statement) )
				self.mysql_conn.commit()
				successCounter += 1
			else:
				errorCounter += 1

		logging.info("Transmitted %s JSON documents to %s"%(successCounter, self.RESTendpoint))
		if errorCounter > 0:   logging.error("%s errors encountered"%(errorCounter))

		self.mysql_conn.close()


class restInterface(object):
	def __init__(self):
		logging.debug("Executing rest.__init__()")

		self.headers = {'Content-type': 'application/json'}

		self.RESTendpoint = configuration.get("REST_statistics", "rest_endpoint")
		self.RESTtimeout = configuration.get("REST_statistics", "timeout")

		if self.RESTendpoint == "":
			logging.error("Cant find the REST endpoint. Please check configuration file")
			sys.exit(1)

		if self.RESTtimeout == "":
			logging.error("Cant find the REST endpoint timeout. Please check configuration file")
			sys.exit(1)

	def sendData(self, jsonData):
		""" Sending data to REST endpoint """
		logging.debug("Executing rest.restInterface.sendData")

		response_code = -1
		try:
			response = requests.post(self.RESTendpoint, data=jsonData, headers=self.headers, timeout=int(self.RESTtimeout))
			response_code = response.status_code
		except requests.exceptions.RequestException as e:
			logging.error(e)

		if response_code != 200: 
			logging.error("REST call unsuccessful! Return code %s"%(response_code))

		logging.debug("Executing rest.restInterface.sendData - Finished")
		return response_code
