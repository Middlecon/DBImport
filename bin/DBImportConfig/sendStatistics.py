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
from common import constants as constant
from DBImportConfig import common_config
import kafka
import ssl

#class postSQLDataToREST(object):
#	def __init__(self):
#		logging.debug("Executing rest.__init__()")
#
#		self.mysql_conn = None
#		self.mysql_cursor_01 = None
#		self.mysql_cursor_02 = None
#
#		self.RESTendpoint = self.common_config.getConfigValue(key = "rest_url")
#
#		# Fetch configuration about MySQL database and how to connect to it
#		mysql_hostname = configuration.get("Database", "mysql_hostname")
#		mysql_port =     configuration.get("Database", "mysql_port")
#		mysql_database = configuration.get("Database", "mysql_database")
#		mysql_username = configuration.get("Database", "mysql_username")
#		mysql_password = configuration.get("Database", "mysql_password")
#
#		# Esablish a connection to the DBImport database in MySQL
#		try:
#			self.mysql_conn = mysql.connector.connect(host=mysql_hostname, 
#												 port=mysql_port, 
#												 database=mysql_database, 
#												 user=mysql_username, 
#												 password=mysql_password)
#		except mysql.connector.Error as err:
#			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#				logging.error("Something is wrong with your user name or password")
#			elif err.errno == errorcode.ER_BAD_DB_ERROR:
#				logging.error("Database does not exist")
#			else:
#				logging.error("%s"%err)
#			logging.error("Error: There was a problem connecting to the MySQL database. Please check configuration and serverstatus and try again")
#			self.remove_temporary_files()
#			sys.exit(1)
#		else:
#			self.mysql_cursor_01 = self.mysql_conn.cursor(buffered=False)
#			self.mysql_cursor_02 = self.mysql_conn.cursor(buffered=False)
#
#		rest = restInterface()
#
#		query = "select id, jsondata from json_to_rest"
#		self.mysql_cursor_01.execute(query)
#		logging.debug("SQL Statement executed: %s" % (self.mysql_cursor_01.statement) )
#
#		successCounter = 0
#		errorCounter = 0
#
#		for row in self.mysql_cursor_01.fetchall():
#			jsonID = row[0]
#			jsonData = row[1]
#			response_code = -1
#
#			response_code = rest.sendData(jsonData)
#
#			if response_code == 200:
#				query = "delete from json_to_rest where id = %s"
#				self.mysql_cursor_02.execute(query, (jsonID, ))
#				logging.debug("SQL Statement executed: %s" % (self.mysql_cursor_02.statement) )
#				self.mysql_conn.commit()
#				successCounter += 1
#			else:
#				errorCounter += 1
#
#		logging.info("Transmitted %s JSON documents to %s"%(successCounter, self.RESTendpoint))
#		if errorCounter > 0:   logging.error("%s errors encountered"%(errorCounter))
#
#		self.mysql_conn.close()


class sendStatistics(object):
	def __init__(self):
		logging.debug("Executing rest.__init__()")

		self.common_config = common_config.config()
		self.RESTheaders = {'Content-type': 'application/json'}

		self.RESTendpoint = self.common_config.getConfigValue(key = "rest_url")
		self.RESTtimeout = self.common_config.getConfigValue(key = "rest_timeout")
		self.RESTverifySSL = self.common_config.getConfigValue(key = "rest_verifyssl")
		self.RESTtrustCAfile = self.common_config.getConfigValue(key = "rest_trustcafile")

		self.kafkaBrokers = self.common_config.getConfigValue(key = "kafka_brokers")
		self.kafkaTrustCAFile = self.common_config.getConfigValue(key = "kafka_trustcafile")
		self.kafkaSecurityProtocol = self.common_config.getConfigValue(key = "kafka_securityprotocol")
		self.kafkaSaslMechanism = self.common_config.getConfigValue(key = "kafka_saslmechanism")
		self.kafkaTopic = self.common_config.getConfigValue(key = "kafka_topic")

		if self.RESTendpoint == "":
			logging.error("Cant find the REST endpoint. Please check configuration file")
			sys.exit(1)

		if self.RESTtimeout == "":
			logging.error("Cant find the REST endpoint timeout. Please check configuration file")
			sys.exit(1)

	def sendRESTdata(self, jsonData):
		""" Sending data to REST endpoint """
		logging.debug("Executing sendStatistics.sendRESTdata")

		logging.info("Sending data to REST interface")
		logging.debug("Sending the following JSON to REST: %s"% (json.dumps(jsonData, sort_keys=True, indent=4)))

		response_code = -1

		try:
			restProtocol = self.RESTendpoint.split(":")[0].lower()
			
			if restProtocol == "http":
				response = requests.post(
					self.RESTendpoint, 
					data=jsonData, 
					headers=self.RESTheaders, 
					timeout=self.RESTtimeout)
			else:
				if self.RESTverifySSL == True:
					restVerify = self.RESTtrustCAfile
				else:
					restVerify = False

				response = requests.post(
					self.RESTendpoint, 
					data=jsonData, 
					headers=self.RESTheaders, 
					timeout=self.RESTtimeout,
					verify=restVerify)

			response_code = response.status_code
		except requests.exceptions.RequestException as e:
			logging.error(e)

		if response_code != 200: 
			logging.error("REST call unsuccessful! Return code %s"%(response_code))

		logging.debug("Executing sendStatistics.sendRESTdata - Finished")
		return response_code

	def publishKafkaData(self, jsonData):
		""" Publish data to Kafka topic """
		logging.debug("Executing sendStatistics.publishKafkaData")

		logging.info("Sending statistics data to Kafka topic")
		logging.debug("Sending the following JSON to Kafka: %s"% (jsonData))
		kafkaLogger = logging.getLogger('kafka')
		kafkaLogger.setLevel(logging.ERROR)

		kafkaError = False
		result = True
#		logging.error("SEND TO KAFKA DISABLED")
#		return result

		try:
			ssl_context = ssl.create_default_context(cafile=self.kafkaTrustCAFile)

		except FileNotFoundError:
			logging.error("SSL CA file for Kafka cant be found")
			ssl_context = None

		try:
			if self.kafkaSaslMechanism != None and self.kafkaSecurityProtocol != None:
				producer = kafka.KafkaProducer(
					bootstrap_servers=self.kafkaBrokers, 
					security_protocol=self.kafkaSecurityProtocol,
					sasl_mechanism=self.kafkaSaslMechanism,
					ssl_context=ssl_context)
			else:
				producer = kafka.KafkaProducer(
					bootstrap_servers=self.kafkaBrokers, 
					ssl_context=ssl_context)

			producer.send(topic=self.kafkaTopic, value=jsonData.encode('utf-8'))

		except AssertionError as e:
			logging.error("Kafka Error: %s"%e)
			kafkaError = True

		except kafka.errors.NoBrokersAvailable:
			logging.error("None of the Kafka brokers configured are available")
			kafkaError = True

		except kafka.errors.UnrecognizedBrokerVersion:
			logging.error("Unrecognized broker version. Are you connecting to a secure Kafka broker without the correct security protocols?")
			kafkaError = True

		except kafka.errors.KafkaTimeoutError:
			logging.error("Timeout error when trying to send statistics to Kafka")
			kafkaError = True

		if kafkaError == True:
			result = False

		logging.debug("Executing sendStatistics.publishKafkaData - Finished")
		return result
