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
import re
import sys
import pty
import errno
import time
import logging
import signal
import subprocess
import shlex
import pandas as pd
import Crypto
import binascii
from queue import Queue
from queue import Empty
#import Queue
import threading
from daemons.prefab import run
from ConfigReader import configuration
from datetime import date, datetime, timedelta
from common import constants as constant
from common.Exceptions import *
from DBImportConfig import configSchema
from DBImportConfig import common_config
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import create_view
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.sql import text, alias, select, func
from sqlalchemy.orm import aliased, sessionmaker, Query
from flask import Flask, request, Response
from flask_restful import Resource, Api
from json import dumps
from flask_jsonpify import jsonify
from flask.logging import default_handler
from waitress import serve
from paste.translogger import TransLogger
from webargs import fields, validate
from webargs.flaskparser import use_args, use_kwargs, parser, abort

class restRoot(Resource):
	def __init__(self):
		self.common_config = common_config.config()

	def get(self, args):
		result = { 'type': 'DBImport', 'version': constant.VERSION }
		return jsonify(result)

class restStatus(Resource):
	def __init__(self):
		self.common_config = common_config.config()

	def get(self):
		result = { 'status': 'ok' }
		return jsonify(result)

class restJdbcConnections(Resource):
	restArgs = {"dbAlias": fields.Str(missing="")}

	def __init__(self):
		self.common_config = common_config.config()

	@use_args(restArgs)
	def get(self, args):
		if self.common_config.configDBSession == None:
			self.common_config.connectSQLAlchemy(exitIfFailure = False)
		session = self.common_config.configDBSession()

		returnJSON = []
		try:
			if args["dbAlias"] == "":
				jdbcConnectionsDf = pd.DataFrame(session.query(configSchema.jdbcConnections.__table__).all())
			else:
				jdbcConnectionsDf = pd.DataFrame(session.query(configSchema.jdbcConnections.__table__)
					.filter(configSchema.jdbcConnections.dbalias == args["dbAlias"])
					.all()
					)

			session.close()

		except SQLAlchemyError as e:
			log.error(str(e.__dict__['orig']))
			session.rollback()
			self.disconnectDBImportDB()

		else:
			for index, row in jdbcConnectionsDf.iterrows():
				returnDict = {}
				for col in jdbcConnectionsDf.columns:
					returnValue = str(row[col])
					if returnValue == "None" or returnValue == "NaT":
						returnValue = ""
					if col == "credentials":
						returnValue  = "<Sensitive data not available over REST>"
					returnDict[col] = returnValue
				returnJSON.append(returnDict)

		return jsonify(returnJSON)

#	def getDBImportSession(self):
#		if self.common_config.configDBSession == None:
#			self.common_config.connectSQLAlchemy(exitIfFailure = False)
#		return self.common_config.configDBSession()


class restServer(threading.Thread):
	def __init__(self, threadStopEvent):
		threading.Thread.__init__(self)
		self.threadStopEvent = threadStopEvent

	def run(self):
		logger = "restServer"
		log = logging.getLogger(logger)
		log.info("REST Server started")
		self.mysql_conn = None
		self.mysql_cursor = None
		self.configDBSession = None
		self.configDBEngine = None
		self.debugLogLevel = False

		if logging.root.level == 10:        # DEBUG
			self.debugLogLevel = True

		restAddress = configuration.get("Server", "restServer_address")
		restPort = configuration.get("Server", "restServer_port")

		if restAddress.strip() != "" and restPort.strip() != "":
			app = Flask("restServer")
			api = Api(app)
			api.add_resource(restRoot, '/')
			api.add_resource(restStatus, '/status')
			api.add_resource(restJdbcConnections, '/jdbc_connections')

			log.info("Starting RESTserver on %s:%s"%(restAddress, restPort))
			serve(
				TransLogger(app, setup_console_handler=False, logger=logging.getLogger("restServerAccess")), 
				host=restAddress, 
				port=restPort, 
				ident="DBImport REST Server", 
				url_scheme='https',
				_quiet=True)


