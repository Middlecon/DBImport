import sys
import pam
import json
import uvicorn
import bcrypt
import logging
from gunicorn.app.wsgiapp import WSGIApplication
from datetime import datetime, timedelta
from typing import Union, NewType, List, Union, Any
from fastapi import Depends, FastAPI, HTTPException, status, Response, Header
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import PlainTextResponse, RedirectResponse
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from typing_extensions import Annotated
from Server import restServerCalls
from Server import dataModels
from Schedule import Airflow
from common import constants as constant
from common.Exceptions import *
from DBImportConfig import common_config
from ConfigReader import configuration

common_config = common_config.config()

ADMIN_USER = common_config.getConfigValue("restserver_admin_user")
AUTHENTICATION_METHOD = common_config.getConfigValue("restserver_authentication_method")
SECRET_KEY = common_config.getConfigValue("restserver_secret_key")
ACCESS_TOKEN_EXPIRE_MINUTES = common_config.getConfigValue("restserver_token_ttl")
ALGORITHM = "HS256"
try:
	AWS_REGION = configuration.get("AWS", "region", exitOnError=False)
except invalidConfiguration:
	AWS_REGION=""


class StandaloneApplication(WSGIApplication):
	def __init__(self, app_uri, options=None):
		self.options = options or {}
		self.app_uri = app_uri
		super().__init__()

	def load_config(self):
		config = {
			key: value
			for key, value in self.options.items()
			if key in self.cfg.settings and value is not None
		}
		for key, value in config.items():
			self.cfg.set(key.lower(), value)


def get_password_hash(password):
	if password.startswith("arn:aws:secretsmanager:"):
		# We dont hash "passwords" that links to external repository
		return password
	password_encoded = password.encode('utf-8')
	salt = bcrypt.gensalt()
	hashed_password = bcrypt.hashpw(password=password_encoded, salt=salt)
	return hashed_password.decode('utf-8')


def verify_password(password, hashed_password):
	try:
		password_encoded = password.encode('utf-8')
		hashed_password_encoded = hashed_password.encode('utf-8')
		return bcrypt.checkpw(password=password_encoded, hashed_password=hashed_password_encoded)
	except ValueError:
		return False


logger = "gunicorn.error"
log = logging.getLogger(logger)

tags_metadata = [
	{"name": "General", "description": "Authentication and general status requests"},
	{"name": "Credentials", "description": "Users and Groups"},
	{"name": "Connections", "description": "Connections to JDBC servers"},
	{"name": "Imports", "description": "All requests related to Exports, except Airflow"},
	{"name": "Exports", "description": "All requests related to Imports, except Airflow"},
	{"name": "Airflow", "description": "Airflow functions"},
	{"name": "Configuration", "description": "All functions related to configurations and customization"}
]

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/oauth2/access_token")
app = FastAPI(openapi_tags=tags_metadata)
dbCalls = restServerCalls.dbCalls()
airflow = Airflow.initialize()
airflow.logger = logger


def get_user(username: str, format_password: bool = True):
	userDict =  dbCalls.getUser(username)	
	if userDict == None:
		return

	if format_password == True:
		if userDict["password"] == None or not userDict["password"].startswith("arn:aws:secretsmanager:"):
			userDict["password"] = "<encrypted>"

	return userDict


def authenticate_user(username: str, password: str):
	user = get_user(username, format_password=False)
	username = user["username"]
	if not user:
		return False

	if AUTHENTICATION_METHOD == "local":
		if user["password"] == None:
			log.warning("User '%s' made an unsuccessful authentication attempt"%(username))
			return False

		if not user["password"].startswith("arn:aws:secretsmanager:"):
			log.debug("Authenticate with LOCAL")
			if not verify_password(password, user["password"]):
				log.warning("User '%s' made an unsuccessful authentication attempt"%(username))
				return False
			else:
				log.info("User '%s' authenticated successfully"%(username))
				return True
		else:
			log.debug("Authenticate with LOCAL using AWS credentials")
			# This user have its password stored in AWS Secrets Manager

			if AWS_REGION == "":
				log.error("AWS Region is not configured in the configuraton file. This is required in order to user Secrets Manager for user credentials")
				return False

#			if "boto3" not in sys.modules:
#				# Only load modules if not already loaded. If not deployed in AWS environment, these does not need to be loaded at all
#				print("Loading modules")
			import boto3
			from botocore.exceptions import ClientError

			# Create a Secrets Manager client
			session = boto3.session.Session()
			client = session.client(
				service_name='secretsmanager',
				region_name=AWS_REGION
				)

			try:
				get_secret_value_response = client.get_secret_value(
					SecretId=user["password"]
				)
			except ClientError as e:
				# For a list of exceptions thrown, see
				# https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
				raise e
				return False

			secretsManagerPassword = json.loads(get_secret_value_response['SecretString'])["password"]

			if password == secretsManagerPassword:
				log.info("User '%s' authenticated successfully"%(username))
				return True
			else:
				log.warning("User '%s' made an unsuccessful authentication attempt"%(username))
				return False


	elif AUTHENTICATION_METHOD == "pam":
		log.debug("Authenticate with PAM")
		if not pam.authenticate(username, password, service='login'):
			log.warning("User '%s' made an unsuccessful authentication attempt"%(username))
			return False
		else:
			log.info("User '%s' authenticated successfully"%(username))
			return True

	else:
		print("AUTHENTICATION_METHOD har fel värde")
		return False

	return False



async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
	credentials_exception = HTTPException(
		status_code=status.HTTP_401_UNAUTHORIZED,
		detail="Could not validate credentials",
		headers={"WWW-Authenticate": "Bearer"},
	)

	try:
		payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])		# This also verified that the token isnt expired
		username: str = payload.get("sub")
		if username is None:
			raise credentials_exception
		token_username = username
	except JWTError:
		raise credentials_exception

	current_user = get_user(token_username)
	if current_user is None:
		raise credentials_exception

#	if current_user.disabled:
	if current_user["disabled"]:
		raise HTTPException(status_code=400, detail="User access is disabled")

	return current_user


def create_access_token(data):
	expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
	data.update({"exp": expire})

	encoded_jwt = jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)

	return encoded_jwt


@app.post("/oauth2/access_token", response_model=dataModels.Token, tags=["General"])
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
	validCredentials = authenticate_user(form_data.username, form_data.password)
	if validCredentials == False:
		raise HTTPException(
			status_code=status.HTTP_401_UNAUTHORIZED,
			detail="Incorrect username or password",
			headers={"WWW-Authenticate": "Bearer"},
		)

	access_token = create_access_token(data={"sub": form_data.username})
	return {"access_token": access_token, "token_type": "bearer"}

@app.get("/status", response_model=dataModels.status, tags=["General"])
# async def get_restServer_status(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
async def get_server_status():
	return json.loads(json.dumps({ 'status': 'ok', 'version': constant.VERSION}))

@app.post("/users/createUser", tags=["Credentials"])
async def create_a_user(user_data: dataModels.User, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	user = get_user(user_data.username)

	if user != None:
		raise HTTPException(
			status_code=status.HTTP_409_CONFLICT,
			detail="User already exists")

	if current_user["username"] != ADMIN_USER:
		raise HTTPException(
			status_code=status.HTTP_403_FORBIDDEN,
			detail="You are not allowed to create users")

	# set the new password in the user object and save it
	user_data.password = get_password_hash(user_data.password)
	dbCalls.createUser(user_data)

	return "User created successfully" 


@app.get("/users/{user}", response_model=dataModels.User, tags=["Credentials"])
async def get_user_details(user: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	user = get_user(user)
	if user == None:
		raise HTTPException(
			status_code=status.HTTP_404_NOT_FOUND,
			detail="User does not exist")

	return user

@app.post("/users/{user}/changePassword", tags=["Credentials"])
async def change_the_user_password(user: str, password_data: dataModels.changePassword, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	user = get_user(user, format_password=False)

	if AUTHENTICATION_METHOD != "local":
		raise HTTPException(
			status_code=status.HTTP_403_FORBIDDEN,
			detail="Changing password is only supported when authentication method is 'local'")

	if user == None:
		raise HTTPException(
			status_code=status.HTTP_404_NOT_FOUND,
			detail="User does not exist")

	if current_user["username"] != ADMIN_USER:
		if user["username"] != current_user["username"]:
			raise HTTPException(
				status_code=status.HTTP_403_FORBIDDEN,
				detail="You are only allowed to change your own password")

		if verify_password(password_data.old_password, user["password"]) == False:
			raise HTTPException(
				status_code=status.HTTP_403_FORBIDDEN,
				detail="Current password does not match")

	# set the new password in the user object and save it
	user["password"] = get_password_hash(password_data.new_password)
	dbCalls.updateUser(user, passwordChanged=True, currentUser=current_user["username"])

	return "Password changed successfully" 

@app.delete("/users/{user}/delete", tags=["Credentials"])
async def update_user_details(user: str, user_data: dataModels.changeUser, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	username = user
	user = get_user(username, format_password=False)
	if user == None:
		raise HTTPException(
			status_code=status.HTTP_404_NOT_FOUND,
			detail="User does not exist")

	if username == ADMIN_USER:
		raise HTTPException(
			status_code=status.HTTP_403_FORBIDDEN,
			detail="Deleting the defined admin user is not allowed")

	if current_user["username"] != ADMIN_USER:
		raise HTTPException(
			status_code=status.HTTP_403_FORBIDDEN,
			detail="You are not allowed to delete users")

	dbCalls.deleteUser(username)

	return "User deleted"

@app.post("/users/{user}/update", response_model=dataModels.User, tags=["Credentials"])
async def update_user_details(user: str, user_data: dataModels.changeUser, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	username = user
	user = get_user(username, format_password=False)
	if user == None:
		raise HTTPException(
			status_code=status.HTTP_404_NOT_FOUND,
			detail="User does not exist")

	if current_user["username"] != ADMIN_USER:
		if user["username"] != current_user["username"]:
			raise HTTPException(
				status_code=status.HTTP_403_FORBIDDEN,
				detail="You are only allowed to change your own user")

	if user_data.disabled != None: user["disabled"] = user_data.disabled
	if user_data.fullname != None: user["fullname"] = user_data.fullname
	if user_data.department != None: user["department"] = user_data.department
	if user_data.email != None: user["email"] = user_data.email

	dbCalls.updateUser(user, passwordChanged=False, currentUser=current_user["username"])

	user = get_user(username)	# Need to call it again to make sure the password is encrypted in the returned data
	return user

# @app.get("/config/getJDBCdrivers", response_model=dataModels.jdbcDriver)
@app.get("/config/getJDBCdrivers", tags=["Configuration"])
async def get_all_configured_jdbc_drivers(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getJDBCdrivers()

@app.post("/config/updateJDBCdriver", tags=["Configuration"])
async def change_configuration_for_jdbc_drivers(jdbcDriver: dataModels.jdbcDriver, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	# response.status_code = status.HTTP_201_CREATED
	returnMsg, response.status_code =  dbCalls.updateJDBCdriver(jdbcDriver, current_user["username"])
	return returnMsg

@app.get("/config/getConfig", response_model=dataModels.configuration, tags=["Configuration"])
async def get_global_configuration(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
    return dbCalls.getConfiguration(current_user["username"])

@app.post("/config/updateConfig", tags=["Configuration"])
async def update_global_configuration(configuration: dataModels.configuration, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.updateConfiguration(configuration, current_user["username"])
	return returnMsg

@app.get("/connection", response_model=List[dataModels.connection], tags=["Connections"])
async def get_all_connections(current_user: Annotated[dataModels.User, Depends(get_current_user)], listOnlyName: bool=False):
	return dbCalls.getAllConnections(listOnlyName)

@app.post("/connection", tags=["Connections"])
async def update_connection(connection: dataModels.connectionDetails, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.updateConnection(connection, current_user["username"])
	return returnMsg

@app.get("/connection/{connection}", response_model=dataModels.connectionDetails, tags=["Connections"])
async def get_connection_details(connection: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getConnection(connection)

@app.delete("/connection/{connection}", tags=["Connections"])
async def delete_connection(connection: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.deleteConnection(connection, current_user["username"])
	return returnMsg

@app.post("/connection/encryptCredentials", response_model=dataModels.defaultResultResponse, tags=["Connections"])
# @app.post("/connection/encryptCredentials", response_model=dataModels.encryptCredentialResult, tags=["Connections"])
async def encrypt_credentials_and_store_in_connection(credentials: dataModels.encryptCredential, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.encryptedCredentials(credentials, current_user["username"])
	return returnMsg

@app.post("/connection/generateJDBCconnectionString", response_model=dataModels.generatedJDBCconnectionString, tags=["Connections"])
async def generate_a_jdbc_connection_string(connectionValues: dataModels.generateJDBCconnectionString, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.generateJDBCconnectionString(connectionValues, current_user["username"])
	return returnMsg

@app.post("/connection/search", response_model=List[dataModels.connection], tags=["Connections"])
async def search_connections(searchValues: dataModels.connectionSearch, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	return dbCalls.searchConnections(searchValues, current_user["username"])

@app.get("/connection/testConnection/{connection}", response_model=dataModels.defaultResultResponse, tags=["Connections"])
# @app.get("/connection/testConnection/{connection}", response_model=dataModels.testConnectionResponse, tags=["Connections"])
async def test_a_connection(connection: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.testConnection(connection, current_user["username"])
	return returnMsg

@app.get("/import/db", response_model=List[dataModels.importDBs], tags=["Imports"])
async def get_all_import_databases(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getAllImportDatabases()

@app.post("/import/discover", response_model=List[dataModels.discoverImportTable], tags=["Imports"])
async def discover_new_import_tables(data: dataModels.discoverImportTableOptions, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.discoverImportTables(data, current_user["username"])
	return returnMsg

@app.post("/import/search", response_model=List[dataModels.importTable], tags=["Imports"])
async def search_import_tables(searchValues: dataModels.importTableSearch, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	return dbCalls.searchImportTables(searchValues, current_user["username"])

@app.get("/import/table/{connection}", response_model=List[dataModels.importTable], tags=["Imports"])
async def get_import_tables_on_a_connection(connection: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getImportTablesOnConnection(connection, current_user["username"])

@app.get("/import/table/{database}", response_model=List[dataModels.importTable], tags=["Imports"])
async def get_import_tables_in_database(database: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getImportTablesInDatabase(database, current_user["username"])

@app.get("/import/table/{database}/{table}", response_model=dataModels.importTableDetailsRead, tags=["Imports"])
async def get_import_table_details(database: str, table: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], includeColumns: bool=True):
	return dbCalls.getImportTableDetails(database = database, table = table, includeColumns = includeColumns)

@app.delete("/import/table/{database}/{table}", tags=["Imports"])
async def delete_import_table(database: str, table: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.deleteImportTable(database, table, current_user["username"])
	return returnMsg

@app.post("/import/table", tags=["Imports"])
async def create_or_update_import_table(table: dataModels.importTableDetailsWrite, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.updateImportTable(table, current_user["username"])
	return returnMsg

@app.post("/import/table/bulk", response_model=dataModels.defaultResultResponse, tags=["Imports"])
async def bulk_update_import_table(bulkData: dataModels.importTableBulkUpdate, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.bulkUpdateImportTable(bulkData, current_user["username"])
	return returnMsg

@app.delete("/import/table/bulk", response_model=dataModels.defaultResultResponse, tags=["Imports"])
async def bulk_delete_import_table(bulkData: List[dataModels.importTablePK], current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.bulkDeleteImportTable(bulkData, current_user["username"])
	return returnMsg

@app.get("/export/connection", response_model=List[dataModels.exportConnections], tags=["Exports"])
async def get_all_connections_with_exports(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getExportConnections()

@app.post("/export/search", response_model=List[dataModels.exportTable], tags=["Exports"])
async def search_export_tables(searchValues: dataModels.exportTableSearch, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	return dbCalls.searchExportTables(searchValues, current_user["username"])

@app.get("/export/table/{connection}", response_model=List[dataModels.exportTable], tags=["Exports"])
async def get_export_tables_on_connection(connection: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	return dbCalls.getExportTables(connection = connection, schema = None, currentUser = current_user["username"])

@app.get("/export/table/{connection}/{schema}", response_model=List[dataModels.exportTable], tags=["Exports"])
async def get_export_tables_on_connection_and_schema(connection: str, schema: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	return dbCalls.getExportTables(connection = connection, schema = schema, currentUser = current_user["username"])

@app.get("/export/table/{connection}/{schema}/{table}", response_model=dataModels.exportTableDetailsRead, tags=["Exports"])
async def get_export_table_details(connection: str, schema: str, table: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], includeColumns: bool=True):
	return dbCalls.getExportTableDetails(connection = connection, schema = schema, table = table, includeColumns = includeColumns)

@app.delete("/export/table/{connection}/{schema}/{table}", tags=["Exports"])
async def delete_export_table(connection: str, schema: str, table: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.deleteExportTable(connection, schema, table, current_user["username"])
	return returnMsg

@app.post("/export/table", tags=["Exports"])
async def create_or_update_export_table(table: dataModels.exportTableDetailsWrite, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.updateExportTable(table, current_user["username"])
	return returnMsg

@app.post("/export/table/bulk", response_model=dataModels.defaultResultResponse, tags=["Exports"])
async def bulk_update_export_table(bulkData: dataModels.exportTableBulkUpdate, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.bulkUpdateExportTable(bulkData, current_user["username"])
	return returnMsg

@app.delete("/export/table/bulk", response_model=dataModels.defaultResultResponse, tags=["Exports"])
async def bulk_delete_export_table(bulkData: List[dataModels.exportTablePK], current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.bulkDeleteExportTable(bulkData, current_user["username"])
	return returnMsg

@app.get("/airflow/dags", response_model=List[dataModels.airflowAllDags], tags=["Airflow"])
async def get_all_airflow_dags(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getAllAirflowDags()

@app.get("/airflow/generate_dag", tags=["Airflow"])
async def generate_airflow_dag(dagname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = airflow.generateDAGfromREST(dagname, current_user["username"])
	return returnMsg

@app.get("/airflow/dags/import", response_model=List[dataModels.airflowImportDags], tags=["Airflow"])
async def get_import_airflow_dags(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getAirflowImportDags()

@app.post("/airflow/dags/import", tags=["Airflow"])
async def create_or_update_import_airflow_dag(airflowDag: dataModels.airflowImportDag, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.updateImportAirflowDag(airflowDag, current_user["username"])
	return returnMsg

@app.post("/airflow/dags/import/bulk", response_model=dataModels.defaultResultResponse, tags=["Airflow"])
async def bulk_update_import_airflow_dag(bulkData: dataModels.airflowDagBulkUpdate, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.bulkUpdateImportAirflowDag(bulkData, current_user["username"])
	return returnMsg

@app.delete("/airflow/dags/import/bulk", response_model=dataModels.defaultResultResponse, tags=["Airflow"])
async def bulk_delete_import_airflow_dag(bulkData: List[dataModels.airflowDagPK], current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.bulkDeleteImportAirflowDag(bulkData, current_user["username"])
	return returnMsg

@app.get("/airflow/dags/import/{dagname}", response_model=dataModels.airflowImportDag, tags=["Airflow"])
async def get_import_airflow_dag(dagname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getAirflowImportDag(dagname)

@app.delete("/airflow/dags/import/{dagname}", tags=["Airflow"])
async def delete_import_airflow_dag(dagname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.deleteImportAirflowDag(dagname, current_user["username"])
	return returnMsg

@app.delete("/airflow/dags/import/{dagname}/{taskname}", tags=["Airflow"])
async def delete_task_from_import_airflow_dag(dagname: str, taskname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.deleteTaskFromAirflowDag(dagname, taskname, current_user["username"])
	return returnMsg

@app.get("/airflow/dags/export", response_model=List[dataModels.airflowExportDags], tags=["Airflow"])
async def get_export_airflow_dags(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getAirflowExportDags()

@app.post("/airflow/dags/export", tags=["Airflow"])
async def create_or_update_export_airflow_dag(airflowDag: dataModels.airflowExportDag, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.updateExportAirflowDag(airflowDag, current_user["username"])
	return returnMsg

@app.post("/airflow/dags/export/bulk", response_model=dataModels.defaultResultResponse, tags=["Airflow"])
async def bulk_update_export_airflow_dag(bulkData: dataModels.airflowDagBulkUpdate, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.bulkUpdateExportAirflowDag(bulkData, current_user["username"])
	return returnMsg

@app.delete("/airflow/dags/export/bulk", response_model=dataModels.defaultResultResponse, tags=["Airflow"])
async def bulk_delete_export_airflow_dag(bulkData: List[dataModels.airflowDagPK], current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.bulkDeleteExportAirflowDag(bulkData, current_user["username"])
	return returnMsg

@app.get("/airflow/dags/export/{dagname}", response_model=dataModels.airflowExportDag, tags=["Airflow"])
async def get_export_airflow_dag(dagname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getAirflowExportDag(dagname)

@app.delete("/airflow/dags/export/{dagname}", tags=["Airflow"])
async def delete_export_airflow_dag(dagname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.deleteExportAirflowDag(dagname, current_user["username"])
	return returnMsg

@app.delete("/airflow/dags/export/{dagname}/{taskname}", tags=["Airflow"])
async def delete_task_from_export_airflow_dag(dagname: str, taskname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.deleteTaskFromAirflowDag(dagname, taskname, current_user["username"])
	return returnMsg

@app.get("/airflow/dags/custom", response_model=List[dataModels.airflowCustomDags], tags=["Airflow"])
async def get_custom_airflow_dags(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getAirflowCustomDags()

@app.post("/airflow/dags/custom", tags=["Airflow"])
async def create_or_update_custom_airflow_dag(airflowDag: dataModels.airflowCustomDag, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.updateCustomAirflowDag(airflowDag, current_user["username"])
	return returnMsg

@app.post("/airflow/dags/custom/bulk", response_model=dataModels.defaultResultResponse, tags=["Airflow"])
async def bulk_update_custom_airflow_dag(bulkData: dataModels.airflowDagBulkUpdate, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.bulkUpdateCustomAirflowDag(bulkData, current_user["username"])
	return returnMsg

@app.delete("/airflow/dags/custom/bulk", response_model=dataModels.defaultResultResponse, tags=["Airflow"])
async def bulk_delete_custom_airflow_dag(bulkData: List[dataModels.airflowDagPK], current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.bulkDeleteCustomAirflowDag(bulkData, current_user["username"])
	return returnMsg

@app.get("/airflow/dags/custom/{dagname}", response_model=dataModels.airflowCustomDag, tags=["Airflow"])
async def get_custom_airflow_dag(dagname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getAirflowCustomDag(dagname)

@app.delete("/airflow/dags/custom/{dagname}", tags=["Airflow"])
async def delete_custom_airflow_dag(dagname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.deleteCustomAirflowDag(dagname, current_user["username"])
	return returnMsg

@app.delete("/airflow/dags/custom/{dagname}/{taskname}", tags=["Airflow"])
async def delete_task_from_custom_airflow_dag(dagname: str, taskname: str, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code =  dbCalls.deleteTaskFromAirflowDag(dagname, taskname, current_user["username"])
	return returnMsg


