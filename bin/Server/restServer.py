import pam
import json
import uvicorn
import bcrypt
from gunicorn.app.wsgiapp import WSGIApplication
from datetime import datetime, timedelta
from typing import Union
from fastapi import Depends, FastAPI, HTTPException, status, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import PlainTextResponse, RedirectResponse
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from typing import NewType
from typing_extensions import Annotated
from Server import restServerCalls
from Server import dataModels
from common import constants as constant
from DBImportConfig import common_config

common_config = common_config.config()

ADMIN_USER = common_config.getConfigValue("restserver_admin_user")
AUTHENTICATION_METHOD = common_config.getConfigValue("restserver_authentication_method")
SECRET_KEY = common_config.getConfigValue("restserver_secret_key")
ACCESS_TOKEN_EXPIRE_MINUTES = common_config.getConfigValue("restserver_token_ttl")
ALGORITHM = "HS256"


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


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/oauth2/access_token")
app = FastAPI()
dbCalls = restServerCalls.dbCalls()


def get_user(username: str, format_password: bool = True):
	userDict =  dbCalls.getUser(username)	
	if userDict == None:
		return
	if format_password == True:
#		if not userObject.password.startswith("arn:aws:secretsmanager:"):
#			userObject.password="<encrypted>"
		if not userDict["password"].startswith("arn:aws:secretsmanager:"):
			userDict["password"] = "<encrypted>"
#	return userObject
	return userDict


def authenticate_user(username: str, password: str):
	user = get_user(username, format_password=False)
	if not user:
		return False

	if AUTHENTICATION_METHOD == "local":
		print("Authenticate with LOCAL")
		if not verify_password(password, user["password"]):
			return False

	elif AUTHENTICATION_METHOD == "pam":
		print("Authenticate with PAM")
		if not pam.authenticate(username, password, service='login'):
			return False

	else:
		print("AUTHENTICATION_METHOD har fel värde")
		return False

	return True



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


@app.post("/oauth2/access_token", response_model=dataModels.Token)
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

@app.get("/status", response_model=dataModels.status)
# async def get_restServer_status(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
async def get_server_status():
	return json.loads(json.dumps({ 'status': 'ok', 'version': constant.VERSION}))

@app.post("/users/createUser")
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


@app.get("/users/{user}", response_model=dataModels.User)
async def get_user_details(user: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	user = get_user(user)
	if user == None:
		raise HTTPException(
			status_code=status.HTTP_404_NOT_FOUND,
			detail="User does not exist")

	return user

@app.post("/users/{user}/changePassword")
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
	dbCalls.updateUser(user)

	return "Password changed successfully" 

@app.post("/users/{user}/delete")
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

@app.post("/users/{user}/update", response_model=dataModels.User)
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

	dbCalls.updateUser(user)

	user = get_user(username)	# Need to call it again to make sure the password is encrypted in the returned data
	return user

# @app.get("/config/getJDBCdrivers", response_model=dataModels.jdbcDriver)
@app.get("/config/getJDBCdrivers")
async def get_all_configured_jdbc_drivers(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getJDBCdrivers()

@app.post("/config/updateJDBCdriver")
async def change_configuration_for_jdbc_drivers(jdbcDriver: dataModels.jdbcDriver, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	# response.status_code = status.HTTP_201_CREATED
	returnMsg, response.status_code =  dbCalls.updateJDBCdriver(jdbcDriver, current_user["username"])
	return returnMsg

@app.get("/config/getConfig", response_model=dataModels.configuration)
async def get_global_configuration(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
    return dbCalls.getConfiguration()

@app.post("/config/updateConfig")
async def update_global_configuration(configuration: dataModels.configuration, current_user: Annotated[dataModels.User, Depends(get_current_user)], response: Response):
	returnMsg, response.status_code = dbCalls.updateConfiguration(configuration, current_user["username"])
	return returnMsg

# API calls bellow this point is tech-preview and is missing a lot of functionallity
@app.get("/import/hiveDBs")
async def get_all_import_databases(current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getDBImportImportTableDBs()

@app.get("/import/hiveTables")
async def get_all_import_tables(db: str, details: bool, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getDBImportImportTables(db, details)

@app.get("/import/hiveTableDetails")
async def get_import_table_details(db: str, table: str, current_user: Annotated[dataModels.User, Depends(get_current_user)]):
	return dbCalls.getDBImportImportTableDetails(db, table)


