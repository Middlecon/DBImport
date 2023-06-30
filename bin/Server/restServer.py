import pam
import json
import uvicorn
from gunicorn.app.wsgiapp import WSGIApplication
from datetime import datetime, timedelta
from typing import Union
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from typing_extensions import Annotated
from Server import restServerCalls
from common import constants as constant

# openssl rand -hex 32
# SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
SECRET_KEY = "99bc5362abbe5e5b0c72c91cc9c4e8f5c363cc57ebc4ff444e1248ca906b154b"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 120

fake_users_db = {
	"boszkk": {
		"username": "boszkk",
		"full_name": "Berry Österlund",
		"email": "berry.osterlund@test.com",
		"disabled": False,
	},
	"testUser": {
		"username": "testUser",
		"full_name": "Test User for DBImport REST Server",
		"email": "berry.osterlund@middlecon.se",
		"disabled": False,
	},
	"servicedlpersonec": {
		"username": "servicedl",
		"full_name": "Service User",
		"email": "berry.osterlund@test.com",
		"disabled": False,
	},
}

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

class Token(BaseModel):
	access_token: str
	token_type: str


class TokenData(BaseModel):
	username: Union[str, None] = None

class User(BaseModel):
	username: str
	email: Union[str, None] = None
	full_name: Union[str, None] = None
	disabled: Union[bool, None] = None

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/oauth2/access_token")

app = FastAPI()

dbCalls = restServerCalls.dbCalls()

def verify_password(plain_password, hashed_password):
	return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
	return pwd_context.hash(password)


def get_user(username: str):
	if username in fake_users_db:
		user_dict = fake_users_db[username]
		return User(**user_dict)


# def authenticate_user(fake_db, username: str, password: str):
def authenticate_user(username: str, password: str):
	# First verify that the user is a DBImport user
	user = get_user(username)
	if not user:
		return False

	# Verify password against PAM
#	if username == "testUser":
#		return True

	if not pam.authenticate(username, password, service='login'):
		return False

	return True


def create_access_token(data, expires_delta):
	to_encode = data.copy()
	expire = datetime.utcnow() + expires_delta
	to_encode.update({"exp": expire})
	encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
	return encoded_jwt


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
	credentials_exception = HTTPException(
		status_code=status.HTTP_401_UNAUTHORIZED,
		detail="Could not validate credentials",
		headers={"WWW-Authenticate": "Bearer"},
	)

	try:
		payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
		username: str = payload.get("sub")
		if username is None:
			raise credentials_exception
		token_data = TokenData(username=username)
	except JWTError:
		raise credentials_exception

	user = get_user(token_data.username)
	if user is None:
		raise credentials_exception

	if user.disabled:
		raise HTTPException(status_code=400, detail="User access is disabled")

	return user

async def get_current_active_user(current_user: Annotated[User, Depends(get_current_user)]):
	return current_user

@app.post("/oauth2/access_token", response_model=Token)
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
	validCredentials = authenticate_user(form_data.username, form_data.password)
	if validCredentials == False:
		raise HTTPException(
			status_code=status.HTTP_401_UNAUTHORIZED,
			detail="Incorrect username or password",
			headers={"WWW-Authenticate": "Bearer"},
		)
	access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
	access_token = create_access_token(data={"sub": form_data.username}, expires_delta=access_token_expires)
	return {"access_token": access_token, "token_type": "bearer"}

@app.get("/status")
async def return_import_hive_db(current_user: Annotated[User, Depends(get_current_user)]):
	return json.dumps({ 'status': 'ok', 'version': constant.VERSION})

@app.get("/users/me", response_model=User)
async def read_users_me(current_user: Annotated[User, Depends(get_current_active_user)]):
	return current_user

@app.get("/import/hiveDBs")
async def return_import_hive_db(current_user: Annotated[User, Depends(get_current_user)]):
	return dbCalls.getDBImportImportTableDBs()

@app.get("/import/hiveTables")
async def return_import_hive_tables(db: str, details: bool, current_user: Annotated[User, Depends(get_current_user)]):
	return dbCalls.getDBImportImportTables(db, details)


