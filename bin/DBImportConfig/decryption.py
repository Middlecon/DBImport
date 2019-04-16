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
from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_OAEP
from subprocess import Popen, PIPE
from ConfigReader import configuration
import pandas as pd

class crypto(object):
	def __init__(self):
		logging.debug("Executing crypto.__init__()")
		self.privateKeyFile = None
		self.publicKeyFile = None
		self.privateKeyString = None
		self.publicKeyString = None
		self.privateKey = None
		self.publicKey = None

	def setPrivateKeyFile(self, privateKeyFile):
		self.privateKeyFile = privateKeyFile

		if self.privateKeyFile.startswith("/") == False:
			self.privateKeyFile = os.environ['DBIMPORT_HOME'] + "/" + self.privateKeyFile

		if os.path.isfile(self.privateKeyFile) == False:
			logging.error("The private key file cant be opened. ")
			logging.error("Please check the path in the configuration file for settings Credentials/private_key")
			raise Exception

		self.privateKeyString = open(self.privateKeyFile,"r").read()
		self.privateKey = RSA.importKey(self.privateKeyString)

	def setPublicKeyFile(self, publicKeyFile):
		self.publicKeyFile = publicKeyFile

		if self.publicKeyFile.startswith("/") == False:
			self.publicKeyFile = os.environ['DBIMPORT_HOME'] + "/" + self.publicKeyFile

		if os.path.isfile(self.publicKeyFile) == False:
			logging.error("The public key file cant be opened. ")
			logging.error("Please check the path in the configuration file for settings Credentials/public_key")
			raise Exception

		self.publicKeyString = open(self.publicKeyFile,"r").read()
		self.publicKey = RSA.importKey(self.publicKeyString)

	def decrypt(self, strToDecrypt):
#		credentials = base64.b64decode(row[1])
#
#		sslKeyPrivateFile = configuration.get("Credentials", "private_key")
#		sslKeyPublicFile = configuration.get("Credentials", "public_key")
#
#		if sslKeyPrivateFile.startswith("/") == False:
#			sslKeyPrivateFile = os.environ['DBIMPORT_HOME'] + "/" + sslKeyPrivateFile
#
#		if sslKeyPublicFile.startswith("/") == False:
#			sslKeyPublicFile = os.environ['DBIMPORT_HOME'] + "/" + sslKeyPublicFile
#
#		if os.path.isfile(sslKeyPrivateFile) == False or os.path.isfile(sslKeyPublicFile) == False:
#			logging.error("The private and/or public key can be opened. Please check the path in the configuration file")
#			logging.error("for settings Credentials/private_key and Credetials/public_key.")
#			raise Exception
#
#		sslKeyPrivateString = open(sslKeyPrivateFile,"r").read()
#		sslKeyPrivate = RSA.importKey(sslKeyPrivateString)

#		sslKeyPublicString = open(sslKeyPublicFile,"r").read()
#		sslKeyPublic = RSA.importKey(sslKeyPublicString)

#		#Encrypt with public key
#		encrypted = sslKeyPublic.encrypt(message, 32)

		#Decrypt with private key
#		key = PKCS1_v1_5.new(sslKeyPrivateString)
#		sentinel = Random.new().read(15+dsize)
#		credentialsDecrypted = key.decrypt(credentials, sentinel)

#		enc_session_key = credentials.read(sslKeyPrivate.size_in_bytes())
#		enc_session_key, nonce, tag, ciphertext = [ credentials.read(x) for x in (sslKeyPrivate.size_in_bytes(), 16, 16, -1) ] 
#		cipher_rsa = PKCS1_OAEP.new(sslKeyPrivate)
#		credentialsDecrypted = cipher_rsa.decrypt(enc_session_key)

		strDecrypted = self.privateKey.decrypt(base64.b64decode(strToDecrypt))

		if len(strDecrypted) > 0 and bytes(strDecrypted[:1]) == b"\x02":
			pos = strDecrypted.index(b"\x00")
			strDecrypted = strDecrypted[pos+1:].decode().strip()
			return strDecrypted
		else:
			return None

#			print(type(credentialsDecrypted))
#			print("Decrypted string: %s"%(credentialsDecrypted.decode().strip()))


