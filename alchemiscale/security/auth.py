"""
:mod:`alchemiscale.security.auth` --- security components for API services
==========================================================================

"""

import secrets
from datetime import datetime, timedelta
from typing import Optional, Union

import bcrypt
from fastapi import HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel

from .models import CredentialedEntity, Token, TokenData

MAX_PASSWORD_SIZE = 4096
_dummy_secret = "dummy"


class BcryptPasswordHandler(object):
    rounds: int = 12
    ident: str = "$2b$"
    salt: str = ""
    checksum: str = ""

    def __init__(self, rounds: int = 12, ident: str = "$2b$"):
        self.rounds = rounds
        self.ident = ident

    def _get_config(self) -> bytes:
        config = bcrypt.gensalt(
            self.rounds, prefix=self.ident.strip("$").encode("ascii")
        )
        self.salt = config.decode("ascii")[len(self.ident) + 3 :]
        return config

    def to_string(self) -> str:
        return "%s%02d$%s%s" % (self.ident, self.rounds, self.salt, self.checksum)

    def hash(self, key: str) -> str:
        validate_secret(key)
        config = self._get_config()
        hash_ = bcrypt.hashpw(key.encode("utf-8"), config)
        if not hash_.startswith(config) or len(hash_) != len(config) + 31:
            raise ValueError("bcrypt.hashpw returned an invalid hash")
        self.checksum = hash_[-31:].decode("ascii")
        return self.to_string()

    def verify(self, key: str, hash: str) -> bool:
        validate_secret(key)

        if hash is None:
            self.hash(_dummy_secret)
            return False

        return bcrypt.checkpw(key.encode("utf-8"), hash.encode("utf-8"))


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
pwd_context = BcryptPasswordHandler()


def validate_secret(secret):
    """ensure secret has correct type & size"""
    if not isinstance(secret, (str, bytes)):
        raise TypeError("secret must be a string or bytes")
    if len(secret) > MAX_PASSWORD_SIZE:
        raise ValueError(
            f"secret is too long, maximum length is {MAX_PASSWORD_SIZE} characters"
        )


def generate_secret_key():
    return secrets.token_hex(32)


def authenticate(db, cls, identifier: str, key: str) -> CredentialedEntity:
    entity: CredentialedEntity = db.get_credentialed_entity(identifier, cls)
    if entity is None:
        return False
    if not pwd_context.verify(key, entity.hashed_key):
        return False
    return entity


class AuthenticationError(Exception): ...


def hash_key(key):
    return pwd_context.hash(key)


def create_access_token(
    *,
    data: dict,
    secret_key: str,
    expires_seconds: Optional[int] = 900,
    jwt_algorithm: Optional[str] = "HS256",
) -> str:
    to_encode = data.copy()

    expire = datetime.utcnow() + timedelta(seconds=expires_seconds)
    to_encode.update({"exp": expire})

    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=jwt_algorithm)
    return encoded_jwt


def get_token_data(
    *, token: str, secret_key: str, jwt_algorithm: Optional[str] = "HS256"
) -> TokenData:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, secret_key, algorithms=[jwt_algorithm])

        token_data = TokenData(entity=payload.get("sub"), scopes=payload.get("scopes"))
    except JWTError:
        raise credentials_exception

    return token_data
