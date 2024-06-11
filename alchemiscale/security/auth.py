"""
:mod:`alchemiscale.security.auth` --- security components for API services
==========================================================================

"""

from datetime import datetime, timedelta
from typing import Union, Optional
import secrets

from fastapi import HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from .models import Token, TokenData, CredentialedEntity


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def generate_secret_key():
    return secrets.token_hex(32)


def authenticate(db, cls, identifier: str, key: str) -> Optional[CredentialedEntity]:
    """Authenticate the given identity+key against the db instance.

    Parameters
    ----------
    db
        State store instance featuring a `get_credentialed_entity` method.
    cls
        The `CredentialedEntity` subclass the identity corresponds to.
    identity
        String identifier for the the identity.
    key
        Secret key string for the identity.

    Returns
    -------
    If successfully authenticated, returns the `CredentialedEntity` subclass instance.
    If not, returns `None`.

    """
    entity: CredentialedEntity = db.get_credentialed_entity(identifier, cls)
    if entity is None:
        return None
    if not pwd_context.verify(key, entity.hashed_key):
        return None
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
