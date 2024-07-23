"""
:mod:`alchemiscale.security.models` --- data models for security components
===========================================================================

"""

from datetime import datetime, timedelta
from typing import List, Union, Optional

from pydantic import BaseModel, field_validator

from ..models import Scope


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    entity: Optional[str] = None
    scopes: List[str] = None


class CredentialedEntity(BaseModel):
    hashed_key: str
    expires: Optional[datetime] = None


class ScopedIdentity(BaseModel):
    identifier: str
    disabled: bool = False
    scopes: List[Union[Scope, str]] = []

    @field_validator("scopes")
    def cast_scopes_to_str(cls, scopes):
        """Ensure that each scope object is correctly cast to its str representation"""
        scopes_ = []
        for scope in scopes:
            if isinstance(scope, Scope):
                scopes_.append(str(scope))
            elif isinstance(scope, str):
                try:
                    scopes_.append(str(Scope.from_str(scope)))
                except:
                    raise ValueError(f"Invalid scope `{scope}` set for `{cls}`")
            else:
                raise ValueError(f"Invalid scope `{scope}` set for `{cls}`")

        return scopes_


class UserIdentity(ScopedIdentity):
    email: Optional[str] = None
    full_name: Optional[str] = None


class CredentialedUserIdentity(UserIdentity, CredentialedEntity): ...


class ComputeIdentity(ScopedIdentity):
    email: Optional[str] = None


class CredentialedComputeIdentity(ComputeIdentity, CredentialedEntity): ...
