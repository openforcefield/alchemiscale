"""
:mod:`alchemiscale.settings` --- settings
=========================================

"""

from functools import lru_cache
from typing import Optional

from pydantic.v1 import BaseSettings


class FrozenSettings(BaseSettings):
    class Config:
        frozen = True


class Neo4jStoreSettings(FrozenSettings):
    """Automatically populates settings from environment variables where they
    match; case-insensitive.

    """

    NEO4J_URL: str
    NEO4J_DBNAME: str = "neo4j"
    NEO4J_USER: str
    NEO4J_PASS: str


class S3ObjectStoreSettings(FrozenSettings):
    """Automatically populates settings from environment variables where they
    match; case-insensitive.

    If deploying APIs that use the S3ObjectStore on an EC2 host or other
    role-based resource (e.g. an ECS container), then don't set these.
    Instead rely on the IAM role of that resource for granting access to S3.

    """

    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_SESSION_TOKEN: Optional[str] = None
    AWS_S3_BUCKET: str
    AWS_S3_PREFIX: str
    AWS_DEFAULT_REGION: str


class JWTSettings(FrozenSettings):
    """Automatically populates settings from environment variables where they
    match; case-insensitive.

    """

    JWT_SECRET_KEY: str
    JWT_EXPIRE_SECONDS: int = 1800
    JWT_ALGORITHM: str = "HS256"


class BaseAPISettings(Neo4jStoreSettings, S3ObjectStoreSettings, JWTSettings): ...


class APISettings(BaseAPISettings):
    """Automatically populates settings from environment variables where they
    match; case-insensitive.

    """

    ALCHEMISCALE_API_HOST: str = "127.0.0.1"
    ALCHEMISCALE_API_PORT: int = 80
    ALCHEMISCALE_API_LOGLEVEL: str = "info"


class ComputeAPISettings(BaseAPISettings):
    """Automatically populates settings from environment variables where they
    match; case-insensitive.

    """

    ALCHEMISCALE_COMPUTE_API_HOST: str = "127.0.0.1"
    ALCHEMISCALE_COMPUTE_API_PORT: int = 80
    ALCHEMISCALE_COMPUTE_API_LOGLEVEL: str = "info"
    ALCHEMISCALE_COMPUTE_API_REGISTRATION_EXPIRE_SECONDS: int = 1800


@lru_cache()
def get_neo4jstore_settings():
    return Neo4jStoreSettings()


@lru_cache()
def get_s3objectstore_settings():
    return S3ObjectStoreSettings()


@lru_cache()
def get_jwt_settings():
    return JWTSettings()


@lru_cache()
def get_base_api_settings():
    return BaseAPISettings()


@lru_cache()
def get_api_settings():
    return APISettings()


@lru_cache()
def get_compute_api_settings():
    return ComputeAPISettings()
