from pathlib import Path
from typing import Union, Optional, List, Dict, Tuple
from pydantic.v1 import BaseModel, Field

from ..models import Scope, ScopedKey


class ComputeServiceSettings(BaseModel):
    """Core settings schema for a compute service."""

    class Config:
        arbitrary_types_allowed = True

    api_url: str = Field(
        ..., description="URL of the compute API to execute Tasks for."
    )
    identifier: str = Field(
        ..., description="Identifier for the compute identity used for authentication."
    )
    key: str = Field(
        ..., description="Credential for the compute identity used for authentication."
    )
    name: str = Field(
        ...,
        description=(
            "The name to give this compute service; used for Task provenance, so "
            "typically set to a distinct value to distinguish different compute "
            "resources, e.g. different hosts or HPC clusters."
        ),
    )
    shared_basedir: Path = Field(
        ..., description="Filesystem path to use for `ProtocolDAG` `shared` space."
    )
    scratch_basedir: Path = Field(
        ..., description="Filesystem path to use for `ProtocolUnit` `scratch` space."
    )
    keep_shared: bool = Field(
        False,
        description="If True, don't remove shared directories for `ProtocolDAG`s after completion.",
    )
    keep_scratch: bool = Field(
        False,
        description="If True, don't remove scratch directories for `ProtocolUnit`s after completion.",
    )
    n_retries: int = Field(
        3,
        description="Number of times to attempt a given Task on failure.",
    )
    sleep_interval: int = Field(
        30, description="Time in seconds to sleep if no Tasks claimed from compute API."
    )
    heartbeat_interval: int = Field(
        300, description="Frequency at which to send heartbeats to compute API."
    )
    scopes: Optional[List[Scope]] = Field(
        None,
        description="Scopes to limit Task claiming to; defaults to all Scopes accessible by compute identity.",
    )
    protocols: Optional[List[str]] = Field(
        None,
        description="Names of Protocols to run with this service; `None` means no restriction.",
    )
    claim_limit: int = Field(
        1000, description="Maximum number of Tasks to claim at a time from a TaskHub."
    )
    loglevel: str = Field(
        "WARN",
        description="The loglevel at which to report; see the :mod:`logging` docs for available levels.",
    )
    logfile: Optional[Path] = Field(
        None,
        description="Path to file for logging output; if not set, logging will only go to STDOUT.",
    )
    client_max_retries: int = Field(
        5,
        description=(
            "Maximum number of times to retry a request. "
            "In the case the API service is unresponsive an expoenential backoff "
            "is applied with retries until this number is reached. "
            "If set to -1, retries will continue indefinitely until success."
        ),
    )
    client_retry_base_seconds: float = Field(
        2.0,
        description="The base number of seconds to use for exponential backoff. Must be greater than 1.0.",
    )
    client_retry_max_seconds: float = Field(
        60.0,
        description="Maximum number of seconds to sleep between retries; avoids runaway exponential backoff while allowing for many retries.",
    )
    client_verify: bool = Field(
        True,
        description="Whether to verify SSL certificate presented by the API server.",
    )
