"""
:mod:`alchemiscale.storage.models` --- data models for storage components
=========================================================================

"""

from abc import abstractmethod
from copy import copy
from datetime import datetime
from enum import Enum
from typing import Union, Dict, Optional
from uuid import uuid4
import hashlib


from pydantic.v1 import BaseModel, Field
from gufe.tokenization import GufeTokenizable, GufeKey

from ..models import ScopedKey, Scope


class ComputeServiceID(str): ...


class ComputeServiceRegistration(BaseModel):
    """Registration for AlchemiscaleComputeService instances."""

    identifier: ComputeServiceID
    registered: datetime
    heartbeat: datetime

    def __repr__(self):  # pragma: no cover
        return f"<ComputeServiceRegistration('{str(self)}')>"

    def __str__(self):
        return "-".join([self.identifier])

    @classmethod
    def from_now(cls, identifier: ComputeServiceID):
        now = datetime.utcnow()
        return cls(identifier=identifier, registered=now, heartbeat=now)

    def to_dict(self):
        dct = self.dict()
        dct["identifier"] = str(self.identifier)

        return dct

    @classmethod
    def from_dict(cls, dct):
        dct_ = copy(dct)
        dct_["identifier"] = ComputeServiceID(dct_["identifier"])

        return cls(**dct_)


class TaskProvenance(BaseModel):
    computeserviceid: ComputeServiceID
    datetime_start: datetime
    datetime_end: datetime

    # this should include versions of various libraries


class TaskStatusEnum(Enum):
    complete = "complete"
    waiting = "waiting"
    running = "running"
    error = "error"
    invalid = "invalid"
    deleted = "deleted"


class Task(GufeTokenizable):
    """A Task that can be used to generate a `ProtocolDAG` on a
    compute node.

    Attributes
    ----------
    status
        Status of the task.
    priority
        Priority of the task; 1 is highest, larger values indicate lower priority.
    claim
        Identifier of the compute service that has a claim on this task.
    datetime_created

    """

    status: TaskStatusEnum
    priority: int
    claim: Optional[str]
    datetime_created: Optional[datetime]
    creator: Optional[str]
    extends: Optional[str]

    def __init__(
        self,
        *,
        status: Union[str, TaskStatusEnum] = TaskStatusEnum.waiting,
        priority: int = 10,
        datetime_created: Optional[datetime] = None,
        creator: Optional[str] = None,
        extends: Optional[str] = None,
        claim: Optional[str] = None,
        _key: str = None,
    ):
        if _key is not None:
            self._key = GufeKey(_key)

        self.status: TaskStatusEnum = TaskStatusEnum(status)
        self.priority = priority

        self.datetime_created = (
            datetime_created if datetime_created is not None else datetime.utcnow()
        )

        self.creator = creator
        self.extends = extends
        self.claim = claim

    def _gufe_tokenize(self):
        # tokenize with uuid
        return uuid4().hex

    def _to_dict(self):
        return {
            "status": self.status.value,
            "priority": self.priority,
            "datetime_created": self.datetime_created,
            "creator": self.creator,
            "extends": self.extends,
            "claim": self.claim,
            "_key": str(self.key),
        }

    @classmethod
    def _from_dict(cls, d):
        return cls(**d)

    @classmethod
    def _defaults(cls):
        return super()._defaults()


class TaskHub(GufeTokenizable):
    """

    Attributes
    ----------
    network : str
        ScopedKey of the AlchemicalNetwork this TaskHub corresponds to.
        Used to ensure that there is only one TaskHub for a given
        AlchemicalNetwork using neo4j constraints.
    weight : float
        Value between 0.0 and 1.0 giving the weight of this TaskHub. This
        number is used to allocate attention to this TaskHub relative to
        others by a ComputeService. TaskHub with equal weight will be given
        equal attention; a TaskHub with greater weight than another will
        receive more attention.

        Setting the weight to 0.0 will give the TaskHub no attention,
        effectively disabling it.

    """

    network: str
    weight: float

    def __init__(self, network: ScopedKey, weight: int = 0.5):
        self.network = network
        self.weight = weight

    def _gufe_tokenize(self):
        return hashlib.md5(
            str(self.network).encode(), usedforsecurity=False
        ).hexdigest()

    def _to_dict(self):
        return {
            "network": self.network,
            "weight": self.weight,
        }

    @classmethod
    def _from_dict(cls, d):
        return cls(**d)

    @classmethod
    def _defaults(cls):
        return super()._defaults()


class Mark(GufeTokenizable):

    def __init__(self, target: ScopedKey):
        self.target = str(target)

    @abstractmethod
    def _to_dict(self):
        raise NotImplementedError

    def _gufe_tokenize(self):
        hash_string = str(self.target)
        return hashlib.md5(hash_string.encode(), usedforsecurity=False).hexdigest()

    @classmethod
    def _from_dict(cls, d):
        return cls(**d)

    @classmethod
    def _defaults(cls):
        return super()._defaults()


class NetworkStateEnum(Enum):
    active = "active"
    inactive = "inactive"
    invalid = "invalid"
    deleted = "deleted"


class NetworkMark(Mark):
    """Mark object for AlchemicalNetworks.

    Attributes
    ----------
    network : str
        ScopedKey of the AlchemicalNetwork this NetworkMark corresponds to.
        Used to ensure that there is only one NetworkMark for a given
        AlchemicalNetwork using neo4j constraints.

    state : NetworkStateEnum
        State of the AlchemicalNetwork, stored on this NetworkMark.
    """

    network: str
    state: NetworkStateEnum

    def __init__(
        self,
        target: ScopedKey,
        state: Union[str, NetworkStateEnum] = NetworkStateEnum.active,
    ):
        self.state = state
        super().__init__(target)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state_value):
        try:
            self._state = NetworkStateEnum(state_value)
        except ValueError:
            valid_states_string = ", ".join(sorted([i.value for i in NetworkStateEnum]))
            msg = f"`state` = {state_value} must be one of the following: {valid_states_string}"
            raise ValueError(msg)

    def _to_dict(self):
        return {"target": self.target, "state": self._state.value}


class TaskArchive(GufeTokenizable):
    ...

    def _to_dict(self):
        return {}

    @classmethod
    def _from_dict(cls, d):
        return cls(**d)

    @classmethod
    def _defaults(cls):
        return super()._defaults()


class ObjectStoreRef(GufeTokenizable):
    location: Optional[str]
    obj_key: Optional[GufeKey]
    scope: Scope

    def __init__(self, *, location: str = None, obj_key: GufeKey = None, scope: Scope):
        self.location = location
        self.obj_key = GufeKey(obj_key) if obj_key is not None else None
        self.scope = scope

    def _to_dict(self):
        return {
            "location": self.location,
            "obj_key": str(self.obj_key),
            "scope": str(self.scope),
        }

    @classmethod
    def _from_dict(cls, d):
        d_ = copy(d)
        d_["scope"] = Scope.from_str(d["scope"])
        return cls(**d_)

    @classmethod
    def _defaults(cls):
        return super()._defaults()


class ProtocolDAGResultRef(ObjectStoreRef):
    ok: bool

    def __init__(
        self,
        *,
        location: Optional[str] = None,
        obj_key: GufeKey,
        scope: Scope,
        ok: bool,
        datetime_created: Optional[datetime] = None,
        creator: Optional[str] = None,
    ):
        self.location = location
        self.obj_key = GufeKey(obj_key)
        self.scope = scope
        self.ok = ok
        self.datetime_created = datetime_created
        self.creator = creator

    def _to_dict(self):
        return {
            "location": self.location,
            "obj_key": str(self.obj_key),
            "scope": str(self.scope),
            "ok": self.ok,
            "datetime_created": (
                self.datetime_created.isoformat()
                if self.datetime_created is not None
                else None
            ),
            "creator": self.creator,
        }

    @classmethod
    def _from_dict(cls, d):
        d_ = copy(d)
        d_["datetime_created"] = (
            datetime.fromisoformat(d["datetime_created"])
            if d.get("received") is not None
            else None
        )

        return super()._from_dict(d_)
