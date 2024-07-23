"""
:mod:`alchemiscale.compute.api` --- compute API components
==========================================================

"""

from typing import Any, Dict, List, Optional
import os
import json
from datetime import datetime, timedelta
import random

from fastapi import FastAPI, APIRouter, Body, Depends
from fastapi.middleware.gzip import GZipMiddleware
from gufe.tokenization import GufeTokenizable, JSON_HANDLER

from ..base.api import (
    QueryGUFEHandler,
    scope_params,
    get_token_data_depends,
    get_n4js_depends,
    get_s3os_depends,
    base_router,
    get_cred_entity,
    validate_scopes,
    validate_scopes_query,
    minimize_scope_space,
    _check_store_connectivity,
    gufe_to_json,
    GzipRoute,
)
from ..settings import (
    get_base_api_settings,
    get_compute_api_settings,
    ComputeAPISettings,
)
from ..storage.statestore import Neo4jStore, get_n4js
from ..storage.objectstore import S3ObjectStore
from ..storage.models import (
    ProtocolDAGResultRef,
    ComputeServiceID,
    ComputeServiceRegistration,
    TaskStatusEnum,
)
from ..models import Scope, ScopedKey
from ..security.auth import get_token_data, oauth2_scheme
from ..security.models import (
    Token,
    TokenData,
    CredentialedComputeIdentity,
)


app = FastAPI(title="AlchemiscaleComputeAPI")
app.dependency_overrides[get_base_api_settings] = get_compute_api_settings
app.include_router(base_router)
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)


def get_cred_compute():
    return CredentialedComputeIdentity


app.dependency_overrides[get_cred_entity] = get_cred_compute


router = APIRouter(
    dependencies=[Depends(get_token_data_depends)],
)
router.route_class = GzipRoute


@app.get("/ping")
def ping():
    return {"api": "AlchemiscaleComputeAPI"}


@router.get("/info")
def info():
    return {"message": "nothing yet"}


@router.get("/check")
def check(
    n4js: Neo4jStore = Depends(get_n4js_depends),
    s3os: S3ObjectStore = Depends(get_s3os_depends),
):
    # check connectivity of storage components
    # if no exception raised, all good
    _check_store_connectivity(n4js, s3os)


@router.get("/identities/{identity_identifier}/scopes")
def list_scopes(
    *,
    identity_identifier,
    n4js: Neo4jStore = Depends(get_n4js_depends),
    token: TokenData = Depends(get_token_data_depends),
) -> List[str]:
    scopes = n4js.list_scopes(identity_identifier, CredentialedComputeIdentity)
    return [str(scope) for scope in scopes]


@router.post("/computeservice/{compute_service_id}/register")
def register_computeservice(
    compute_service_id,
    n4js: Neo4jStore = Depends(get_n4js_depends),
):
    now = datetime.utcnow()
    csreg = ComputeServiceRegistration(
        identifier=ComputeServiceID(compute_service_id), registered=now, heartbeat=now
    )

    compute_service_id_ = n4js.register_computeservice(csreg)

    return compute_service_id_


@router.post("/computeservice/{compute_service_id}/deregister")
def deregister_computeservice(
    compute_service_id,
    n4js: Neo4jStore = Depends(get_n4js_depends),
):
    compute_service_id_ = n4js.deregister_computeservice(
        ComputeServiceID(compute_service_id)
    )

    return compute_service_id_


@router.post("/computeservice/{compute_service_id}/heartbeat")
def heartbeat_computeservice(
    compute_service_id,
    n4js: Neo4jStore = Depends(get_n4js_depends),
    settings: ComputeAPISettings = Depends(get_base_api_settings),
):
    now = datetime.utcnow()

    # expire any stale registrations, along with their claims
    expire_delta = timedelta(
        seconds=settings.ALCHEMISCALE_COMPUTE_API_REGISTRATION_EXPIRE_SECONDS
    )
    expire_time = now - expire_delta
    n4js.expire_registrations(expire_time)

    compute_service_id_ = n4js.heartbeat_computeservice(compute_service_id, now)

    return compute_service_id_


@router.get("/taskhubs")
def query_taskhubs(
    *,
    return_gufe: bool = False,
    scope: Scope = Depends(scope_params),
    n4js: Neo4jStore = Depends(get_n4js_depends),
    token: TokenData = Depends(get_token_data_depends),
):
    # intersect query scopes with accessible scopes in the token
    query_scopes = validate_scopes_query(scope, token)
    taskhubs_handler = QueryGUFEHandler(return_gufe)

    # query each scope
    # loop might be more removable in the future with a Union like operator on scopes
    for single_query_scope in query_scopes:
        # add new task hubs
        taskhubs_handler.update_results(
            n4js.query_taskhubs(
                scope=single_query_scope, return_gufe=taskhubs_handler.return_gufe
            )
        )

    return taskhubs_handler.format_return()


@router.post("/taskhubs/{taskhub_scoped_key}/claim")
def claim_taskhub_tasks(
    taskhub_scoped_key,
    *,
    compute_service_id: str = Body(),
    count: int = Body(),
    protocols: Optional[List[str]] = Body(None, embed=True),
    n4js: Neo4jStore = Depends(get_n4js_depends),
    token: TokenData = Depends(get_token_data_depends),
):
    sk = ScopedKey.from_str(taskhub_scoped_key)
    validate_scopes(sk.scope, token)

    tasks = n4js.claim_taskhub_tasks(
        taskhub=taskhub_scoped_key,
        compute_service_id=ComputeServiceID(compute_service_id),
        count=count,
        protocols=protocols,
    )

    return [str(t) if t is not None else None for t in tasks]


@router.post("/claim")
def claim_tasks(
    scopes: List[Scope] = Body(),
    compute_service_id: str = Body(),
    count: int = Body(),
    protocols: Optional[List[str]] = Body(None, embed=True),
    n4js: Neo4jStore = Depends(get_n4js_depends),
    token: TokenData = Depends(get_token_data_depends),
):
    # intersect query scopes with accessible scopes in the token
    scopes_reduced = minimize_scope_space(scopes)
    query_scopes = []
    for scope in scopes_reduced:
        query_scopes.extend(validate_scopes_query(scope, token))

    taskhubs = dict()
    # query each scope for available taskhubs
    # loop might be more removable in the future with a Union like operator on scopes
    for single_query_scope in set(query_scopes):
        taskhubs.update(n4js.query_taskhubs(scope=single_query_scope, return_gufe=True))

    # list of tasks to return
    tasks = []

    if len(taskhubs) == 0:
        return []

    # claim tasks from taskhubs based on weight; keep going till we hit our
    # total desired task count, or we run out of taskhubs to draw from
    while len(tasks) < count and len(taskhubs) > 0:
        weights = [th.weight for th in taskhubs.values()]

        if sum(weights) == 0:
            break

        # based on weights, choose taskhub to draw from
        taskhub: ScopedKey = random.choices(list(taskhubs.keys()), weights=weights)[0]

        # claim tasks from the taskhub
        claimed_tasks = n4js.claim_taskhub_tasks(
            taskhub,
            compute_service_id=ComputeServiceID(compute_service_id),
            count=(count - len(tasks)),
            protocols=protocols,
        )

        # gather up claimed tasks, if present
        for t in claimed_tasks:
            if t is not None:
                tasks.append(t)

        # remove this taskhub from the options available; repeat
        taskhubs.pop(taskhub)

    return [str(t) for t in tasks] + [None] * (count - len(tasks))


@router.get("/tasks/{task_scoped_key}/transformation")
def get_task_transformation(
    task_scoped_key,
    *,
    n4js: Neo4jStore = Depends(get_n4js_depends),
    token: TokenData = Depends(get_token_data_depends),
):
    sk = ScopedKey.from_str(task_scoped_key)
    validate_scopes(sk.scope, token)

    transformation: ScopedKey

    transformation, _ = n4js.get_task_transformation(
        task=task_scoped_key,
        return_gufe=False,
    )

    return str(transformation)


@router.get("/tasks/{task_scoped_key}/transformation/gufe")
def retrieve_task_transformation(
    task_scoped_key,
    *,
    n4js: Neo4jStore = Depends(get_n4js_depends),
    s3os: S3ObjectStore = Depends(get_s3os_depends),
    token: TokenData = Depends(get_token_data_depends),
):
    sk = ScopedKey.from_str(task_scoped_key)
    validate_scopes(sk.scope, token)

    transformation_sk, protocoldagresultref_sk = n4js.get_task_transformation(
        task=task_scoped_key, return_gufe=False
    )

    transformation = n4js.get_gufe(transformation_sk)

    if protocoldagresultref_sk:
        protocoldagresultref = n4js.get_gufe(protocoldagresultref_sk)
        pdr_sk = ScopedKey(gufe_key=protocoldagresultref.obj_key, **sk.scope.dict())

        # we keep this as a string to avoid useless deserialization/reserialization here
        try:
            pdr: str = s3os.pull_protocoldagresult(
                pdr_sk, transformation_sk, return_as="json", ok=True
            )
        except:
            # if we fail to get the object with the above, fall back to
            # location-based retrieval
            pdr: str = s3os.pull_protocoldagresult(
                location=protocoldagresultref.location,
                return_as="json",
                ok=True,
            )

    else:
        pdr = None

    return (gufe_to_json(transformation), pdr)


# TODO: support compression performed client-side
@router.post("/tasks/{task_scoped_key}/results", response_model=ScopedKey)
def set_task_result(
    task_scoped_key,
    *,
    protocoldagresult: str = Body(embed=True),
    compute_service_id: Optional[str] = Body(embed=True),
    n4js: Neo4jStore = Depends(get_n4js_depends),
    s3os: S3ObjectStore = Depends(get_s3os_depends),
    token: TokenData = Depends(get_token_data_depends),
):
    task_sk = ScopedKey.from_str(task_scoped_key)
    validate_scopes(task_sk.scope, token)

    pdr = json.loads(protocoldagresult, cls=JSON_HANDLER.decoder)
    pdr = GufeTokenizable.from_dict(pdr)

    tf_sk, _ = n4js.get_task_transformation(
        task=task_scoped_key,
        return_gufe=False,
    )

    # push the ProtocolDAGResult to the object store
    protocoldagresultref: ProtocolDAGResultRef = s3os.push_protocoldagresult(
        pdr, transformation=tf_sk, creator=compute_service_id
    )

    # push the reference to the state store
    result_sk: ScopedKey = n4js.set_task_result(
        task=task_sk, protocoldagresultref=protocoldagresultref
    )

    # if success, set task complete, remove from all hubs
    # otherwise, set as errored, leave in hubs
    if protocoldagresultref.ok:
        n4js.set_task_complete(tasks=[task_sk])
    else:
        n4js.set_task_error(tasks=[task_sk])

    return result_sk


### add router

app.include_router(router)
