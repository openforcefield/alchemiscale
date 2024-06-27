from copy import copy
from collections import defaultdict

import pytest
from fastapi.testclient import TestClient

from gufe import AlchemicalNetwork, NonTransformation

from alchemiscale.settings import get_base_api_settings
from alchemiscale.storage.statestore import Neo4jStore
from alchemiscale.compute import api
from alchemiscale.security.models import CredentialedComputeIdentity, TokenData
from alchemiscale.security.auth import hash_key
from alchemiscale.base.api import (
    get_token_data_depends,
    get_s3os_depends,
)

from alchemiscale.tests.integration.compute.utils import get_compute_settings_override


## compute api


@pytest.fixture(scope="module")
def compute_identity():
    return dict(identifier="test-compute-identity", key="strong passphrase lol")


@pytest.fixture(scope="module")
def compute_identity_prepped(compute_identity):
    return {
        "identifier": compute_identity["identifier"],
        "hashed_key": hash_key(compute_identity["key"]),
    }


@pytest.fixture(scope="module")
def scopeless_credentialed_compute(compute_identity_prepped):
    compute = CredentialedComputeIdentity(**compute_identity_prepped)
    return compute


@pytest.fixture(scope="module")
def single_scoped_credentialed_compute(compute_identity_prepped, scope_test):
    identity = copy(compute_identity_prepped)
    identity["identifier"] = identity["identifier"] + "-a"

    compute = CredentialedComputeIdentity(
        **identity, scopes=[scope_test]
    )  # Ensure list
    return compute


@pytest.fixture(scope="module")
def fully_scoped_credentialed_compute(compute_identity_prepped, multiple_scopes):
    identity = copy(compute_identity_prepped)
    identity["identifier"] = identity["identifier"] + "-b"

    compute = CredentialedComputeIdentity(**identity, scopes=multiple_scopes)
    return compute


@pytest.fixture
def second_network_an2(network_tyk2):
    """Create a secondary network fixture"""
    return AlchemicalNetwork(edges=list(network_tyk2.edges)[:-2], name="incomplete")


@pytest.fixture
def n4js_preloaded(
    n4js_fresh,
    network_tyk2,
    transformation,
    second_network_an2,
    multiple_scopes,
    scopeless_credentialed_compute,
    single_scoped_credentialed_compute,
    fully_scoped_credentialed_compute,
):
    n4js: Neo4jStore = n4js_fresh

    # Set up tasks from select set of transformations
    # we need to use second_network_an2 because its edges
    # are a subset of network_tyk2's edges
    transformations = sorted(
        filter(lambda x: type(x) is not NonTransformation, second_network_an2.edges)
    )[0:3]

    # set starting contents for many of the tests in this module
    for single_scope in multiple_scopes:
        # Create initial network for this scope
        sk1, th_sk1, _ = n4js.assemble_network(network_tyk2, single_scope)
        sk2, th_sk2, _ = n4js.assemble_network(second_network_an2, single_scope)

        # Spawn tasks
        task_sks = defaultdict(list)
        for transformation in transformations:
            trans_sk = n4js.get_scoped_key(transformation, single_scope)

            extends = None
            for i in range(3):
                extends = n4js.create_task(trans_sk, extends=extends)
                task_sks[transformation].append(extends)

        # set task priority higher the first transformation
        # used for claim determinism in some tests
        n4js.set_task_priority([task_sks[transformations[0]][0]], 1)

        # add tasks from each transformation selected to each task hubs
        n4js.action_tasks(
            [task_sks[transformation][0] for transformation in transformations], th_sk1
        )

        n4js.action_tasks(
            [task_sks[transformation][0] for transformation in transformations],
            th_sk2,
        )

    # Create compute identities
    for compute in [
        scopeless_credentialed_compute,
        single_scoped_credentialed_compute,
        fully_scoped_credentialed_compute,
    ]:
        n4js.create_credentialed_entity(compute)

    return n4js


@pytest.fixture(scope="module")
def scope_consistent_token_data_depends_override(scope_test):
    """Make a consistent helper to provide an override to the api.app while still accessing fixtures"""

    def get_token_data_depends_override():
        token_data = TokenData(entity="carl-compute", scopes=[str(scope_test)])
        return token_data

    return get_token_data_depends_override


@pytest.fixture
def compute_api_no_auth(
    n4js_preloaded, s3os, scope_consistent_token_data_depends_override
):
    def get_s3os_override():
        return s3os

    overrides = copy(api.app.dependency_overrides)
    get_token_data_depends_override = scope_consistent_token_data_depends_override

    api.app.dependency_overrides[get_base_api_settings] = get_compute_settings_override
    api.app.dependency_overrides[get_s3os_depends] = get_s3os_override
    api.app.dependency_overrides[get_token_data_depends] = (
        get_token_data_depends_override
    )
    yield api.app
    api.app.dependency_overrides = overrides


@pytest.fixture
def test_client(compute_api_no_auth):
    client = TestClient(compute_api_no_auth)
    return client
