from copy import copy

import pytest
from fastapi.testclient import TestClient

from gufe import AlchemicalNetwork

from alchemiscale.settings import get_base_api_settings
from alchemiscale.interface import api
from alchemiscale.security.models import CredentialedUserIdentity, TokenData
from alchemiscale.security.auth import hash_key
from alchemiscale.base.api import (
    get_token_data_depends,
    get_n4js_depends,
    get_s3os_depends,
)


from alchemiscale.tests.integration.interface.utils import get_user_settings_override


## user api


@pytest.fixture(scope="module")
def user_identity():
    return dict(identifier="test-user-identity", key="strong passphrase lol")


@pytest.fixture(scope="module")
def user_identity_prepped(user_identity):
    return {
        "identifier": user_identity["identifier"],
        "hashed_key": hash_key(user_identity["key"]),
    }


@pytest.fixture(scope="module")
def scopeless_credentialed_user(user_identity_prepped):
    identity = copy(user_identity_prepped)
    identity["identifier"] = identity["identifier"] + "-a"

    user = CredentialedUserIdentity(**identity)
    return user


@pytest.fixture(scope="module")
def single_scoped_credentialed_user(user_identity_prepped, scope_test):
    identity = copy(user_identity_prepped)
    identity["identifier"] = identity["identifier"] + "-b"

    user = CredentialedUserIdentity(**identity, scopes=[scope_test])  # Ensure list
    return user


@pytest.fixture(scope="module")
def fully_scoped_credentialed_user(user_identity_prepped, multiple_scopes):
    user = CredentialedUserIdentity(**user_identity_prepped, scopes=multiple_scopes)
    return user


@pytest.fixture
def n4js_preloaded(
    n4js_fresh,
    network_tyk2,
    multiple_scopes,
    scopeless_credentialed_user,
    single_scoped_credentialed_user,
    fully_scoped_credentialed_user,
):
    n4js = n4js_fresh

    # Spin up a secondary alchemical network
    an2 = AlchemicalNetwork(edges=list(network_tyk2.edges)[:-2], name="incomplete")

    # set starting contents for many of the tests in this module
    for single_scope in multiple_scopes:
        # Create initial network for this scope
        sk1 = n4js.create_network(network_tyk2, single_scope)
        # Create another network for this scope
        sk2 = n4js.create_network(an2, single_scope)

        # add a taskhub for each network
        n4js.create_taskhub(sk1)
        n4js.create_taskhub(sk2)

        # set network states
        n4js.set_network_state([sk1, sk2], ["active", "inactive"])

    # Create user identities
    for user in [
        scopeless_credentialed_user,
        single_scoped_credentialed_user,
        fully_scoped_credentialed_user,
    ]:
        n4js.create_credentialed_entity(user)

    return n4js


@pytest.fixture(scope="module")
def scope_consistent_token_data_depends_override(scope_test):
    """Make a consistent helper to provide an override to the api.app while still accessing fixtures"""

    def get_token_data_depends_override():
        token_data = TokenData(entity="karen-interface", scopes=[str(scope_test)])
        return token_data

    return get_token_data_depends_override


@pytest.fixture(scope="module")
def user_api_no_auth(s3os, scope_consistent_token_data_depends_override):
    def get_s3os_override():
        return s3os

    get_token_data_depends_override = scope_consistent_token_data_depends_override

    overrides = copy(api.app.dependency_overrides)

    api.app.dependency_overrides[get_base_api_settings] = get_user_settings_override
    api.app.dependency_overrides[get_s3os_depends] = get_s3os_override
    api.app.dependency_overrides[get_token_data_depends] = (
        get_token_data_depends_override
    )
    yield api.app
    api.app.dependency_overrides = overrides


@pytest.fixture(scope="module")
def test_client(user_api_no_auth):
    client = TestClient(user_api_no_auth)
    return client
