import pytest
from copy import copy

import uvicorn

from alchemiscale.settings import get_base_api_settings
from alchemiscale.base.api import get_s3os_depends
from alchemiscale.interface import api, client

from alchemiscale.tests.integration.interface.utils import get_user_settings_override
from alchemiscale.tests.integration.utils import running_service


## user client


@pytest.fixture(scope="module")
def user_api(s3os_server):
    def get_s3os_override():
        return s3os_server

    overrides = copy(api.app.dependency_overrides)

    api.app.dependency_overrides[get_base_api_settings] = get_user_settings_override
    api.app.dependency_overrides[get_s3os_depends] = get_s3os_override
    yield api.app
    api.app.dependency_overrides = overrides


def run_server(fastapi_app, settings):
    uvicorn.run(
        fastapi_app,
        host=settings.ALCHEMISCALE_API_HOST,
        port=settings.ALCHEMISCALE_API_PORT,
        log_level=settings.ALCHEMISCALE_API_LOGLEVEL,
    )


@pytest.fixture(scope="module")
def uvicorn_server(user_api):
    settings = get_user_settings_override()
    with running_service(
        run_server, port=settings.ALCHEMISCALE_API_PORT, args=(user_api, settings)
    ):
        yield


@pytest.fixture(scope="session")
def cache_dir(tmp_path_factory):
    cache_dir = tmp_path_factory.mktemp("alchemiscale-cache")
    return cache_dir


@pytest.fixture(scope="module")
def user_client(uvicorn_server, user_identity, cache_dir):

    test_client = client.AlchemiscaleClient(
        api_url="http://127.0.0.1:8000/",
        identifier=user_identity["identifier"],
        key=user_identity["key"],
        cache_directory=cache_dir,
        cache_size_limit=int(1073741824 / 4),
    )
    test_client._cache.stats(enable=True, reset=True)

    return test_client


@pytest.fixture(scope="module")
def user_client_wrong_credential(uvicorn_server, user_identity):
    return client.AlchemiscaleClient(
        api_url="http://127.0.0.1:8000/",
        identifier=user_identity["identifier"],
        key="incorrect credential",
    )
