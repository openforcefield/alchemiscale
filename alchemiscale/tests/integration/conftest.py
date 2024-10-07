"""Components for standing up services for integration tests, including databases.

"""

## storage
### below from `py2neo.test.integration.conftest.py`

import os
from time import sleep
from pathlib import Path
from typing import Union

from grolt import Neo4jService, Neo4jDirectorySpec, docker
from grolt.security import install_self_signed_certificate
from pytest import fixture
from moto import mock_aws
from moto.server import ThreadedMotoServer

from neo4j import GraphDatabase

from gufe import ChemicalSystem, NonTransformation, Transformation, AlchemicalNetwork
from gufe.protocols.protocoldag import execute_DAG
from gufe.tests.test_protocol import DummyProtocol, BrokenProtocol
from openfe_benchmarks import tyk2

from alchemiscale.models import Scope
from alchemiscale.settings import Neo4jStoreSettings, S3ObjectStoreSettings
from alchemiscale.storage.statestore import Neo4jStore
from alchemiscale.storage.objectstore import S3ObjectStore, get_s3os
from alchemiscale.storage.models import ComputeServiceID


NEO4J_PROCESS = {}
NEO4J_VERSION = os.getenv("NEO4J_VERSION", "")


class DeploymentProfile(object):
    def __init__(self, release=None, topology=None, cert=None, schemes=None):
        self.release = release
        self.topology = topology  # "CE|EE-SI|EE-C3|EE-C3-R2"
        self.cert = cert
        self.schemes = schemes

    def __str__(self):
        server = "%s.%s %s" % (self.release[0], self.release[1], self.topology)
        if self.cert:
            server += " %s" % (self.cert,)
        schemes = " ".join(self.schemes)
        return "[%s]-[%s]" % (server, schemes)


class TestProfile:
    def __init__(self, deployment_profile=None, scheme=None):
        self.deployment_profile = deployment_profile
        self.scheme = scheme
        assert self.topology == "CE"

    def __str__(self):
        extra = "%s" % (self.topology,)
        if self.cert:
            extra += "; %s" % (self.cert,)
        bits = [
            "Neo4j/%s.%s (%s)" % (self.release[0], self.release[1], extra),
            "over",
            "'%s'" % self.scheme,
        ]
        return " ".join(bits)

    @property
    def release(self):
        return self.deployment_profile.release

    @property
    def topology(self):
        return self.deployment_profile.topology

    @property
    def cert(self):
        return self.deployment_profile.cert

    @property
    def release_str(self):
        return ".".join(map(str, self.release))

    def generate_uri(self, service_name=None):
        if self.cert == "full":
            raise NotImplementedError("Full certificates are not yet supported")
        elif self.cert == "ssc":
            certificates_dir = install_self_signed_certificate(self.release_str)
            dir_spec = Neo4jDirectorySpec(certificates_dir=certificates_dir)
        else:
            dir_spec = None
        with Neo4jService(
            name=service_name,
            image=self.release_str,
            auth=("neo4j", "password"),
            dir_spec=dir_spec,
            config={},
        ) as service:
            uris = [router.uri(self.scheme) for router in service.routers()]
            yield service, uris[0]


# TODO: test with full certificates
neo4j_deployment_profiles = [
    DeploymentProfile(release=(5, 19), topology="CE", schemes=["bolt"]),
]

if NEO4J_VERSION == "LATEST":
    neo4j_deployment_profiles = neo4j_deployment_profiles[:1]
elif NEO4J_VERSION == "4.x":
    neo4j_deployment_profiles = [
        profile for profile in neo4j_deployment_profiles if profile.release[0] == 4
    ]
elif NEO4J_VERSION == "4.4":
    neo4j_deployment_profiles = [
        profile for profile in neo4j_deployment_profiles if profile.release == (4, 4)
    ]


neo4j_test_profiles = [
    TestProfile(deployment_profile, scheme=scheme)
    for deployment_profile in neo4j_deployment_profiles
    for scheme in deployment_profile.schemes
]


@fixture(
    scope="session", params=neo4j_test_profiles, ids=list(map(str, neo4j_test_profiles))
)
def test_profile(request):
    test_profile = request.param
    yield test_profile


@fixture(scope="session")
def neo4j_service_and_uri(test_profile):
    for service, uri in test_profile.generate_uri("py2neo"):
        yield service, uri

    # prune all docker volumes left behind
    docker.volumes.prune()
    return


@fixture(scope="session")
def uri(neo4j_service_and_uri):
    _, uri = neo4j_service_and_uri
    return uri


# TODO: this should be pulling from the defined profile
@fixture(scope="session")
def graph(uri):
    return GraphDatabase.driver(
        uri,
        auth=("neo4j", "password"),
    )


## data
### below specific to alchemiscale


@fixture(scope="module")
def n4js(graph):
    return Neo4jStore(graph)


@fixture
def n4js_fresh(graph):
    n4js = Neo4jStore(graph)

    n4js.reset()
    n4js.initialize()

    return n4js


@fixture(scope="module")
def s3objectstore_settings():
    os.environ["AWS_ACCESS_KEY_ID"] = "test-key-id"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test-key"
    os.environ["AWS_SESSION_TOKEN"] = "test-session-token"
    os.environ["AWS_S3_BUCKET"] = "test-bucket"
    os.environ["AWS_S3_PREFIX"] = "test-prefix"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    return S3ObjectStoreSettings()


@fixture(scope="module")
def s3os_server(s3objectstore_settings):
    server = ThreadedMotoServer()
    server.start()

    s3os = get_s3os(s3objectstore_settings, endpoint_url="http://127.0.0.1:5000")
    s3os.initialize()

    yield s3os

    server.stop()


@fixture
def s3os_server_fresh(s3os_server):
    s3os_server.reset()
    s3os_server.initialize()

    return s3os_server


@fixture(scope="module")
def s3os(s3objectstore_settings):
    with mock_aws():
        s3os = get_s3os(s3objectstore_settings)
        s3os.initialize()

        yield s3os


# test alchemical networks


## define varying protocols to simulate protocol variety
class DummyProtocolA(DummyProtocol):
    pass


class DummyProtocolB(DummyProtocol):
    pass


class DummyProtocolC(DummyProtocol):
    pass


# TODO: add in atom mapping once `gufe`#35 is settled


@fixture(scope="module")
def network_tyk2():
    tyk2s = tyk2.get_system()

    solvated = {
        l.name: ChemicalSystem(
            components={"ligand": l, "solvent": tyk2s.solvent_component},
            name=f"{l.name}_water",
        )
        for l in tyk2s.ligand_components
    }
    complexes = {
        l.name: ChemicalSystem(
            components={
                "ligand": l,
                "solvent": tyk2s.solvent_component,
                "protein": tyk2s.protein_component,
            },
            name=f"{l.name}_complex",
        )
        for l in tyk2s.ligand_components
    }

    complex_network = [
        Transformation(
            stateA=complexes[edge[0]],
            stateB=complexes[edge[1]],
            protocol=DummyProtocolA(settings=DummyProtocolA.default_settings()),
            name=f"{edge[0]}_to_{edge[1]}_complex",
        )
        for edge in tyk2s.connections
    ]
    solvent_network = [
        Transformation(
            stateA=solvated[edge[0]],
            stateB=solvated[edge[1]],
            protocol=DummyProtocolB(settings=DummyProtocolB.default_settings()),
            name=f"{edge[0]}_to_{edge[1]}_solvent",
        )
        for edge in tyk2s.connections
    ]

    nontransformations = []
    for cs in list(solvated.values()) + list(complexes.values()):
        nt = NonTransformation(
            system=cs,
            protocol=DummyProtocolC(DummyProtocolC.default_settings()),
            name=f"f{cs.name}_nt",
        )
        nontransformations.append(nt)

    return AlchemicalNetwork(
        edges=(solvent_network + complex_network + nontransformations),
        name="tyk2_relative_benchmark",
    )


def get_edge_type(
    network: AlchemicalNetwork, edge_class
) -> Union[Transformation, NonTransformation]:
    for tf in sorted(network.edges):
        if type(tf) is edge_class:
            return tf
    raise RuntimeError("Network does not contain a `{edge_class.__qualname__}`")


@fixture(scope="module")
def transformation(network_tyk2):
    return get_edge_type(network_tyk2, Transformation)


@fixture(scope="module")
def nontransformation(network_tyk2):
    return get_edge_type(network_tyk2, NonTransformation)


@fixture(scope="module")
def chemicalsystem(network_tyk2):
    return list(network_tyk2.nodes)[0]


@fixture(scope="module")
def protocoldagresults(tmpdir_factory, transformation):
    pdrs = []
    for i in range(3):
        # Use tempdir_factory instead of tempdir to handle session level scope correctly
        protocoldag = transformation.create()

        # execute the task
        with tmpdir_factory.mktemp("protocol_dag").as_cwd():
            shared_basedir = Path("shared").absolute()
            shared_basedir.mkdir()
            scratch_basedir = Path("scratch").absolute()
            scratch_basedir.mkdir()

            protocoldagresult = execute_DAG(
                protocoldag,
                shared_basedir=shared_basedir,
                scratch_basedir=scratch_basedir,
            )

        pdrs.append(protocoldagresult)
    return pdrs


@fixture
def network_tyk2_failure(network_tyk2):
    transformation = get_edge_type(network_tyk2, Transformation)

    broken_transformation = Transformation(
        stateA=transformation.stateA,
        stateB=transformation.stateB,
        protocol=BrokenProtocol(settings=BrokenProtocol.default_settings()),
        name="broken",
    )

    return AlchemicalNetwork(
        edges=[broken_transformation] + list(network_tyk2.edges), name="tyk2_broken"
    )


@fixture
def transformation_failure(network_tyk2_failure):
    return [t for t in network_tyk2_failure.edges if t.name == "broken"][0]


@fixture
def protocoldagresults_failure(tmpdir_factory, transformation_failure):
    pdrs = []
    for i in range(3):
        # Use tempdir_factory instead of tempdir to handle session level scope correctly
        protocoldag = transformation_failure.create()

        # execute the task
        with tmpdir_factory.mktemp("protocol_dag").as_cwd():
            shared_basedir = Path("shared").absolute()
            shared_basedir.mkdir()
            scratch_basedir = Path("scratch").absolute()
            scratch_basedir.mkdir()

            protocoldagresult = execute_DAG(
                protocoldag,
                shared_basedir=shared_basedir,
                scratch_basedir=scratch_basedir,
                raise_error=False,
            )

        pdrs.append(protocoldagresult)
    return pdrs


@fixture(scope="module")
def scope_test():
    """Primary scope for individual tests"""
    return Scope(org="test_org", campaign="test_campaign", project="test_project")


@fixture(scope="module")
def multiple_scopes(scope_test):
    scopes = [scope_test]  # Append initial test
    # Augment
    scopes.extend(
        [
            Scope(
                org=f"test_org_{x}",
                campaign=f"test_campaign_{x}",
                project=f"test_project_{x}",
            )
            for x in range(1, 3)
        ]
    )
    return scopes


@fixture(scope="module")
def compute_service_id():
    return ComputeServiceID("compute-service-123")
