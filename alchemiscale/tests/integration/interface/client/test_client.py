import pytest
from time import sleep
from pathlib import Path
from itertools import chain

from gufe import AlchemicalNetwork
from gufe.tokenization import TOKENIZABLE_REGISTRY, GufeKey
from gufe.protocols.protocoldag import execute_DAG
import networkx as nx

from alchemiscale.models import ScopedKey, Scope
from alchemiscale.storage.models import TaskStatusEnum, NetworkStateEnum
from alchemiscale.storage.cypher import cypher_list_from_scoped_keys
from alchemiscale.interface import client
from alchemiscale.tests.integration.interface.utils import (
    get_user_settings_override,
)
from alchemiscale.interface.client import AlchemiscaleClientError


class TestClient:
    def test_wrong_credential(
        self,
        scope_test,
        n4js_preloaded,
        user_client_wrong_credential: client.AlchemiscaleClient,
        uvicorn_server,
    ):
        with pytest.raises(client.AlchemiscaleClientError):
            user_client_wrong_credential.get_info()

    def test_refresh_credential(
        self,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        uvicorn_server,
    ):
        settings = get_user_settings_override()
        assert user_client._jwtoken is None
        user_client._get_token()

        token = user_client._jwtoken
        assert token is not None

        # token shouldn't change this fast
        user_client.get_info()
        assert token == user_client._jwtoken

        # should change if we wait a bit
        sleep(settings.JWT_EXPIRE_SECONDS + 2)
        user_client.get_info()
        assert token != user_client._jwtoken

    def test_api_check(
        self,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        uvicorn_server,
    ):
        user_client._api_check()

    def test_list_scopes(
        self,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        uvicorn_server,
        multiple_scopes,
    ):
        scopes = user_client.list_scopes()
        # multiple scopes matches identity used to initialise the client in conftest
        assert set(scopes) == set(multiple_scopes)

    ### inputs

    def test_create_network(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
    ):
        # make a smaller network that overlaps with an existing one in DB
        an = AlchemicalNetwork(edges=list(network_tyk2.edges)[4:-2], name="smaller")
        an_sk = user_client.create_network(an, scope_test, state="active")

        network_sks = user_client.query_networks()
        assert an_sk in network_sks

        # TODO: make a network in a scope that doesn't have any components in
        # common with an existing network
        # user_client.create_network(

    @pytest.mark.parametrize(("state",), [[state.value] for state in NetworkStateEnum])
    def test_set_network_state(
        self,
        state,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        an = network_tyk2

        network_sk = user_client.create_network(
            an.copy_with_replacements(
                name=an.name + f"_test_set_network_state_{state}"
            ),
            scope_test,
        )

        q = """
            MATCH (:AlchemicalNetwork {`_scoped_key`: $network})<-[:MARKS]-(nm:NetworkMark)
            RETURN nm.state as state
        """

        results = n4js_preloaded.execute_query(
            q, parameters_={"network": str(network_sk)}
        )

        assert len(results.records) == 1
        assert results.records[0]["state"] == "active"

        updated_sk = user_client.set_network_state(network_sk, state)

        assert updated_sk is not None

        results = n4js_preloaded.execute_query(
            q, parameters_={"network": str(network_sk)}
        )

        assert len(results.records) == 1
        assert results.records[0]["state"] == state

    def test_set_network_state_no_network(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
    ):

        fake_scoped_key = ScopedKey.from_str(
            "AlchemicalNetwork-FakeKey-test_org-test_campaign-test_project"
        )
        result = user_client.set_network_state(fake_scoped_key, "active")
        assert result is None

    def test_set_network_state_invalid_state(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        invalid_state = "notastate"

        an = network_tyk2

        network_sk = user_client.create_network(an, scope_test)
        with pytest.raises(
            AlchemiscaleClientError,
            match="Status Code 400 : Bad Request : 'notastate' is not a valid state. Valid values include: \['",
        ):
            user_client.set_network_state(network_sk, invalid_state)

    def test_set_networks_state(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        an = network_tyk2

        network_sks = []
        for i in range(3):
            network = an.copy_with_replacements(
                name=an.name + f"_test_set_networks_state_{i}"
            )
            network_sks.append(user_client.create_network(network, scope_test))

        network_str_sks = list(map(str, network_sks))

        q = """
            UNWIND $networks as network
            MATCH (:AlchemicalNetwork {`_scoped_key`: network})<-[:MARKS]-(nm:NetworkMark)
            RETURN nm.state as state
        """

        results = n4js_preloaded.execute_query(
            q, parameters_={"networks": network_str_sks}
        )

        assert len(results.records) == 3
        assert results.records[0]["state"] == "active"

        for record in results.records:
            assert record["state"] == "active"

        updated_sks = user_client.set_networks_state(
            network_sks, ["active", "inactive", "deleted"]
        )
        assert all([updated_sk is not None for updated_sk in updated_sks])

        results = n4js_preloaded.execute_query(
            q, parameters_={"networks": network_str_sks}
        )

        assert len(results.records) == 3

        assert results.records[0]["state"] == "active"
        assert results.records[1]["state"] == "inactive"
        assert results.records[2]["state"] == "deleted"

    def test_get_network_state(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        an = network_tyk2

        network_sk = user_client.create_network(
            an.copy_with_replacements(name=an.name + "test_get_network_state"),
            scope_test,
        )

        result = user_client.get_network_state(network_sk)

        assert result == "active"

        n4js_preloaded.set_network_state([network_sk], ["inactive"])

        result = user_client.get_network_state(network_sk)

        assert result == "inactive"

    def test_get_networks_state(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        an = network_tyk2

        network_sks = []
        for i in range(3):
            network = an.copy_with_replacements(
                name=an.name + f"test_get_networks_state_{i}"
            )
            network_sks.append(user_client.create_network(network, scope_test))

        results = user_client.get_networks_state(network_sks)

        assert results == ["active", "active", "active"]

        new_states = ["inactive", "deleted", "active"]
        n4js_preloaded.set_network_state(
            network_sks,
            new_states,
        )

        results = user_client.get_networks_state(network_sks)

        assert results == new_states

    def test_get_network_state_no_network(
        self,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        fake_scoped_key = ScopedKey.from_str(
            "AlchemicalNetwork-FakeKey-test_org-test_campaign-test_project"
        )

        result = user_client.get_network_state(fake_scoped_key)

        assert result is None

    def test_query_networks(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        # explicit None for state
        network_sks = user_client.query_networks(state=None)

        assert len(network_sks) == 6
        assert scope_test in [n_sk.scope for n_sk in network_sks]

        # default value for state, should get active states
        network_sks = user_client.query_networks()

        assert set(user_client.query_networks(state="active")) == set(
            user_client.query_networks()
        )

        # only active states
        network_sks = user_client.query_networks(state="active")

        assert len(network_sks) == 3
        assert scope_test in [n_sk.scope for n_sk in network_sks]

        # only inactive states
        network_sks = user_client.query_networks(state="inactive")

        assert len(network_sks) == 3
        assert scope_test in [n_sk.scope for n_sk in network_sks]

        # either active or inactive, in a single scope
        assert (
            len(user_client.query_networks(scope=scope_test, state="active|inactive"))
            == 2
        )
        # either active or inactive given a network name
        assert (
            len(
                user_client.query_networks(
                    name=network_tyk2.name, state="active|inactive"
                )
            )
            == 3
        )

    def test_query_transformations(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        transformation_sks = user_client.query_transformations()

        assert len(transformation_sks) == len(network_tyk2.edges) * 3
        assert len(user_client.query_transformations(scope=scope_test)) == len(
            network_tyk2.edges
        )
        assert (
            len(
                user_client.query_transformations(
                    name="lig_ejm_31_to_lig_ejm_50_complex"
                )
            )
            == 3
        )
        assert (
            len(
                user_client.query_transformations(
                    scope=scope_test, name="lig_ejm_31_to_lig_ejm_50_complex"
                )
            )
            == 1
        )

    def test_query_chemicalsystems(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        chemicalsystem_sks = user_client.query_chemicalsystems()

        assert len(chemicalsystem_sks) == len(network_tyk2.nodes) * 3
        assert len(user_client.query_chemicalsystems(scope=scope_test)) == len(
            network_tyk2.nodes
        )
        assert len(user_client.query_chemicalsystems(name="lig_ejm_31_complex")) == 3
        assert (
            len(
                user_client.query_chemicalsystems(
                    scope=scope_test, name="lig_ejm_31_complex"
                )
            )
            == 1
        )

    def test_get_network_transformations(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        n_sk = user_client.get_scoped_key(network_tyk2, scope_test)
        tf_sks = user_client.get_network_transformations(n_sk)

        assert len(tf_sks) == len(network_tyk2.edges)
        assert set(tf_sk.gufe_key for tf_sk in tf_sks) == set(
            t.key for t in network_tyk2.edges
        )

    def test_get_transformation_networks(
        self,
        scope_test,
        n4js_preloaded,
        transformation,
        user_client: client.AlchemiscaleClient,
    ):
        tf_sk = user_client.get_scoped_key(transformation, scope_test)
        an_sks = user_client.get_transformation_networks(tf_sk)

        for an_sk in an_sks:
            assert tf_sk in user_client.get_network_transformations(an_sk)

    def test_get_network_chemicalsystems(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        n_sk = user_client.get_scoped_key(network_tyk2, scope_test)
        cs_sks = user_client.get_network_chemicalsystems(n_sk)

        assert len(cs_sks) == len(network_tyk2.nodes)
        assert set(cs_sk.gufe_key for cs_sk in cs_sks) == set(
            cs.key for cs in network_tyk2.nodes
        )

    def test_get_chemicalsystem_networks(
        self,
        scope_test,
        n4js_preloaded,
        chemicalsystem,
        user_client: client.AlchemiscaleClient,
    ):
        cs_sk = user_client.get_scoped_key(chemicalsystem, scope_test)
        an_sks = user_client.get_chemicalsystem_networks(cs_sk)

        for an_sk in an_sks:
            assert cs_sk in user_client.get_network_chemicalsystems(an_sk)

    def test_get_transformation_chemicalsystems(
        self,
        scope_test,
        n4js_preloaded,
        transformation,
        user_client: client.AlchemiscaleClient,
    ):
        tf_sk = user_client.get_scoped_key(transformation, scope_test)
        cs_sks = user_client.get_transformation_chemicalsystems(tf_sk)

        assert len(cs_sks) == 2
        assert set(cs_sk.gufe_key for cs_sk in cs_sks) == set(
            [transformation.stateA.key, transformation.stateB.key]
        )

    def test_get_chemicalsystem_transformations(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        chemicalsystem,
        user_client: client.AlchemiscaleClient,
    ):
        cs_sk = user_client.get_scoped_key(chemicalsystem, scope_test)
        tf_sks = user_client.get_chemicalsystem_transformations(cs_sk)

        tfs = []
        for tf in network_tyk2.edges:
            if chemicalsystem in (tf.stateA, tf.stateB):
                tfs.append(tf)

        assert set(tf_sk.gufe_key for tf_sk in tf_sks) == set(t.key for t in tfs)

    def test_get_network(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        an_sk = user_client.get_scoped_key(network_tyk2, scope_test)
        an = user_client.get_network(an_sk)

        assert an == network_tyk2
        assert an is network_tyk2

    def test_get_network_bad_network_key(
        self,
        scope_test: Scope,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
    ):
        invalid_key = "AlchemicalNetwork-00000000000000000000000000000000-test_org-test_campaign-test_project"
        an_sk = ScopedKey.from_str(invalid_key)

        with pytest.raises(
            AlchemiscaleClientError,
            match="Status Code 400 : Bad Request : 'No such object in database'",
        ):
            user_client.get_network(an_sk)

    def test_get_network_weight(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        an_sk = user_client.get_scoped_key(network_tyk2, scope_test)
        client_query_result = user_client.get_network_weight(an_sk)
        preloaded_taskhub_weight = n4js_preloaded.get_taskhub_weight([an_sk])[0]

        assert preloaded_taskhub_weight == client_query_result
        assert client_query_result == 0.5

    def test_get_networks_weight(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        networks = [
            network_tyk2.copy_with_replacements(
                name=network_tyk2.name + f"_test_get_networks_weight_{i}"
            )
            for i in range(2)
        ]

        network_sks = [
            user_client.create_network(network, scope_test) for network in networks
        ]

        network_weight_0 = 0.25
        network_weight_1 = 0.75

        n4js_preloaded.set_taskhub_weight([network_sks[0]], [network_weight_0])
        n4js_preloaded.set_taskhub_weight([network_sks[1]], [network_weight_1])

        client_query_result = user_client.get_networks_weight(network_sks)

        assert client_query_result == [0.25, 0.75]

    @pytest.mark.parametrize(
        "weight, shouldfail",
        [
            (0.0, False),
            (0.5, False),
            (1.0, False),
            (-1.0, True),
            (-1.5, True),
        ],
    )
    def test_set_network_weight(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
        weight,
        shouldfail,
    ):
        an_sk = user_client.get_scoped_key(network_tyk2, scope_test)

        if shouldfail:
            with pytest.raises(
                AlchemiscaleClientError,
                match="Status Code 400 : Bad Request : all `weights` must be",
            ):
                user_client.set_network_weight(an_sk, weight)
        else:
            user_client.set_network_weight(an_sk, weight)
            assert user_client.get_network_weight(an_sk) == weight

            weight = abs(weight - 0.5)
            result = user_client.set_network_weight(an_sk, weight)

            assert result == an_sk
            assert user_client.get_network_weight(an_sk) == weight

    @pytest.mark.parametrize(
        "weights, shouldfail",
        [
            ((0.0, 1.0), False),
            ((0.5, 0.5), False),
            ((1.0, 1.0), False),
            ((-1.0, 0.2), True),
            ((-1.5, 0.5), True),
        ],
    )
    def test_set_networks_weight(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
        weights,
        shouldfail,
    ):

        # create new networks and taskhubs
        network_sks = []
        for i in range(2):
            network = network_tyk2.copy_with_replacements(
                name=network_tyk2.name + f"_test_set_networks_weight_{i}"
            )

            network_sk, _, _ = n4js_preloaded.assemble_network(network, scope_test)

            network_sks.append(network_sk)

        # test for invalid input
        if shouldfail:
            with pytest.raises(
                AlchemiscaleClientError,
                match="Status Code 400 : Bad Request : all `weights` must be",
            ):
                user_client.set_networks_weight(network_sks, weights)
        else:
            user_client.set_networks_weight(network_sks, weights)
            assert n4js_preloaded.get_taskhub_weight(network_sks) == list(weights)

            weights = [abs(weight - 0.5) for weight in weights]
            results = user_client.set_networks_weight(network_sks, weights)

            assert results == network_sks
            assert n4js_preloaded.get_taskhub_weight(network_sks) == list(weights)

    def test_get_transformation(
        self,
        scope_test,
        n4js_preloaded,
        transformation,
        user_client: client.AlchemiscaleClient,
    ):
        tf_sk = user_client.get_scoped_key(transformation, scope_test)
        tf = user_client.get_transformation(tf_sk)

        assert tf == transformation
        assert tf is transformation

    def test_get_transformation_bad_transformation_key(
        self, scope_test, n4js_preloaded, user_client
    ):

        invalid_key = "Transformation-00000000000000000000000000000000-test_org-test_campaign-test_project"
        tf_sk = ScopedKey.from_str(invalid_key)

        with pytest.raises(
            AlchemiscaleClientError,
            match="Status Code 400 : Bad Request : 'No such object in database'",
        ):
            user_client.get_transformation(tf_sk)

    def test_get_chemicalsystem(
        self,
        scope_test,
        n4js_preloaded,
        chemicalsystem,
        user_client: client.AlchemiscaleClient,
    ):
        cs_sk = user_client.get_scoped_key(chemicalsystem, scope_test)
        cs = user_client.get_chemicalsystem(cs_sk)

        assert cs == chemicalsystem
        assert cs is chemicalsystem

    def test_get_chemicalsystem_bad_chemicalsystem_key(
        self, scope_test, n4js_preloaded, user_client
    ):

        invalid_key = "ChemicalSystem-00000000000000000000000000000000-test_org-test_campaign-test_project"
        cs_sk = ScopedKey.from_str(invalid_key)

        with pytest.raises(
            AlchemiscaleClientError,
            match="Status Code 400 : Bad Request : 'No such object in database'",
        ):
            user_client.get_chemicalsystem(cs_sk)

    ### compute

    def test_create_tasks(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
    ):
        n4js = n4js_preloaded

        # select the transformation we want to compute
        an = network_tyk2
        transformation = list(an.edges)[0]
        sk = user_client.get_scoped_key(transformation, scope_test)

        task_sks = user_client.create_tasks(sk, count=3)

        assert set(task_sks) == set(n4js.get_transformation_tasks(sk))

        # try creating tasks that extend one of those we just created
        task_sks_e = user_client.create_tasks(sk, count=4, extends=task_sks[0])

        # check that we now get additional tasks
        assert set(task_sks + task_sks_e) == set(n4js.get_transformation_tasks(sk))

        # check that tasks are structured as we expect
        assert set(task_sks_e) == set(
            n4js.get_transformation_tasks(sk, extends=task_sks[0])
        )
        assert set() == set(n4js.get_transformation_tasks(sk, extends=task_sks[1]))

    def test_create_tasks_extends_qualname_not_task(
        self,
        network_tyk2,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
    ):
        # get Transformation to test against
        transformation = list(network_tyk2.edges)[0]
        sk = user_client.get_scoped_key(transformation, scope_test)

        # create the task and get the ScopedKey
        task_sk = user_client.create_tasks(sk)[0]

        # mess up the qualname
        scoped_key_wrong_qualname = ScopedKey.from_str(
            str(task_sk).replace("Task", "NotTask")
        )

        assert scoped_key_wrong_qualname.qualname == "NotTask"

        # use the incorrect ScopedKey, expect to see an AlchemiscaleClientError
        with pytest.raises(
            AlchemiscaleClientError,
            match="Status Code 400 : Bad Request : `extends` ScopedKey \(",
        ):
            user_client.create_tasks(sk, count=4, extends=scoped_key_wrong_qualname)

    def test_create_transformations_tasks(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
    ):
        n4js = n4js_preloaded

        an = network_tyk2
        transformations = list(an.edges)[:5]

        transformation_sks = [
            user_client.get_scoped_key(transformation, scope_test)
            for transformation in transformations
        ]

        # create three copies tasks per transformation
        task_sks = user_client.create_transformations_tasks(transformation_sks * 3)

        all_tasks = set()
        extends_list = []
        for transformation_sk in transformation_sks:
            transformation_tasks = n4js.get_transformation_tasks(transformation_sk)
            # there should be three tasks for each transformation
            assert len(transformation_tasks) == 3
            all_tasks |= set(transformation_tasks)

            # we will want to test extensions, hold on to some tasks
            extends_list.append(transformation_tasks[0])

        assert set(task_sks) == all_tasks

        # create a new set of tasks
        extends_tasks = user_client.create_transformations_tasks(
            transformation_sks, extends=extends_list
        )

        # should still have 5
        assert len(extends_tasks) == 5

        # get all of the original tasks, given our extension tasks
        q = f"""UNWIND {cypher_list_from_scoped_keys(extends_tasks)} AS e_task
        MATCH (Task {{`_scoped_key`: e_task}})-[:EXTENDS]->(original_task:Task)
        RETURN original_task._scoped_key AS original_task
        """
        results = n4js.execute_query(q)

        assert len(results.records) == 5

        # check that we extended the correct tasks
        originals = [
            ScopedKey.from_str(record["original_task"]) for record in results.records
        ]
        assert set(originals) == set(extends_list)

        # make sure the first transformation_sk isn't extending an already existing task
        extends_list[0] = None

        # confirm we still have 5 entries
        assert len(extends_list) == 5

        extends_tasks = user_client.create_transformations_tasks(
            transformation_sks, extends=extends_list
        )

        q = f"""UNWIND {cypher_list_from_scoped_keys(extends_tasks)} AS e_task
        MATCH (Task {{`_scoped_key`: e_task}})-[:EXTENDS]->(original_task:Task)
        RETURN original_task._scoped_key AS original_task
        """
        results = n4js.execute_query(q)

        # we should only have 4 original tasks even though we
        # created 5 new tasks
        assert len(results.records) == 4 and len(extends_tasks) == 5

    def test_query_tasks(
        self,
        scope_test,
        multiple_scopes,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
    ):
        task_sks = user_client.query_tasks()
        assert len(task_sks) == 0

        tf_sks = user_client.query_transformations(scope=scope_test)

        for tf_sk in tf_sks[:10]:
            user_client.create_tasks(tf_sk, count=3)

        task_sks = user_client.query_tasks()
        assert len(task_sks) == 10 * 3

        task_sks = user_client.query_tasks(scope=scope_test)
        assert len(task_sks) == 10 * 3

        task_sks = user_client.query_tasks(scope=multiple_scopes[1])
        assert len(task_sks) == 0

        # check that we can query by status
        task_sks = user_client.query_tasks()
        user_client.set_tasks_status(task_sks[:10], "invalid")

        task_sks = user_client.query_tasks(status="waiting")
        assert len(task_sks) == 10 * 3 - 10

        task_sks = user_client.query_tasks(status="invalid")
        assert len(task_sks) == 10

        task_sks = user_client.query_tasks(status="complete")
        assert len(task_sks) == 0

    def test_get_network_tasks(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
    ):
        an_sk = user_client.query_networks(scope=scope_test)[0]
        tf_sks = user_client.get_network_transformations(an_sk)

        task_sks = []
        for tf_sk in tf_sks[:10]:
            task_sks.extend(user_client.create_tasks(tf_sk, count=3))

        task_sks_network = user_client.get_network_tasks(an_sk)
        assert set(task_sks_network) == set(task_sks)
        assert len(task_sks_network) == len(task_sks)

        user_client.set_tasks_status(task_sks[:10], "invalid")

        task_sks = user_client.get_network_tasks(an_sk, status="waiting")
        assert len(task_sks) == len(task_sks_network) - 10

        task_sks = user_client.get_network_tasks(an_sk, status="invalid")
        assert len(task_sks) == 10

        task_sks = user_client.get_network_tasks(an_sk, status="complete")
        assert len(task_sks) == 0

    def test_get_network_tasks_invalid_status_enum(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
    ):
        an_sk = user_client.query_networks(scope=scope_test)[0]
        tf_sks = user_client.get_network_transformations(an_sk)

        task_sks = []
        for tf_sk in tf_sks[:10]:
            task_sks.extend(user_client.create_tasks(tf_sk, count=3))

        with pytest.raises(
            AlchemiscaleClientError,
            match="Status Code 400 : Bad Request : 'notastatus' is not a valid TaskStatusEnum",
        ):
            user_client.get_network_tasks(an_sk, status="notastatus")

    def test_get_task_networks(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
    ):
        an_sk = user_client.query_networks(scope=scope_test)[0]
        tf_sks = user_client.get_network_transformations(an_sk)

        task_sks = []
        for tf_sk in tf_sks[:10]:
            task_sks.extend(user_client.create_tasks(tf_sk, count=3))

        for task_sk in task_sks:
            an_sks = user_client.get_task_networks(task_sk)
            assert an_sk in an_sks
            for an_sk in an_sks:
                assert task_sk in user_client.get_network_tasks(an_sk)

    def test_get_transformation_tasks(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
    ):
        # select the transformation we want to compute
        an = network_tyk2
        transformation = list(an.edges)[0]
        sk = user_client.get_scoped_key(transformation, scope_test)

        task_sks = user_client.create_tasks(sk, count=3)

        assert set(task_sks) == set(user_client.get_transformation_tasks(sk))

        # try creating tasks that extend one of those we just created
        task_sks_e = user_client.create_tasks(sk, count=4, extends=task_sks[0])

        # check that we now get additional tasks
        assert set(task_sks + task_sks_e) == set(
            user_client.get_transformation_tasks(sk)
        )

        # check that tasks are structured as we expect
        assert set(task_sks_e) == set(
            user_client.get_transformation_tasks(sk, extends=task_sks[0])
        )
        assert set() == set(
            user_client.get_transformation_tasks(sk, extends=task_sks[1])
        )

        # check graph form of output
        graph: nx.DiGraph = user_client.get_transformation_tasks(sk, return_as="graph")

        for task_sk in task_sks:
            assert len(list(graph.successors(task_sk))) == 0

        for task_sk in task_sks_e:
            assert graph.has_edge(task_sk, task_sks[0])

        # try filtering on status
        # check that tasks are structured as we expect
        user_client.set_tasks_status(task_sks_e[:2], "invalid")
        assert set(task_sks_e[2:]) == set(
            user_client.get_transformation_tasks(
                sk, extends=task_sks[0], status="waiting"
            )
        )
        assert set(task_sks_e[:2]) == set(
            user_client.get_transformation_tasks(
                sk, extends=task_sks[0], status="invalid"
            )
        )
        assert set(task_sks_e) == set(
            user_client.get_transformation_tasks(sk, extends=task_sks[0])
        )

    def test_get_task_transformation(
        self,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
    ):
        tf_sks = user_client.query_transformations()

        task_sks = []
        for tf_sk in tf_sks[:10]:
            task_sks.append(user_client.create_tasks(tf_sk, count=3))

        for tf_sk, tf_task_sks in zip(tf_sks, task_sks):
            for task_sk in tf_task_sks:
                assert user_client.get_task_transformation(task_sk) == tf_sk

    def test_get_task_transformation_transformation_no_exists(
        self,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
    ):

        task_sk = ScopedKey.from_str(
            "Task-00000000000000000000000000000000-test_org-test_campaign-test_project"
        )

        with pytest.raises(
            AlchemiscaleClientError,
            match="Status Code 400 : Bad Request : 'No such object in database'",
        ):
            user_client.get_task_transformation(task_sk)

    def test_get_scope_status(
        self,
        multiple_scopes,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):
        # for each of the following scopes, create tasks for a single random
        # transformation
        for scope in multiple_scopes:
            tf_sks = user_client.query_transformations(scope=scope)
            user_client.create_tasks(tf_sks[0], count=3)

        # now, get status for each scope; check that we are filtering down
        # properly
        for scope in multiple_scopes:
            status_counts = user_client.get_scope_status(scope)

            for status in status_counts:
                if status == "waiting":
                    assert status_counts[status] == 3
                else:
                    assert status_counts[status] == 0

        # create tasks in a scope we don't have access to
        other_scope = Scope("other_org", "other_campaign", "other_project")
        n4js_preloaded.assemble_network(network_tyk2, other_scope)
        other_tf_sk = n4js_preloaded.query_transformations(scope=other_scope)[0]
        _ = n4js_preloaded.create_task(other_tf_sk)

        # ask for the scope that we don't have access to
        status_counts = user_client.get_scope_status(other_scope)
        assert status_counts == {}

        # try a more general scope
        status_counts = user_client.get_scope_status(Scope())
        for status in status_counts:
            if status == "waiting":
                assert status_counts[status] == 3 * len(multiple_scopes)
            else:
                assert status_counts[status] == 0

    def test_get_scope_status_network_state(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
    ):

        # first, set all existing networks in the test scope to deleted
        # to avoid interactions with our new networks
        an_sks = user_client.query_networks(scope=scope_test, state=None)
        user_client.set_networks_state(an_sks, states=["deleted"] * len(an_sks))

        # create two AlchemicalNetworks with only 1 shared Transformation
        transformations = list(network_tyk2.edges)

        an1 = AlchemicalNetwork(edges=transformations[:4], name="0 - 3")
        an2 = AlchemicalNetwork(edges=transformations[3:], name="3 - ...")

        # set the first network as active, the second as inactive
        an1_sk = user_client.create_network(an1, scope_test, state="active")
        an2_sk = user_client.create_network(an2, scope_test, state="inactive")

        # for each transformation in these networks, create 3 tasks
        tf_sks = set(
            user_client.get_network_transformations(an1_sk)
            + user_client.get_network_transformations(an2_sk)
        )
        user_client.create_transformations_tasks(
            list(chain(*[[tf_sk] * 3 for tf_sk in tf_sks]))
        )

        # get scope status; expect to only see task statuses for
        # transformations in active network by default
        statuses = user_client.get_scope_status(scope_test)

        assert len(statuses) == 1
        assert statuses["waiting"] == len(an1.edges) * 3

        # get inactive task status counts
        statuses = user_client.get_scope_status(scope_test, network_state="inactive")

        assert len(statuses) == 1
        assert statuses["waiting"] == len(an2.edges) * 3

        # get all task status counts;
        # show that status counts are not double counted
        statuses = user_client.get_scope_status(scope_test, network_state=None)

        assert len(statuses) == 1
        assert statuses["waiting"] == len(network_tyk2.edges) * 3

        # set the inactive network to active, then get status counts
        user_client.set_networks_state([an2_sk], states=["active"])

        statuses = user_client.get_scope_status(scope_test)

        assert len(statuses) == 1
        assert statuses["waiting"] == len(network_tyk2.edges) * 3

        # set all networks to not active, get status counts
        user_client.set_networks_state([an1_sk, an2_sk], states=["inactive", "deleted"])

        statuses = user_client.get_scope_status(scope_test)

        assert len(statuses) == 0

    def test_get_network_status(
        self,
        n4js_preloaded,
        multiple_scopes,
        user_client: client.AlchemiscaleClient,
    ):
        # for each of the following scopes, get one of the networks present,
        # create tasks for a single random transformation
        an_sks = []
        for scope in multiple_scopes:
            an_sk = user_client.query_networks(scope=scope)[0]
            tf_sks = user_client.get_network_transformations(an_sk)
            user_client.create_tasks(tf_sks[0], count=3)

            an_sks.append(an_sk)

        # now, get status for each network
        for an_sk in an_sks:
            status_counts = user_client.get_network_status(an_sk)

            for status in status_counts:
                if status == "waiting":
                    assert status_counts[status] == 3
                else:
                    assert status_counts[status] == 0

    def test_get_networks_status(
        self,
        n4js_preloaded,
        multiple_scopes,
        user_client: client.AlchemiscaleClient,
    ):
        # for each of the following scopes, get one of the networks present,
        # create tasks for a single random transformation
        an_sks = []
        for scope in multiple_scopes:
            an_sk = user_client.query_networks(scope=scope)[0]
            tf_sks = user_client.get_network_transformations(an_sk)
            user_client.create_tasks(tf_sks[0], count=3)

            an_sks.append(an_sk)

        status_counts = user_client.get_networks_status(an_sks)

        assert len(status_counts) == len(an_sks)

        for i, statuses in enumerate(status_counts):
            for status in statuses:
                if status == "waiting":
                    assert status_counts[i][status] == 3
                else:
                    assert status_counts[i][status] == 0

    def test_get_transformation_status(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
        uvicorn_server,
    ):
        network_sk = user_client.get_scoped_key(network_tyk2, scope_test)
        transformation_sk = user_client.get_network_transformations(network_sk)[0]

        all_tasks = user_client.create_tasks(transformation_sk, count=5)

        # check the status of the tasks; should all be waiting
        stat = user_client.get_transformation_status(transformation_sk)
        assert stat == {"waiting": 5}

        # cheat and set the status of all tasks to running
        ret_task = n4js_preloaded.set_task_status(all_tasks, TaskStatusEnum.running)
        assert set(ret_task) == set(all_tasks)
        stat = user_client.get_transformation_status(transformation_sk)
        assert stat == {"running": 5}

        # cheat and set the status of all tasks to complete
        ret_task = n4js_preloaded.set_task_status(all_tasks, TaskStatusEnum.complete)
        assert set(ret_task) == set(all_tasks)
        stat = user_client.get_transformation_status(transformation_sk)
        assert stat == {"complete": 5}

    @pytest.mark.parametrize(
        "get_weights",
        [
            (True),
            (False),
        ],
    )
    def test_get_network_actioned_tasks(
        self,
        scope_test,
        n4js_preloaded,
        user_client,
        network_tyk2,
        get_weights,
    ):
        an = network_tyk2
        transformation = list(an.edges)[0]

        network_sk = user_client.get_scoped_key(an, scope_test)
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        # if no tasks actioned, should get nothing back
        assert (
            len(
                user_client.get_network_actioned_tasks(
                    network_sk, task_weights=get_weights
                )
            )
            == 0
        )

        task_sks = user_client.create_tasks(transformation_sk, count=3)
        user_client.action_tasks(task_sks[:2], network_sk)

        results = user_client.get_network_actioned_tasks(
            network_sk, task_weights=get_weights
        )

        if get_weights:
            assert list(results.values()) == [0.5, 0.5]

        assert set(results) == set(task_sks[:2])

    def test_get_network_actioned_tasks_network_scopedkey_wrong_qualname(
        self,
        scope_test,
        n4js_preloaded,
        user_client,
        network_tyk2,
    ):
        an = network_tyk2
        network_sk = user_client.get_scoped_key(an, scope_test)

        wrong_qualname_sk = ScopedKey.from_str(
            str(network_sk).replace("AlchemicalNetwork", "NotANetwork")
        )

        with pytest.raises(
            AlchemiscaleClientError,
            match="Status Code 400 : Bad Request : `network` ScopedKey does not correspond to an `AlchemicalNetwork`",
        ):
            user_client.get_network_actioned_tasks(wrong_qualname_sk)

    @pytest.mark.parametrize(
        "get_weights",
        [
            (True),
            (False),
        ],
    )
    def test_get_networks_actioned_tasks(
        self,
        scope_test,
        n4js_preloaded,
        user_client,
        network_tyk2,
        get_weights,
    ):
        base_an = network_tyk2

        # 2 test networks
        n_networks = 2
        # 3 tasks per network
        n_tasks = 3

        networks = [
            network_tyk2.copy_with_replacements(name=base_an.name + f"_copy_{i}")
            for i in range(n_networks)
        ]

        network_sks = [
            user_client.create_network(network, scope_test) for network in networks
        ]

        transformation_sks = [
            user_client.get_scoped_key(list(network.edges)[0], scope_test)
            for network in networks
        ]

        for network_sk in network_sks:
            # if no tasks actioned, should get nothing back
            assert (
                len(
                    user_client.get_networks_actioned_tasks(
                        [network_sk], task_weights=get_weights
                    )[0]
                )
                == 0
            )

        all_task_sks = []
        for network_sk, transformation_sk in zip(network_sks, transformation_sks):
            task_sks = user_client.create_tasks(transformation_sk, count=n_tasks)

            # action all but one task for each network
            actioned = user_client.action_tasks(task_sks[: n_tasks - 1], network_sk)
            all_task_sks.append(actioned)

        results = user_client.get_networks_actioned_tasks(
            network_sks, task_weights=get_weights
        )

        assert len(results) == n_networks and isinstance(results, list)

        if get_weights:
            assert isinstance(results[0], dict)
        else:
            assert isinstance(results[0], list)

        for network_result, task_sks in zip(results, all_task_sks):
            if get_weights:
                assert list(network_result.values()) == [0.5] * (n_tasks - 1)

            assert set(network_result) == set(task_sks)

    @pytest.mark.parametrize(
        ("actioned_tasks"),
        [
            (True),
            (False),
        ],
    )
    def test_get_task_actioned_networks(
        self,
        scope_test,
        n4js_preloaded,
        user_client,
        network_tyk2,
        actioned_tasks,
    ):
        an = network_tyk2

        transformation = list(an.edges)[0]
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        networks = user_client.get_transformation_networks(transformation_sk)

        task_sks = user_client.create_tasks(transformation_sk)

        if actioned_tasks:
            for network in networks:
                user_client.action_tasks(task_sks, network)

        # without requesting weights, default
        results = user_client.get_task_actioned_networks(
            task_sks[0], task_weights=False
        )

        if actioned_tasks:
            assert len(results) == 2
            assert set(results) == set(networks)
        else:
            assert results == []

        # requesting weights
        results = user_client.get_task_actioned_networks(task_sks[0], task_weights=True)

        if actioned_tasks:
            assert len(results) == 2
            assert set(results) == set(networks)
            assert list(results.values()) == [0.5, 0.5]
        else:
            assert len(results) == 0

    def test_action_tasks(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
    ):
        n4js = n4js_preloaded

        # select the transformation we want to compute
        an = network_tyk2
        transformation = list(an.edges)[0]

        network_sk = user_client.get_scoped_key(an, scope_test)
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        task_sks = user_client.create_tasks(transformation_sk, count=3)

        # action these task for this network, in reverse order
        actioned_sks = user_client.action_tasks(task_sks[::-1], network_sk)

        # check that the taskhub looks as we expect
        taskhub_sk = n4js.get_taskhub(network_sk)
        hub_task_sks = n4js.get_taskhub_tasks(taskhub_sk)

        assert set(actioned_sks) == set(hub_task_sks)
        assert actioned_sks == task_sks[::-1]

        # create extending tasks; these should be actioned to the
        # taskhub but not claimable
        task_sks_e = user_client.create_tasks(
            transformation_sk, count=4, extends=task_sks[0]
        )
        actioned_sks_e = user_client.action_tasks(task_sks_e, network_sk)

        assert set(task_sks_e) == set(actioned_sks_e)

    def test_action_tasks_network_scopedkey_wrong_qualname(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
    ):
        an = network_tyk2
        transformation = list(an.edges)[0]

        network_sk = user_client.get_scoped_key(an, scope_test)
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        task_sks = user_client.create_tasks(transformation_sk, count=3)

        wrong_qualname_sk = ScopedKey.from_str(
            str(network_sk).replace("AlchemicalNetwork", "NotANetwork")
        )

        with pytest.raises(
            AlchemiscaleClientError,
            match="Status Code 400 : Bad Request : `network` ScopedKey does not correspond to an `AlchemicalNetwork`",
        ):
            user_client.action_tasks(task_sks, wrong_qualname_sk)

    @pytest.mark.parametrize(
        "weight,shouldfail",
        [
            (None, False),
            (1.0, False),
            ([0.25, 0.5, 0.75], False),
            (-1, True),
            (1.5, True),
        ],
    )
    def test_action_tasks_with_weights(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
        weight,
        shouldfail,
    ):
        n4js = n4js_preloaded

        # select the transformation we want to compute
        an = network_tyk2
        transformation = list(an.edges)[0]

        network_sk = user_client.get_scoped_key(an, scope_test)
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        task_sks = user_client.create_tasks(transformation_sk, count=3)

        if shouldfail:
            with pytest.raises(AlchemiscaleClientError):
                user_client.action_tasks(
                    task_sks,
                    network_sk,
                    weight,
                )
        else:
            user_client.action_tasks(
                task_sks,
                network_sk,
                weight,
            )

            th_sk = n4js.get_taskhub(network_sk)
            task_weights = n4js.get_task_weights(task_sks, th_sk)

            _weight = weight

            if weight is None:
                _weight = [0.5] * len(task_sks)
            elif not isinstance(weight, list):
                _weight = [weight] * len(task_sks)

            assert task_weights == _weight

            # actioning tasks again with None should preserve
            # task weights
            user_client.action_tasks(task_sks, network_sk, weight=None)

            task_weights = n4js.get_task_weights(task_sks, th_sk)
            assert task_weights == _weight

    def test_action_tasks_update_weights(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
    ):
        n4js = n4js_preloaded

        # select the transformation we want to compute
        an = network_tyk2
        transformation = list(an.edges)[0]

        network_sk = user_client.get_scoped_key(an, scope_test)
        th_sk = n4js.get_taskhub(network_sk)
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        task_sks = user_client.create_tasks(transformation_sk, count=3)
        user_client.action_tasks(task_sks, network_sk)

        # base case
        assert [0.5, 0.5, 0.5] == n4js.get_task_weights(task_sks, th_sk)

        new_weights = [1.0, 0.7, 0.4]
        user_client.action_tasks(task_sks, network_sk, new_weights)

        assert new_weights == n4js.get_task_weights(task_sks, th_sk)

        # action a couple more tasks along with existing ones, then check weights as expected
        new_task_sks = user_client.create_tasks(transformation_sk, count=2)
        user_client.action_tasks(task_sks + new_task_sks, network_sk)

        assert new_weights + [0.5] * 2 == n4js.get_task_weights(
            task_sks + new_task_sks, th_sk
        )

    def test_cancel_tasks(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
    ):
        n4js = n4js_preloaded

        # select the transformation we want to compute
        an = network_tyk2
        transformation = list(an.edges)[0]

        network_sk = user_client.get_scoped_key(an, scope_test)
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        task_sks = user_client.create_tasks(transformation_sk, count=3)

        # action these task for this network, in reverse order
        actioned_sks = user_client.action_tasks(task_sks[::-1], network_sk)

        # try canceling one of these tasks
        canceled_sks = user_client.cancel_tasks(task_sks[1:2], network_sk)

        # check that the taskhub looks as we expect
        taskhub_sk = n4js.get_taskhub(network_sk)
        hub_task_sks = n4js.get_taskhub_tasks(taskhub_sk)

        assert set([actioned_sks[0], actioned_sks[2]]) == set(hub_task_sks)
        assert canceled_sks == [actioned_sks[1]]

        # try to cancel a task that's not present on the hub
        canceled_sks_2 = user_client.cancel_tasks(task_sks[1:2], network_sk)

        assert canceled_sks_2 == [None]

    def test_cancel_tasks_wrong_qualname(
        self,
        scope_test,
        n4js_preloaded,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
    ):

        an = network_tyk2
        transformation = list(an.edges)[0]

        network_sk = user_client.get_scoped_key(an, scope_test)
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        task_sks = user_client.create_tasks(transformation_sk, count=3)

        user_client.action_tasks(task_sks[::-1], network_sk)

        wrong_qualname_sk = ScopedKey.from_str(
            str(network_sk).replace("AlchemicalNetwork", "NotANetwork")
        )

        with pytest.raises(
            AlchemiscaleClientError,
            match="Status Code 400 : Bad Request : `network` ScopedKey does not correspond to an `AlchemicalNetwork`",
        ):
            user_client.cancel_tasks(task_sks[1:2], wrong_qualname_sk)

    @pytest.mark.parametrize(
        "status, should_raise",
        [
            (TaskStatusEnum.waiting, False),
            (TaskStatusEnum.running, True),
            (TaskStatusEnum.complete, True),
            (TaskStatusEnum.error, True),
            (TaskStatusEnum.invalid, False),
            (TaskStatusEnum.deleted, False),
        ],
    )
    def test_set_tasks_status(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
        uvicorn_server,
        status,
        should_raise,
    ):
        an = network_tyk2
        transformation = list(an.edges)[0]

        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        all_tasks = user_client.create_tasks(transformation_sk, count=5)

        if should_raise:
            with pytest.raises(AlchemiscaleClientError):
                user_client.set_tasks_status(all_tasks, status)
        else:
            # set the status of a task
            user_client.set_tasks_status(all_tasks, status)

            # check that the status has been set
            # note must be list on n4js side
            statuses = n4js_preloaded.get_task_status(all_tasks)
            assert all([s == status for s in statuses])

    @pytest.mark.parametrize(
        "status, should_raise",
        [
            (TaskStatusEnum.waiting, False),
            (TaskStatusEnum.running, True),
            (TaskStatusEnum.complete, True),
            (TaskStatusEnum.error, True),
            (TaskStatusEnum.invalid, False),
            (TaskStatusEnum.deleted, False),
        ],
    )
    def test_get_tasks_status(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
        uvicorn_server,
        status,
        should_raise,
    ):
        an = network_tyk2
        transformation = list(an.edges)[0]

        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        all_tasks = user_client.create_tasks(transformation_sk, count=5)

        # set the status of a task
        if should_raise:
            with pytest.raises(AlchemiscaleClientError):
                user_client.set_tasks_status(all_tasks, status)
        else:
            user_client.set_tasks_status(all_tasks, status)

            # check that the status has been set
            statuses = user_client.get_tasks_status(all_tasks)
            assert all([s == status.value for s in statuses])

    def test_get_tasks_priority(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
        uvicorn_server,
    ):
        an = network_tyk2
        transformation = list(an.edges)[0]
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        all_tasks = user_client.create_tasks(transformation_sk, count=5)
        priorities = user_client.get_tasks_priority(all_tasks)

        assert all([p == 10 for p in priorities])

    @pytest.mark.parametrize(
        "priority, should_raise",
        [
            (1, False),
            (-1, True),
        ],
    )
    def test_set_tasks_priority(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
        uvicorn_server,
        priority,
        should_raise,
    ):
        an = network_tyk2
        transformation = list(an.edges)[0]

        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        all_tasks = user_client.create_tasks(transformation_sk, count=5)
        priorities = user_client.get_tasks_priority(all_tasks)

        # baseline, we need to confirm that values are different after set call
        assert all([p == 10 for p in priorities])

        if should_raise:
            with pytest.raises(
                AlchemiscaleClientError,
                match="Status Code 400 : Bad Request : priority must be between",
            ):
                user_client.set_tasks_priority(all_tasks, priority)
        else:
            user_client.set_tasks_priority(all_tasks, priority)
            priorities = user_client.get_tasks_priority(all_tasks)
            assert all([p == priority for p in priorities])

    def test_set_tasks_priority_missing_tasks(
        self,
        scope_test,
        n4js_preloaded,
        network_tyk2,
        user_client: client.AlchemiscaleClient,
        uvicorn_server,
    ):
        an = network_tyk2
        transformation = list(an.edges)[0]

        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        all_tasks = user_client.create_tasks(transformation_sk, count=5)

        fake_tasks = [
            ScopedKey.from_str(t)
            for t in [
                "Task-FAKE1-test_org-test_campaign-test_project",
                "Task-FAKE2-test_org-test_campaign-test_project",
            ]
        ]

        updated_tasks = user_client.set_tasks_priority(fake_tasks + all_tasks, 10)
        assert updated_tasks[0:2] == [None, None]
        assert [t is not None for t in updated_tasks[2:]]

    ### results

    @staticmethod
    def _execute_tasks(tasks, n4js, s3os_server):
        shared_basedir = Path("shared").absolute()
        shared_basedir.mkdir()
        scratch_basedir = Path("scratch").absolute()
        scratch_basedir.mkdir()

        protocoldagresults = []
        for task_sk in tasks:
            if task_sk is None:
                continue

            # get the transformation and extending protocoldagresult as if we
            # were a compute service
            (
                transformation_sk,
                extends_protocoldagresultref_sk,
            ) = n4js.get_task_transformation(task=task_sk, return_gufe=False)

            transformation = n4js.get_gufe(transformation_sk)
            if extends_protocoldagresultref_sk:
                extends_protocoldagresultref = n4js.get_gufe(
                    extends_protocoldagresultref_sk
                )
                extends_protocoldagresult = s3os_server.pull_protocoldagresult(
                    extends_protocoldagresultref, transformation_sk
                )
            else:
                extends_protocoldagresult = None

            protocoldag = transformation.create(
                extends=extends_protocoldagresult,
                name=str(task_sk),
            )

            shared = shared_basedir / str(protocoldag.key)
            shared.mkdir()

            scratch = scratch_basedir / str(protocoldag.key)
            scratch.mkdir()

            protocoldagresult = execute_DAG(
                protocoldag,
                shared_basedir=shared,
                scratch_basedir=scratch,
                raise_error=False,
            )

            assert protocoldagresult.transformation_key == transformation.key
            if extends_protocoldagresult:
                assert protocoldagresult.extends_key == extends_protocoldagresult.key

            protocoldagresults.append(protocoldagresult)

            protocoldagresultref = s3os_server.push_protocoldagresult(
                protocoldagresult, transformation=transformation_sk
            )

            n4js.set_task_result(
                task=task_sk, protocoldagresultref=protocoldagresultref
            )

        return protocoldagresults

    def test_get_transformation_and_network_results(
        self,
        scope_test,
        n4js_preloaded,
        s3os_server,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
        tmpdir,
    ):
        n4js = n4js_preloaded

        # select the transformation we want to compute
        an = network_tyk2
        transformation = list(t for t in an.edges if "_solvent" in t.name)[0]

        network_sk = user_client.get_scoped_key(an, scope_test)
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        # user client : create three independent tasks for the transformation
        user_client.create_tasks(transformation_sk, count=3)

        # user client : action the tasks for execution
        all_tasks = user_client.get_transformation_tasks(transformation_sk)
        actioned_tasks = user_client.action_tasks(all_tasks, network_sk)

        # execute the actioned tasks and push results directly using statestore and object store
        with tmpdir.as_cwd():
            protocoldagresults = self._execute_tasks(actioned_tasks, n4js, s3os_server)

        # clear local gufe registry of pdr objects
        # not critical, but ensures we see the objects that are deserialized
        # instead of our instances already in memory post-pull
        for pdr in protocoldagresults:
            TOKENIZABLE_REGISTRY.pop(pdr.key, None)

        protocolresult = user_client.get_transformation_results(transformation_sk)

        assert protocolresult.get_estimate() == 95500.0
        assert set(protocolresult.data.keys()) == {"logs", "key_results"}
        assert len(protocolresult.data["key_results"]) == 3

        # get back protocoldagresults instead
        protocoldagresults_r = user_client.get_transformation_results(
            transformation_sk, return_protocoldagresults=True
        )

        assert set(protocoldagresults_r) == set(protocoldagresults)

        for pdr in protocoldagresults_r:
            assert pdr.transformation_key == transformation.key
            assert isinstance(pdr.extends_key, GufeKey) or pdr.extends_key is None
            assert pdr.ok()

        # so we don't have to recompute the above, which depends on
        # function-scoped fixtures, we also test getting all network results
        # here too
        network_results = user_client.get_network_results(network_sk)
        for tf_sk, pr in network_results.items():
            if tf_sk == transformation_sk:
                assert pr.get_estimate() == 95500.0
                assert set(pr.data.keys()) == {"logs", "key_results"}
                assert len(pr.data["key_results"]) == 3
            else:
                assert pr is None

        network_results = user_client.get_network_results(
            network_sk, return_protocoldagresults=True
        )
        for tf_sk, pdrs in network_results.items():
            if tf_sk == transformation_sk:
                assert set(pdrs) == set(protocoldagresults)
                for pdr in pdrs:
                    assert pdr.transformation_key == transformation.key
                    assert (
                        isinstance(pdr.extends_key, GufeKey) or pdr.extends_key is None
                    )
                    assert pdr.ok()
            else:
                assert pdrs == []

    def test_get_transformation_and_network_failures(
        self,
        scope_test,
        n4js_preloaded,
        s3os_server,
        user_client: client.AlchemiscaleClient,
        network_tyk2_failure,
        tmpdir,
    ):
        n4js = n4js_preloaded

        # select the transformation we want to compute
        an = network_tyk2_failure
        network_sk = user_client.create_network(an, scope_test)

        # seeing what appears to be random race condition in CI; adding this
        # ensures full AlchemicalNetwork present before we proceed
        while True:
            try:
                an_ = user_client.get_network(network_sk)

                if an_ != an:
                    raise Exception("Network out doesn't exactly match network in yet")
                else:
                    break
            except Exception:
                sleep(0.1)

        tf_sks = user_client.get_network_transformations(network_sk)

        # select the transformation we want to compute
        for tf_sk in tf_sks:
            tf = user_client.get_transformation(tf_sk)
            if tf.name == "broken":
                transformation_sk = tf_sk
                transformation = tf
                break

        # user client : create tasks for the transformation
        tasks = user_client.create_tasks(transformation_sk, count=2)

        # user client : action the tasks for execution
        actioned_tasks = user_client.action_tasks(tasks, network_sk)

        # execute the actioned tasks and push results directly using statestore and object store
        with tmpdir.as_cwd():
            protocoldagresults = self._execute_tasks(actioned_tasks, n4js, s3os_server)

        # clear local gufe registry of pdr objects
        # not critical, but ensures we see the objects that are deserialized
        # instead of our instances already in memory post-pull
        for pdr in protocoldagresults:
            TOKENIZABLE_REGISTRY.pop(pdr.key, None)

        # user client : attempt to pull transformation results; should yield nothing
        protocolresult = user_client.get_transformation_results(transformation_sk)

        # with the way BrokenProtocol.gather constructs its output, we expect
        # this to be empty
        assert protocolresult is None

        # user client : instead, pull failures
        protocoldagresults_r = user_client.get_transformation_failures(
            transformation_sk
        )

        assert set(protocoldagresults_r) == set(protocoldagresults)
        assert len(protocoldagresults_r) == 2

        for pdr in protocoldagresults_r:
            assert pdr.transformation_key == transformation.key
            assert isinstance(pdr.extends_key, GufeKey) or pdr.extends_key is None
            assert not pdr.ok()

        # so we don't have to recompute the above, which depends on
        # function-scoped fixtures, we also test getting all network failures
        # here too
        network_failures = user_client.get_network_failures(network_sk)
        for tf_sk, pdrs in network_failures.items():
            if tf_sk == transformation_sk:
                assert set(pdrs) == set(protocoldagresults)
                assert len(pdrs) == 2

                for pdr in pdrs:
                    assert pdr.transformation_key == transformation.key
                    assert (
                        isinstance(pdr.extends_key, GufeKey) or pdr.extends_key is None
                    )
                    assert not pdr.ok()
                    assert len(pdr.protocol_unit_failures) == 1
            else:
                assert pdrs == []

    def test_get_task_results(
        self,
        scope_test,
        n4js_preloaded,
        s3os_server,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
        tmpdir,
    ):
        n4js = n4js_preloaded

        # select the transformation we want to compute
        an = network_tyk2
        transformation = list(an.edges)[0]

        network_sk = user_client.get_scoped_key(an, scope_test)
        transformation_sk = user_client.get_scoped_key(transformation, scope_test)

        # user client : create tasks for the transformation
        tasks = user_client.create_tasks(transformation_sk, count=2)

        # only tasks that do not extend an incomplete task are actioned
        actioned_tasks = user_client.action_tasks(tasks, network_sk)

        # execute the actioned tasks and push results directly using statestore and object store
        with tmpdir.as_cwd():
            protocoldagresults = self._execute_tasks(actioned_tasks, n4js, s3os_server)

        # clear local gufe registry of pdr objects
        # not critical, but ensures we see the objects that are deserialized
        # instead of our instances already in memory post-pull
        for pdr in protocoldagresults:
            TOKENIZABLE_REGISTRY.pop(pdr.key, None)

        # user client : pull task results, evaluate
        for task in tasks:
            protocoldagresults_r = user_client.get_task_results(task)

            assert len(protocoldagresults_r) == 1
            assert set(protocoldagresults_r).issubset(set(protocoldagresults))

            for pdr in protocoldagresults_r:
                assert pdr.transformation_key == transformation.key
                assert isinstance(pdr.extends_key, GufeKey) or pdr.extends_key is None
                assert pdr.ok()

    def test_get_task_failures(
        self,
        scope_test,
        n4js_preloaded,
        s3os_server,
        user_client: client.AlchemiscaleClient,
        network_tyk2_failure,
        tmpdir,
    ):
        n4js = n4js_preloaded

        # select the transformation we want to compute
        an = network_tyk2_failure
        network_sk = user_client.create_network(an, scope_test)

        # seeing what appears to be random race condition in CI; adding this
        # ensures full AlchemicalNetwork present before we proceed
        while True:
            try:
                an_ = user_client.get_network(network_sk)

                if an_ != an:
                    raise Exception("Network out doesn't exactly match network in yet")
                else:
                    break
            except Exception:
                sleep(0.1)

        tf_sks = user_client.get_network_transformations(network_sk)

        # select the transformation we want to compute
        for tf_sk in tf_sks:
            tf = user_client.get_transformation(tf_sk)
            if tf.name == "broken":
                transformation_sk = tf_sk
                transformation = tf
                break

        # user client : create tasks for the transformation
        tasks = user_client.create_tasks(transformation_sk, count=2)

        # only tasks that do not extend an incomplete task are actioned
        actioned_tasks = user_client.action_tasks(tasks, network_sk)

        # execute the actioned tasks and push results directly using statestore and object store
        with tmpdir.as_cwd():
            protocoldagresults = self._execute_tasks(actioned_tasks, n4js, s3os_server)

        # clear local gufe registry of pdr objects
        # not critical, but ensures we see the objects that are deserialized
        # instead of our instances already in memory post-pull
        for pdr in protocoldagresults:
            TOKENIZABLE_REGISTRY.pop(pdr.key, None)

        # user client : attempt to pull task results; should yield nothing
        for task in tasks:
            protocoldagresults_r = user_client.get_task_results(task)

            assert len(protocoldagresults_r) == 0

        for task in tasks:
            protocoldagresults_r = user_client.get_task_failures(task)

            assert len(protocoldagresults_r) == 1
            assert set(protocoldagresults_r).issubset(set(protocoldagresults))

            for pdr in protocoldagresults_r:
                assert pdr.transformation_key == transformation.key
                assert isinstance(pdr.extends_key, GufeKey) or pdr.extends_key is None
                assert not pdr.ok()

        # TODO: can we mix in a success in here somewhere?
        # not possible with current BrokenProtocol, unfortunately


class TestTaskRestartPolicy:

    default_max_retries = 3
    default_patterns = ["Pattern 1", "Pattern 2", "Pattern 3"]

    def create_default_network(self, network, client, scope):
        network_scoped_key = client.create_network(network, scope)
        client.add_task_restart_patterns(
            network_scoped_key, self.default_patterns, self.default_max_retries
        )
        return network_scoped_key

    def test_add_task_restart_patterns(
        self, user_client, network_tyk2, scope_test, n4js_preloaded
    ):

        network_scoped_key = self.create_default_network(
            network_tyk2, user_client, scope_test
        )

        query = """
        MATCH (trp: TaskRestartPattern)-[:ENFORCES]->(:TaskHub)-[:PERFORMS]->(:AlchemicalNetwork {`_scoped_key`: $network_scoped_key})
        RETURN trp
        """

        results = n4js_preloaded.execute_query(
            query, network_scoped_key=str(network_scoped_key)
        )

        assert len(results.records) == 3

        patterns_list = self.default_patterns[:]
        for record in results.records:
            trp = record["trp"]
            assert trp["pattern"] in patterns_list
            patterns_list.remove(trp["pattern"])

    def test_get_task_restart_patterns(
        self,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
        scope_test,
        n4js_preloaded,
    ):
        network_scoped_key = self.create_default_network(
            network_tyk2, user_client, scope_test
        )
        taskrestartpatterns = user_client.get_task_restart_patterns(network_scoped_key)
        expected = {
            pattern: self.default_max_retries for pattern in self.default_patterns
        }
        assert taskrestartpatterns == expected

    def test_remove_task_restart_patterns(
        self,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
        scope_test,
        n4js_preloaded,
    ):
        network_scoped_key = self.create_default_network(
            network_tyk2, user_client, scope_test
        )
        expected = {
            pattern: self.default_max_retries for pattern in self.default_patterns
        }

        # check that we have the expected 3 restart patterns
        assert user_client.get_task_restart_patterns(network_scoped_key) == expected

        pattern_to_remove = next(expected.__iter__())
        user_client.remove_task_restart_patterns(
            network_scoped_key, [pattern_to_remove]
        )
        del expected[pattern_to_remove]

        # check that one was removed
        assert user_client.get_task_restart_patterns(network_scoped_key) == expected

        patterns_to_remove = [pattern for pattern in expected]
        user_client.remove_task_restart_patterns(network_scoped_key, patterns_to_remove)

        # check the remaining patterns are removed
        assert user_client.get_task_restart_patterns(network_scoped_key) == {}

    def test_clear_task_restart_patterns(
        self,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
        scope_test,
        n4js_preloaded,
    ):
        network_scoped_key = self.create_default_network(
            network_tyk2, user_client, scope_test
        )

        query = """
        MATCH (trp:TaskRestartPattern)-[:ENFORCES]->(:TaskHub)-[:PERFORMS]->(:AlchemicalNetwork {`_scoped_key`: $network_scoped_key})
        RETURN trp
        """

        assert (
            len(
                n4js_preloaded.execute_query(
                    query, network_scoped_key=str(network_scoped_key)
                ).records
            )
            == 3
        )
        user_client.clear_task_restart_patterns(network_scoped_key)
        assert (
            len(
                n4js_preloaded.execute_query(
                    query, network_scoped_key=str(network_scoped_key)
                ).records
            )
            == 0
        )

    def test_set_task_restart_patterns_allowed_restarts(
        self,
        user_client: client.AlchemiscaleClient,
        network_tyk2,
        scope_test,
        n4js_preloaded,
    ):
        network_scoped_key = self.create_default_network(
            network_tyk2, user_client, scope_test
        )
        user_client.set_task_restart_patterns_allowed_restarts(
            network_scoped_key, self.default_patterns[:2], 1
        )

        expected = {pattern: 1 for pattern in self.default_patterns[:2]}
        expected[self.default_patterns[-1]] = self.default_max_retries
        assert user_client.get_task_restart_patterns(network_scoped_key) == expected
