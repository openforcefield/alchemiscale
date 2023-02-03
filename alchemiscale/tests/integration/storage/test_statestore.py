import random
from time import sleep
from typing import List, Dict
from pathlib import Path

import pytest
from gufe import AlchemicalNetwork
from gufe.tokenization import TOKENIZABLE_REGISTRY
from gufe.protocols.protocoldag import execute_DAG, ProtocolDAG, ProtocolDAGResult

from alchemiscale.storage import Neo4jStore
from alchemiscale.storage.models import Task, TaskHub, ObjectStoreRef
from alchemiscale.models import Scope, ScopedKey
from alchemiscale.security.models import (
    CredentialedEntity,
    CredentialedUserIdentity,
    CredentialedComputeIdentity,
)
from alchemiscale.security.auth import hash_key


class TestStateStore:
    ...


class TestNeo4jStore(TestStateStore):
    ...

    @pytest.fixture
    def n4js(self, n4js_fresh):
        return n4js_fresh

    def test_server(self, graph):
        graph.service.system_graph.call("dbms.security.listUsers")

    ### gufe otject handling

    def test_create_network(self, n4js, network_tyk2, scope_test):
        an = network_tyk2

        sk: ScopedKey = n4js.create_network(an, scope_test)

        out = n4js.graph.run(
            f"""
                match (n:AlchemicalNetwork {{_gufe_key: '{an.key}', 
                                             _org: '{sk.org}', _campaign: '{sk.campaign}', 
                                             _project: '{sk.project}'}}) 
                return n
                """
        )
        n = out.to_subgraph()

        assert n["name"] == "tyk2_relative_benchmark"

    def test_create_overlapping_networks(self, n4js, network_tyk2, scope_test):
        an = network_tyk2

        sk: ScopedKey = n4js.create_network(an, scope_test)

        n = n4js.graph.run(
            f"""
                match (n:AlchemicalNetwork {{_gufe_key: '{an.key}', 
                                             _org: '{sk.org}', _campaign: '{sk.campaign}', 
                                             _project: '{sk.project}'}}) 
                return n
                """
        ).to_subgraph()

        assert n["name"] == "tyk2_relative_benchmark"

        # add the same network twice
        sk2: ScopedKey = n4js.create_network(an, scope_test)
        assert sk2 == sk

        n2 = n4js.graph.run(
            f"""
                match (n:AlchemicalNetwork {{_gufe_key: '{an.key}', 
                                             _org: '{sk.org}', _campaign: '{sk.campaign}', 
                                             _project: '{sk.project}'}}) 
                return n
                """
        ).to_subgraph()

        assert n2["name"] == "tyk2_relative_benchmark"
        assert n2.identity == n.identity

        # add a slightly different network
        an2 = AlchemicalNetwork(
            edges=list(an.edges)[:-1], name="tyk2_relative_benchmark_-1"
        )
        sk3 = n4js.create_network(an2, scope_test)
        assert sk3 != sk

        n3 = n4js.graph.run(
            f"""
                match (n:AlchemicalNetwork) 
                return n
                """
        ).to_subgraph()

        assert len(n3.nodes) == 2

    def test_delete_network(self):
        ...

    def test_get_network(self, n4js, network_tyk2, scope_test):
        an = network_tyk2
        sk: ScopedKey = n4js.create_network(an, scope_test)

        an2 = n4js.get_gufe(sk)

        assert an2 == an
        assert an2 is an

        TOKENIZABLE_REGISTRY.clear()

        an3 = n4js.get_gufe(sk)

        assert an3 == an2 == an

    def test_query_network(self, n4js, network_tyk2, scope_test):
        an = network_tyk2
        an2 = AlchemicalNetwork(edges=list(an.edges)[:-2], name="incomplete")

        sk: ScopedKey = n4js.create_network(an, scope_test)
        sk2: ScopedKey = n4js.create_network(an2, scope_test)

        networks_sk: List[ScopedKey] = n4js.query_networks()

        assert sk in networks_sk
        assert sk2 in networks_sk
        assert len(networks_sk) == 2

        # add in a scope test

        # add in a name test

    def test_query_transformations(self):
        ...

    def test_query_chemicalsystems(self):
        ...

    ### compute

    def test_create_task(self, n4js, network_tyk2, scope_test):
        # add alchemical network, then try generating task
        an = network_tyk2
        n4js.create_network(an, scope_test)

        transformation = list(an.edges)[0]
        transformation_sk = n4js.get_scoped_key(transformation, scope_test)

        task_sk: ScopedKey = n4js.create_task(transformation_sk)

        m = n4js.graph.run(
            f"""
                match (n:Task {{_gufe_key: '{task_sk.gufe_key}', 
                                             _org: '{task_sk.org}', _campaign: '{task_sk.campaign}', 
                                             _project: '{task_sk.project}'}})-[:PERFORMS]->(m:Transformation)
                return m
                """
        ).to_subgraph()

        assert m["_gufe_key"] == transformation.key

    def test_create_taskhub(self, n4js, network_tyk2, scope_test):
        # add alchemical network, then try adding a taskhub
        an = network_tyk2
        network_sk = n4js.create_network(an, scope_test)

        # create taskhub
        taskhub_sk: ScopedKey = n4js.create_taskhub(network_sk)

        # verify creation looks as we expect
        m = n4js.graph.run(
            f"""
                match (n:TaskHub {{_gufe_key: '{taskhub_sk.gufe_key}', 
                                             _org: '{taskhub_sk.org}', _campaign: '{taskhub_sk.campaign}', 
                                             _project: '{taskhub_sk.project}'}})-[:PERFORMS]->(m:AlchemicalNetwork)
                return m
                """
        ).to_subgraph()

        assert m["_gufe_key"] == an.key

        # try adding the task hub again; this should yield exactly the same node
        taskhub_sk2: ScopedKey = n4js.create_taskhub(network_sk)

        assert taskhub_sk2 == taskhub_sk

        records = n4js.graph.run(
            f"""
                match (n:TaskHub {{network: '{network_sk}', 
                                             _org: '{taskhub_sk.org}', _campaign: '{taskhub_sk.campaign}', 
                                             _project: '{taskhub_sk.project}'}})-[:PERFORMS]->(m:AlchemicalNetwork)
                return n
                """
        )

        assert len(list(records)) == 1

    def test_create_taskhub_weight(self, n4js, network_tyk2, scope_test):
        an = network_tyk2
        network_sk = n4js.create_network(an, scope_test)

        # create taskhub
        taskhub_sk: ScopedKey = n4js.create_taskhub(network_sk)

        n = n4js.graph.run(
            f"""
                match (n:TaskHub)
                return n
                """
        ).to_subgraph()

        assert n["weight"] == 0.5

        # change the weight
        n4js.set_taskhub_weight(network_sk, 0.7)

        n = n4js.graph.run(
            f"""
                match (n:TaskHub)
                return n
                """
        ).to_subgraph()

        assert n["weight"] == 0.7

    def test_query_taskhubs(self, n4js: Neo4jStore, network_tyk2, scope_test):
        an = network_tyk2
        network_sk = n4js.create_network(an, scope_test)
        taskhub_sk: ScopedKey = n4js.create_taskhub(network_sk)

        # add a slightly different network
        an2 = AlchemicalNetwork(
            edges=list(an.edges)[:-1], name="tyk2_relative_benchmark_-1"
        )
        network_sk2 = n4js.create_network(an2, scope_test)
        taskhub_sk2: ScopedKey = n4js.create_taskhub(network_sk2)

        tq_sks: List[ScopedKey] = n4js.query_taskhubs()
        assert len(tq_sks) == 2
        assert all([isinstance(i, ScopedKey) for i in tq_sks])

        tq_dict: Dict[ScopedKey, TaskHub] = n4js.query_taskhubs(return_gufe=True)
        assert len(tq_dict) == 2
        assert all([isinstance(i, TaskHub) for i in tq_dict.values()])

    def test_queue_task(self, n4js: Neo4jStore, network_tyk2, scope_test):
        an = network_tyk2
        network_sk = n4js.create_network(an, scope_test)
        taskhub_sk: ScopedKey = n4js.create_taskhub(network_sk)

        transformation = list(an.edges)[0]
        transformation_sk = n4js.get_scoped_key(transformation, scope_test)

        # create 10 tasks
        task_sks = [n4js.create_task(transformation_sk) for i in range(10)]

        # queue the tasks
        n4js.queue_taskhub_tasks(task_sks, taskhub_sk)

        # count tasks in queue
        queued_task_sks = n4js.get_taskhub_tasks(taskhub_sk)
        assert set(task_sks) == set(queued_task_sks)

        # add a second network, with the transformation above missing
        # try to add a task from that transformation to the new network's queue
        # this should fail

        an2 = AlchemicalNetwork(
            edges=list(an.edges)[1:], name="tyk2_relative_benchmark_-1"
        )
        assert transformation not in an2.edges

        network_sk2 = n4js.create_network(an2, scope_test)
        taskhub_sk2: ScopedKey = n4js.create_taskhub(network_sk2)

        with pytest.raises(ValueError, match="not found in same network"):
            task_sks_fail = n4js.queue_taskhub_tasks(task_sks, taskhub_sk2)

    def test_get_unclaimed_tasks(self, n4js: Neo4jStore, network_tyk2, scope_test):
        an = network_tyk2
        network_sk = n4js.create_network(an, scope_test)
        taskhub_sk: ScopedKey = n4js.create_taskhub(network_sk)

        transformation = list(an.edges)[0]
        transformation_sk = n4js.get_scoped_key(transformation, scope_test)

        # create 10 tasks
        task_sks = [n4js.create_task(transformation_sk) for i in range(10)]

        # shuffle the tasks; want to check that order of claiming is actually
        # based on order in queue
        random.shuffle(task_sks)

        # queue the tasks
        n4js.queue_taskhub_tasks(task_sks, taskhub_sk)

        # claim a single task; There is no deterministic ordering of tasks, so
        # simply test that the claimed task is one of the queued tasks
        claimed = n4js.claim_taskhub_tasks(taskhub_sk, "the best task handler")

        assert claimed[0] in task_sks

        # query for unclaimed tasks
        unclaimed = n4js.get_taskhub_unclaimed_tasks(taskhub_sk)

        assert set(unclaimed) == set(task_sks) - set(claimed)
        assert len(unclaimed) == 9

    def test_get_set_weights(self, n4js: Neo4jStore, network_tyk2, scope_test):
        an = network_tyk2
        network_sk = n4js.create_network(an, scope_test)
        taskhub_sk: ScopedKey = n4js.create_taskhub(network_sk)

        transformation = list(an.edges)[0]
        transformation_sk = n4js.get_scoped_key(transformation, scope_test)

        # create 10 tasks
        task_sks = [n4js.create_task(transformation_sk) for i in range(10)]
        n4js.queue_taskhub_tasks(task_sks, taskhub_sk)

        # weights should all be the default 1.0
        weights = n4js.get_task_weights(task_sks, taskhub_sk)
        assert all([w == 1.0 for sk, w in weights.items()])
        # set weights on the tasks to be all 10
        n4js.set_task_weights(task_sks, taskhub_sk, weight=10)
        weights = n4js.get_task_weights(task_sks, taskhub_sk)
        assert all([w == 10 for sk, w in weights.items()])

    def test_claim_task(self, n4js: Neo4jStore, network_tyk2, scope_test):
        an = network_tyk2
        network_sk = n4js.create_network(an, scope_test)
        taskhub_sk: ScopedKey = n4js.create_taskhub(network_sk)

        transformation = list(an.edges)[0]
        transformation_sk = n4js.get_scoped_key(transformation, scope_test)

        # create 10 tasks
        task_sks = [n4js.create_task(transformation_sk) for i in range(10)]

        # shuffle the tasks; want to check that order of claiming is actually
        # based on order in queue
        random.shuffle(task_sks)

        # try to claim from an empty queue
        nothing = n4js.claim_taskhub_tasks(taskhub_sk, "early bird task handler")

        assert nothing[0] is None

        # queue the tasks
        n4js.queue_taskhub_tasks(task_sks, taskhub_sk)

        # claim a single task; There is no deterministic ordering of tasks, so
        # simply test that the claimed task is one of the queued tasks
        claimed = n4js.claim_taskhub_tasks(taskhub_sk, "the best task handler")

        assert claimed[0] in task_sks

        # filter out the claimed task so that we have clean list of remaining
        # tasks
        remaining_tasks = n4js.get_taskhub_unclaimed_tasks(taskhub_sk)

        # set all tasks to priority 5, first task to priority 1; claim should
        # yield first task
        for task_sk in remaining_tasks:
            n4js.set_task_priority(task_sk, 5)
        n4js.set_task_priority(remaining_tasks[0], 1)

        claimed2 = n4js.claim_taskhub_tasks(taskhub_sk, "another task handler")
        assert claimed2[0] == remaining_tasks[0]

        remaining_tasks = n4js.get_taskhub_unclaimed_tasks(taskhub_sk)

        # next task claimed should be one of the remaining tasks
        claimed3 = n4js.claim_taskhub_tasks(taskhub_sk, "yet another task handler")
        assert claimed3[0] in remaining_tasks

        remaining_tasks = n4js.get_taskhub_unclaimed_tasks(taskhub_sk)

        # try to claim multiple tasks
        claimed4 = n4js.claim_taskhub_tasks(taskhub_sk, "last task handler", count=4)
        assert len(claimed4) == 4
        for sk in claimed4:
            assert sk in remaining_tasks

        # exhaust the queue
        claimed5 = n4js.claim_taskhub_tasks(taskhub_sk, "last task handler", count=3)

        # try to claim from a queue with no tasks available
        claimed6 = n4js.claim_taskhub_tasks(taskhub_sk, "last task handler", count=2)
        assert claimed6 == [None] * 2

    def test_claim_task_byweight(self, n4js: Neo4jStore, network_tyk2, scope_test):
        an = network_tyk2
        network_sk = n4js.create_network(an, scope_test)
        taskhub_sk: ScopedKey = n4js.create_taskhub(network_sk)

        transformation = list(an.edges)[0]
        transformation_sk = n4js.get_scoped_key(transformation, scope_test)

        # create 10 tasks
        task_sks = [n4js.create_task(transformation_sk) for i in range(10)]

        # shuffle the tasks; want to check that order of claiming is actually
        # based on order in queue
        random.shuffle(task_sks)

        # try to claim from an empty queue

        # queue the tasks
        n4js.queue_taskhub_tasks(task_sks, taskhub_sk)

        # set weights on the tasks to be all 0, disabling them
        n4js.set_task_weights(task_sks, taskhub_sk, weight=0)
        # set the weight of the first task to be 10
        weight_dict = {task_sks[0]: 10}
        n4js.set_task_weights(weight_dict, taskhub_sk)
        # check that the claimed task is the first task
        claimed = n4js.claim_taskhub_tasks(taskhub_sk, "the best task handler")
        assert claimed[0] == task_sks[0]
        # claim again; should get None as no other tasks have any weight
        claimed_again = n4js.claim_taskhub_tasks(taskhub_sk, "the best task handler")
        assert claimed_again[0] == None

    def test_set_task_result(self, n4js: Neo4jStore, network_tyk2, scope_test, tmpdir):
        # need to understand why ProtocolDAGResult fails to be represented as
        # subgraph

        an = network_tyk2
        network_sk = n4js.create_network(an, scope_test)
        taskhub_sk: ScopedKey = n4js.create_taskhub(network_sk)

        transformation = list(an.edges)[0]
        transformation_sk = n4js.get_scoped_key(transformation, scope_test)

        # create a task; compute its result, and attempt to submit it
        task_sk = n4js.create_task(transformation_sk)

        transformation, protocoldag_prev = n4js.get_task_transformation(task_sk)
        protocoldag = transformation.protocol.create(
            stateA=transformation.stateA,
            stateB=transformation.stateB,
            mapping=transformation.mapping,
            extend_from=protocoldag_prev,
            name=str(task_sk),
        )

        # execute the task
        with tmpdir.as_cwd():
            protocoldagresult = execute_DAG(protocoldag, shared=Path(".").absolute())

        osr = ObjectStoreRef(location="protocoldagresult/{protocoldagresult.key}")

        # try to push the result
        n4js.set_task_result(task_sk, osr)

        n = n4js.graph.run(
            f"""
                match (n:ObjectStoreRef)<-[:RESULTS_IN]-(t:Task)
                return n
                """
        ).to_subgraph()

        assert n["location"] == osr.location

    ### authentication

    @pytest.mark.parametrize(
        "credential_type", [CredentialedUserIdentity, CredentialedComputeIdentity]
    )
    def test_create_credentialed_entity(
        self, n4js: Neo4jStore, credential_type: CredentialedEntity
    ):
        user = credential_type(
            identifier="bill",
            hashed_key=hash_key("and ted"),
        )

        cls_name = credential_type.__name__

        n4js.create_credentialed_entity(user)

        n = n4js.graph.run(
            f"""
            match (n:{cls_name} {{identifier: '{user.identifier}'}})
            return n
            """
        ).to_subgraph()

        assert n["identifier"] == user.identifier
        assert n["hashed_key"] == user.hashed_key

    @pytest.mark.parametrize(
        "credential_type", [CredentialedUserIdentity, CredentialedComputeIdentity]
    )
    def test_get_credentialed_entity(
        self, n4js: Neo4jStore, credential_type: CredentialedEntity
    ):
        user = credential_type(
            identifier="bill",
            hashed_key=hash_key("and ted"),
        )

        n4js.create_credentialed_entity(user)

        # get the user back
        user_g = n4js.get_credentialed_entity(user.identifier, credential_type)

        assert user_g == user

    @pytest.mark.parametrize(
        "credential_type", [CredentialedUserIdentity, CredentialedComputeIdentity]
    )
    def test_list_credentialed_entities(
        self, n4js: Neo4jStore, credential_type: CredentialedEntity
    ):
        identities = ("bill", "ted", "napoleon")

        for ident in identities:
            user = credential_type(
                identifier=ident,
                hashed_key=hash_key("a string for a key"),
            )

            n4js.create_credentialed_entity(user)

        # get the user back
        identities_ = n4js.list_credentialed_entities(credential_type)

        assert set(identities) == set(identities_)

    @pytest.mark.parametrize(
        "credential_type", [CredentialedUserIdentity, CredentialedComputeIdentity]
    )
    def test_remove_credentialed_entity(
        self, n4js: Neo4jStore, credential_type: CredentialedEntity
    ):
        user = credential_type(
            identifier="bill",
            hashed_key=hash_key("and ted"),
        )

        n4js.create_credentialed_entity(user)

        # get the user back
        user_g = n4js.get_credentialed_entity(user.identifier, credential_type)

        assert user_g == user

        n4js.remove_credentialed_identity(user.identifier, credential_type)
        with pytest.raises(KeyError):
            n4js.get_credentialed_entity(user.identifier, credential_type)

    @pytest.mark.parametrize(
        "credential_type", [CredentialedUserIdentity, CredentialedComputeIdentity]
    )
    @pytest.mark.parametrize(
        "scope_strs", (["*-*-*"], ["a-*-*"], ["a-b-*"], ["a-b-c", "a-b-d"])
    )
    def test_list_scope(
        self,
        n4js: Neo4jStore,
        credential_type: CredentialedEntity,
        scope_strs: List[str],
    ):
        user = credential_type(
            identifier="bill",
            hashed_key=hash_key("and ted"),
        )

        n4js.create_credentialed_entity(user)
        ref_scopes = []
        for scope_str in scope_strs:
            scope = Scope.from_str(scope_str)
            ref_scopes.append(scope)
            n4js.add_scope(user.identifier, credential_type, scope)

        scopes = n4js.list_scopes(user.identifier, credential_type)
        assert set(scopes) == set(ref_scopes)

    @pytest.mark.parametrize(
        "credential_type", [CredentialedUserIdentity, CredentialedComputeIdentity]
    )
    @pytest.mark.parametrize("scope_str", ("*-*-*", "a-*-*", "a-b-*", "a-b-c"))
    def test_add_scope(
        self, n4js: Neo4jStore, credential_type: CredentialedEntity, scope_str: str
    ):
        user = credential_type(
            identifier="bill",
            hashed_key=hash_key("and ted"),
        )

        n4js.create_credentialed_entity(user)

        scope = Scope.from_str(scope_str)

        n4js.add_scope(user.identifier, credential_type, scope)

        q = f"""
        MATCH (n:{credential_type.__name__} {{identifier: '{user.identifier}'}})
        RETURN n
        """
        scopes_qr = n4js.graph.run(q).to_subgraph()
        scopes = scopes_qr.get("scopes")
        assert len(scopes) == 1

        new_scope = Scope.from_str(scopes[0])
        assert new_scope == scope

    @pytest.mark.parametrize(
        "credential_type", [CredentialedUserIdentity, CredentialedComputeIdentity]
    )
    def test_add_scope_duplicate(
        self, n4js: Neo4jStore, credential_type: CredentialedEntity
    ):
        user = credential_type(
            identifier="bill",
            hashed_key=hash_key("and ted"),
        )

        n4js.create_credentialed_entity(user)

        scope1 = Scope.from_str("*-*-*")
        scope2 = Scope.from_str("*-*-*")

        n4js.add_scope(user.identifier, credential_type, scope1)
        n4js.add_scope(user.identifier, credential_type, scope2)

        q = f"""
        MATCH (n:{credential_type.__name__} {{identifier: '{user.identifier}'}})
        RETURN n
        """
        scopes_qr = n4js.graph.run(q).to_subgraph()
        scopes = scopes_qr.get("scopes")
        assert len(scopes) == 1

        new_scope = Scope.from_str(scopes[0])
        assert new_scope == scope1

    @pytest.mark.parametrize(
        "credential_type", [CredentialedUserIdentity, CredentialedComputeIdentity]
    )
    @pytest.mark.parametrize("scope_str", ("*-*-*", "a-*-*", "a-b-*", "a-b-c"))
    def test_remove_scope(
        self, n4js: Neo4jStore, credential_type: CredentialedEntity, scope_str: str
    ):
        user = credential_type(
            identifier="bill",
            hashed_key=hash_key("and ted"),
        )

        n4js.create_credentialed_entity(user)

        scope = Scope.from_str(scope_str)
        not_removed = Scope.from_str("scope-not-removed")

        n4js.add_scope(user.identifier, credential_type, scope)
        n4js.add_scope(user.identifier, credential_type, not_removed)

        n4js.remove_scope(user.identifier, credential_type, scope)

        scopes = n4js.list_scopes(user.identifier, credential_type)
        assert scope not in scopes
        assert not_removed in scopes

    @pytest.mark.parametrize(
        "credential_type", [CredentialedUserIdentity, CredentialedComputeIdentity]
    )
    @pytest.mark.parametrize("scope_str", ("*-*-*", "a-*-*", "a-b-*", "a-b-c"))
    def test_remove_scope_duplicate(
        self, n4js: Neo4jStore, credential_type: CredentialedEntity, scope_str: str
    ):
        user = credential_type(
            identifier="bill",
            hashed_key=hash_key("and ted"),
        )

        n4js.create_credentialed_entity(user)

        scope = Scope.from_str(scope_str)
        not_removed = Scope.from_str("scope-not-removed")

        n4js.add_scope(user.identifier, credential_type, scope)
        n4js.add_scope(user.identifier, credential_type, not_removed)

        n4js.remove_scope(user.identifier, credential_type, scope)
        n4js.remove_scope(user.identifier, credential_type, scope)

        scopes = n4js.list_scopes(user.identifier, credential_type)
        assert scope not in scopes
        assert not_removed in scopes