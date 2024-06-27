import pytest
from time import sleep

from gufe.tokenization import GufeTokenizable

from alchemiscale.models import ScopedKey
from alchemiscale.compute import client
from alchemiscale.storage.models import TaskStatusEnum, ComputeServiceID

from alchemiscale.tests.integration.compute.utils import get_compute_settings_override


class TestComputeClient:
    def test_wrong_credential(
        self,
        scope_test,
        n4js_preloaded,
        compute_client_wrong_credential: client.AlchemiscaleComputeClient,
        uvicorn_server,
    ):
        with pytest.raises(client.AlchemiscaleComputeClientError):
            compute_client_wrong_credential.get_info()

    def test_refresh_credential(
        self,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        uvicorn_server,
    ):
        settings = get_compute_settings_override()
        assert compute_client._jwtoken == None
        compute_client._get_token()

        token = compute_client._jwtoken
        assert token is not None

        # token shouldn't change this fast
        compute_client.get_info()
        assert token == compute_client._jwtoken

        # should change if we wait a bit
        sleep(settings.JWT_EXPIRE_SECONDS + 2)
        compute_client.get_info()
        assert token != compute_client._jwtoken

    def test_api_check(
        self,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        uvicorn_server,
    ):
        compute_client._api_check()

    def test_register(
        self,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        uvicorn_server,
        compute_service_id,
    ):
        out = compute_client.register(compute_service_id)
        assert out == compute_service_id

        csreg = n4js_preloaded.graph.execute_query(
            f"""
            match (csreg:ComputeServiceRegistration {{identifier: '{compute_service_id}'}})
            return csreg
            """
        )

        assert csreg.records
        assert (
            csreg.records[0]["csreg"]["registered"]
            == csreg.records[0]["csreg"]["heartbeat"]
        )

    def test_deregister(
        self,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        uvicorn_server,
        compute_service_id,
    ):
        out = compute_client.register(compute_service_id)
        assert out == compute_service_id

        out = compute_client.deregister(compute_service_id)
        assert out == compute_service_id

        q = f"""
            match (csreg:ComputeServiceRegistration {{identifier: '{compute_service_id}'}})
            return csreg
            """

        csreg = n4js_preloaded.graph.execute_query(q)

        assert not csreg.records

    def test_heartbeat(
        self,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        uvicorn_server,
        compute_service_id,
    ):
        compute_client.register(compute_service_id)

        out = compute_client.heartbeat(compute_service_id)
        assert out == compute_service_id

        q = f"""
            match (csreg:ComputeServiceRegistration {{identifier: '{compute_service_id}'}})
            return csreg
            """

        csreg = n4js_preloaded.graph.execute_query(q)

        assert csreg.records
        assert len(csreg.records) == 1

        csreg = csreg.records[0]["csreg"]

        assert csreg["registered"] < csreg["heartbeat"]

    def test_list_scope(
        self,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        uvicorn_server,
        scope_test,
    ):
        scopes = compute_client.list_scopes()
        # scope_test matches identity used to initialise the client in conftest
        assert scopes == [scope_test]

    ### compute

    def test_query_taskhubs(
        self,
        scope_test,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        uvicorn_server,
    ):
        taskhub_sks = compute_client.query_taskhubs([scope_test])

        assert len(taskhub_sks) == 2

        taskhubs = compute_client.query_taskhubs([scope_test], return_gufe=True)
        assert all([tq.weight == 0.5 for tq in taskhubs.values()])

    def test_claim_taskhub_task(
        self,
        scope_test,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        compute_service_id,
        uvicorn_server,
    ):
        # register compute service id
        compute_client.register(compute_service_id)

        taskhub_sks = compute_client.query_taskhubs([scope_test])

        remaining_tasks = n4js_preloaded.get_taskhub_unclaimed_tasks(taskhub_sks[0])
        assert len(remaining_tasks) == 3

        # claim a single task; should get highest priority task
        task_sks = compute_client.claim_taskhub_tasks(
            taskhub_sks[0], compute_service_id=compute_service_id
        )

        remaining_tasks = n4js_preloaded.get_taskhub_unclaimed_tasks(taskhub_sks[0])
        assert len(remaining_tasks) == 2

        all_tasks = n4js_preloaded.get_taskhub_tasks(taskhub_sks[0], return_gufe=True)

        assert len(task_sks) == 1
        assert task_sks[0] in all_tasks.keys()
        assert [t.gufe_key for t in task_sks] == [
            task.key for task in all_tasks.values() if task.priority == 1
        ]

        remaining_tasks = n4js_preloaded.get_taskhub_unclaimed_tasks(taskhub_sks[0])
        # claim two more tasks
        task_sks2 = compute_client.claim_taskhub_tasks(
            taskhub_sks[0], count=2, compute_service_id=compute_service_id
        )
        assert task_sks2[0] in remaining_tasks
        assert task_sks2[1] in remaining_tasks

    def test_claim_tasks(
        self,
        scope_test,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        compute_service_id,
        uvicorn_server,
    ):
        # register compute service id
        compute_client.register(compute_service_id)

        # claim a single task; should get highest priority task
        task_sks = compute_client.claim_tasks(
            scopes=[scope_test],
            compute_service_id=compute_service_id,
        )
        all_tasks = n4js_preloaded.query_tasks(scope=scope_test)
        priorities = {
            task_sk: priority
            for task_sk, priority in zip(
                all_tasks, n4js_preloaded.get_task_priority(all_tasks)
            )
        }

        assert len(task_sks) == 1
        assert task_sks[0] in all_tasks
        assert [t.gufe_key for t in task_sks] == [
            t.gufe_key for t in all_tasks if priorities[t] == 1
        ]

    def test_get_task_transformation(
        self,
        scope_test,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        compute_service_id,
        network_tyk2,
        transformation,
        uvicorn_server,
    ):
        # register compute service id
        compute_client.register(compute_service_id)

        an_sk = ScopedKey(gufe_key=network_tyk2.key, **scope_test.dict())

        taskhub_sk = n4js_preloaded.get_taskhub(an_sk)

        # claim our first task
        task_sks = compute_client.claim_taskhub_tasks(
            taskhub_sk, compute_service_id=compute_service_id
        )

        # get the transformation corresponding to this task
        (
            transformation_,
            extends_protocoldagresult,
        ) = compute_client.retrieve_task_transformation(task_sks[0])

        assert transformation_ == transformation
        assert extends_protocoldagresult is None

    ### results

    def test_set_task_result(
        self,
        scope_test,
        n4js_preloaded,
        compute_client: client.AlchemiscaleComputeClient,
        compute_service_id,
        network_tyk2,
        transformation,
        protocoldagresults,
        uvicorn_server,
    ):
        # register compute service id
        compute_client.register(compute_service_id)

        an_sk = ScopedKey(gufe_key=network_tyk2.key, **scope_test.dict())
        tf_sk = ScopedKey(gufe_key=transformation.key, **scope_test.dict())
        taskhub_sk = n4js_preloaded.get_taskhub(an_sk)

        # claim our first task
        task_sks = compute_client.claim_taskhub_tasks(
            taskhub_sk, compute_service_id=compute_service_id
        )

        # get the transformation corresponding to this task
        (
            transformation_,
            extends_protocoldagresult,
        ) = compute_client.retrieve_task_transformation(task_sks[0])

        assert transformation_ == transformation
        assert extends_protocoldagresult is None

        # push a result for the task
        pdr_sk = compute_client.set_task_result(task_sks[0], protocoldagresults[0])

        # now, create a task that extends the one we just "performed"
        task_sk2 = n4js_preloaded.create_task(tf_sk, extends=task_sks[0])

        # get the transformation and the protocoldagresult for the task this extends
        # no need to claim to actually do this
        (
            transformation2,
            extends_protocoldagresult2,
        ) = compute_client.retrieve_task_transformation(task_sk2)

        assert transformation2 == transformation_
        assert extends_protocoldagresult2 == protocoldagresults[0]
