from datetime import datetime

from gufe.protocols import ProtocolUnitFailure

from alchemiscale.storage.statestore import Neo4jStore
from alchemiscale import ScopedKey
from alchemiscale.storage.models import TaskStatusEnum, ProtocolDAGResultRef


def tasks_are_not_actioned_on_taskhub(
    n4js: Neo4jStore,
    task_scoped_keys: list[ScopedKey],
    taskhub_scoped_key: ScopedKey,
) -> bool:

    actioned_tasks = n4js.get_taskhub_actioned_tasks([taskhub_scoped_key])

    for task in task_scoped_keys:
        if task in actioned_tasks[0].keys():
            return False
    return True


def tasks_are_errored(n4js: Neo4jStore, task_scoped_keys: list[ScopedKey]) -> bool:
    query = """
    UNWIND $task_scoped_keys as task_scoped_key
    MATCH (task:Task {_scoped_key: task_scoped_key, status: $error})
    RETURN task
    """

    results = n4js.execute_query(
        query,
        task_scoped_keys=list(map(str, task_scoped_keys)),
        error=TaskStatusEnum.error.value,
    )

    return len(results.records) == len(task_scoped_keys)


def tasks_are_waiting(n4js: Neo4jStore, task_scoped_keys: list[ScopedKey]) -> bool:
    query = """
    UNWIND $task_scoped_keys as task_scoped_key
    MATCH (task:Task {_scoped_key: task_scoped_key, status: $waiting})
    RETURN task
    """

    results = n4js.execute_query(
        query,
        task_scoped_keys=list(map(str, task_scoped_keys)),
        waiting=TaskStatusEnum.waiting.value,
    )

    return len(results.records) == len(task_scoped_keys)


def complete_tasks(
    n4js: Neo4jStore,
    tasks: list[ScopedKey],
):
    n4js.set_task_running(tasks)
    for task in tasks:
        ok_pdrr = ProtocolDAGResultRef(
            ok=True,
            datetime_created=datetime.utcnow(),
            obj_key=task.gufe_key,
            scope=task.scope,
        )

        _ = n4js.set_task_result(task, ok_pdrr)

    n4js.set_task_complete(tasks)


def fail_task(
    n4js: Neo4jStore,
    task: ScopedKey,
    resolve: bool = False,
    error_messages: list[str] = [],
) -> None:
    n4js.set_task_running([task])

    not_ok_pdrr = ProtocolDAGResultRef(
        ok=False,
        datetime_created=datetime.utcnow(),
        obj_key=task.gufe_key,
        scope=task.scope,
    )

    protocol_unit_failures = []
    for j, message in enumerate(error_messages):
        puf = ProtocolUnitFailure(
            source_key=f"FakeProtocolUnitKey-123{j}",
            inputs={},
            outputs={},
            exception=RuntimeError,
            traceback=message,
        )
        protocol_unit_failures.append(puf)

    pdrr_scoped_key = n4js.set_task_result(task, not_ok_pdrr)

    n4js.add_protocol_dag_result_ref_tracebacks(protocol_unit_failures, pdrr_scoped_key)
    n4js.set_task_error([task])

    if resolve:
        n4js.resolve_task_restarts([task])
