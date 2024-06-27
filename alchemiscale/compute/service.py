"""
:mod:`alchemiscale.compute.service` --- compute services for FEC execution
==========================================================================

"""

import os
import gc
import asyncio
import sched
import time
import logging
from uuid import uuid4
import random
import threading
from typing import Union, Optional, List, Dict, Tuple
from pathlib import Path
from threading import Thread
import shutil

import requests

from gufe import Transformation
from gufe.protocols.protocoldag import execute_DAG, ProtocolDAG, ProtocolDAGResult

from .client import AlchemiscaleComputeClient
from .settings import ComputeServiceSettings
from ..storage.models import Task, TaskHub, ComputeServiceID
from ..models import Scope, ScopedKey


class SleepInterrupted(BaseException):
    """
    Exception class used to signal that an InterruptableSleep was interrupted

    This (like KeyboardInterrupt) derives from BaseException to prevent
    it from being handled with "except Exception".
    """

    pass


class InterruptableSleep:
    """
    A class for sleeping, but interruptable

    This class uses threading Events to wake up from a sleep before the entire sleep
    duration has run. If the sleep is interrupted, then an SleepInterrupted exception is raised.

    This class is a functor, so an instance can be passed as the delay function to a python
    sched.scheduler
    """

    def __init__(self):
        self._event = threading.Event()

    def __call__(self, delay: float):
        interrupted = self._event.wait(delay)
        if interrupted:
            raise SleepInterrupted()

    def interrupt(self):
        self._event.set()

    def clear(self):
        self._event.clear()


class SynchronousComputeService:
    """Fully synchronous compute service.

    This service is intended for use as a reference implementation, and for
    testing/debugging protocols.

    """

    def __init__(self, settings: ComputeServiceSettings):
        """Create a `SynchronousComputeService` instance."""
        self.settings = settings

        self.api_url = self.settings.api_url
        self.name = self.settings.name
        self.sleep_interval = self.settings.sleep_interval
        self.heartbeat_interval = self.settings.heartbeat_interval
        self.claim_limit = self.settings.claim_limit

        self.client = AlchemiscaleComputeClient(
            self.settings.api_url,
            self.settings.identifier,
            self.settings.key,
            max_retries=self.settings.client_max_retries,
            retry_base_seconds=self.settings.client_retry_base_seconds,
            retry_max_seconds=self.settings.client_retry_max_seconds,
            verify=self.settings.client_verify,
        )

        if self.settings.scopes is None:
            self.scopes = [Scope()]
        else:
            self.scopes = self.settings.scopes

        self.shared_basedir = Path(self.settings.shared_basedir).absolute()
        self.shared_basedir.mkdir(exist_ok=True)
        self.keep_shared = self.settings.keep_shared

        self.scratch_basedir = Path(self.settings.scratch_basedir).absolute()
        self.scratch_basedir.mkdir(exist_ok=True)
        self.keep_scratch = self.settings.keep_scratch

        self.scheduler = sched.scheduler(time.monotonic, time.sleep)

        self.compute_service_id = ComputeServiceID(f"{self.name}-{uuid4()}")

        self.int_sleep = InterruptableSleep()

        self._stop = False

        # logging
        extra = {"compute_service_id": str(self.compute_service_id)}
        logger = logging.getLogger("AlchemiscaleSynchronousComputeService")
        logger.setLevel(self.settings.loglevel)

        formatter = logging.Formatter(
            "[%(asctime)s] [%(compute_service_id)s] [%(levelname)s] %(message)s"
        )
        formatter.converter = time.gmtime  # use utc time for logging timestamps

        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        logger.addHandler(sh)

        if self.settings.logfile is not None:
            fh = logging.FileHandler(self.settings.logfile)
            fh.setFormatter(formatter)
            logger.addHandler(fh)

        self.logger = logging.LoggerAdapter(logger, extra)

    def _register(self):
        """Register this compute service with the compute API."""
        self.client.register(self.compute_service_id)

    def _deregister(self):
        """Deregister this compute service with the compute API."""
        self.client.deregister(self.compute_service_id)

    def beat(self):
        """Deliver a heartbeat to the compute API, indicating this service is still alive."""
        self.client.heartbeat(self.compute_service_id)
        self.logger.debug("Updated heartbeat")

    def heartbeat(self):
        """Start up the heartbeat, sleeping for `self.heartbeat_interval`"""
        while True:
            if self._stop:
                break
            self.beat()
            time.sleep(self.heartbeat_interval)

    def claim_tasks(
        self, count=1, protocols: Optional[List[str]] = None
    ) -> List[Optional[ScopedKey]]:
        """Get a Task to execute from compute API.

        Returns `None` if no Task was available matching service configuration.

        Parameters
        ----------
        count
            The maximum number of Tasks to claim.
        protocols
            Protocol names to restrict Task claiming to. `None` means no restriction.
            Regex patterns are allowed.

        """

        tasks = self.client.claim_tasks(
            scopes=self.scopes,
            compute_service_id=self.compute_service_id,
            count=count,
            protocols=protocols,
        )

        return tasks

    def task_to_protocoldag(
        self, task: ScopedKey
    ) -> Tuple[ProtocolDAG, Transformation, Optional[ProtocolDAGResult]]:
        """Given a Task, produce a corresponding ProtocolDAG that can be executed.

        Also gives the Transformation that this ProtocolDAG corresponds to.
        If the Task extends another Task, then the ProtocolDAGResult for that
        other Task is also given; otherwise `None` given.

        """

        (
            transformation,
            extends_protocoldagresult,
        ) = self.client.retrieve_task_transformation(task)

        protocoldag = transformation.create(
            extends=extends_protocoldagresult,
            name=str(task),
        )
        return protocoldag, transformation, extends_protocoldagresult

    def push_result(
        self, task: ScopedKey, protocoldagresult: ProtocolDAGResult
    ) -> ScopedKey:
        # TODO: this method should postprocess any paths,
        # leaf nodes in DAG for blob results that should go to object store

        # TODO: ship paths to object store

        # finally, push ProtocolDAGResult
        sk: ScopedKey = self.client.set_task_result(
            task, protocoldagresult, self.compute_service_id
        )

        return sk

    def execute(self, task: ScopedKey) -> ScopedKey:
        """Executes given Task.

        Returns ScopedKey of ProtocolDAGResultRef following push to database.

        """
        # obtain a ProtocolDAG from the task
        self.logger.info("Creating ProtocolDAG from '%s'...", task)
        protocoldag, transformation, extends = self.task_to_protocoldag(task)
        self.logger.info(
            "Created '%s' from '%s' performing '%s'",
            protocoldag,
            task,
            transformation.protocol,
        )

        # execute the task; this looks the same whether the ProtocolDAG is a
        # success or failure

        shared = self.shared_basedir / str(protocoldag.key)
        shared.mkdir()
        scratch = self.scratch_basedir / str(protocoldag.key)
        scratch.mkdir()

        self.logger.info("Executing '%s'...", protocoldag)
        try:
            protocoldagresult = execute_DAG(
                protocoldag,
                shared_basedir=shared,
                scratch_basedir=scratch,
                keep_scratch=self.keep_scratch,
                raise_error=False,
                n_retries=self.settings.n_retries,
            )
        finally:
            if not self.keep_shared:
                shutil.rmtree(shared)

            if not self.keep_scratch:
                shutil.rmtree(scratch)

        if protocoldagresult.ok():
            self.logger.info("'%s' -> '%s' : SUCCESS", protocoldag, protocoldagresult)
        else:
            for failure in protocoldagresult.protocol_unit_failures:
                self.logger.info(
                    "'%s' -> '%s' : FAILURE :: '%s' : %s",
                    protocoldag,
                    protocoldagresult,
                    failure,
                    failure.exception,
                )

        # push the result (or failure) back to the compute API
        result_sk = self.push_result(task, protocoldagresult)
        self.logger.info("Pushed result `%s'", protocoldagresult)

        return result_sk

    def _check_max_tasks(self, max_tasks):
        if max_tasks is not None:
            if self._tasks_counter >= max_tasks:
                self.logger.info(
                    "Performed %s tasks; at or beyond max tasks = %s",
                    self._tasks_counter,
                    max_tasks,
                )
                raise KeyboardInterrupt

    def _check_max_time(self, max_time):
        if max_time is not None:
            run_time = time.time() - self._start_time
            if run_time >= max_time:
                self.logger.info(
                    "Ran for %s seconds; at or beyond max time = %s seconds",
                    run_time,
                    max_time,
                )
                raise KeyboardInterrupt

    def cycle(self, max_tasks: Optional[int] = None, max_time: Optional[int] = None):
        self._check_max_tasks(max_tasks)
        self._check_max_time(max_time)

        # claim tasks from the compute API
        self.logger.info("Claiming tasks")
        tasks: List[ScopedKey] = self.claim_tasks(self.claim_limit)
        self.logger.info("Claimed %d tasks", len([t for t in tasks if t is not None]))

        # if no tasks claimed, sleep
        if all([task is None for task in tasks]):
            self.logger.info(
                "No tasks claimed; sleeping for %d seconds", self.sleep_interval
            )
            time.sleep(self.sleep_interval)
            return

        # otherwise, process tasks
        self.logger.info("Executing tasks...")
        for task in tasks:
            if task is None:
                continue

            # execute each task
            self.logger.info("Executing task '%s'...", task)
            self.execute(task)
            self.logger.info("Finished task '%s'", task)

            if max_tasks is not None:
                self._tasks_counter += 1

            # stop checks
            self._check_max_tasks(max_tasks)
            self._check_max_time(max_time)

        self._check_max_tasks(max_tasks)
        self._check_max_time(max_time)

    def start(self, max_tasks: Optional[int] = None, max_time: Optional[int] = None):
        """Start the service.

        Limits to the maximum number of executed tasks or seconds to run for
        can be set. The first maximum to be hit will trigger the service to
        exit.

        Parameters
        ----------
        max_tasks
            Max number of Tasks to execute before exiting.
            If `None`, the service will have no task limit.
        max_time
            Max number of seconds to run before exiting.
            If `None`, the service will have no time limit.

        """
        # add ComputeServiceRegistration
        self.logger.info("Starting up service '%s'", self.name)
        self._register()
        self.logger.info(
            "Registered service with registration '%s'", str(self.compute_service_id)
        )

        # start up heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.heartbeat, daemon=True)
        self.heartbeat_thread.start()

        # stop conditions will use these
        self._tasks_counter = 0
        self._start_time = time.time()

        try:
            self.logger.info("Starting main loop")
            while not self._stop:
                # check that heartbeat is still alive; if not, resurrect it
                if not self.heartbeat_thread.is_alive():
                    self.heartbeat_thread = threading.Thread(
                        target=self.heartbeat, daemon=True
                    )
                    self.heartbeat_thread.start()

                # perform main loop cycle
                self.cycle(max_tasks, max_time)

                # force a garbage collection to avoid consuming too much memory
                gc.collect()
        except KeyboardInterrupt:
            self.logger.info("Caught SIGINT/Keyboard interrupt.")
        except SleepInterrupted:
            self.logger.info("Service stopping.")
        finally:
            # remove ComputeServiceRegistration, drop all claims
            self._deregister()
            self.logger.info(
                "Deregistered service with registration '%s'",
                str(self.compute_service_id),
            )

    def stop(self):
        self.int_sleep.interrupt()
        self._stop = True


class AsynchronousComputeService(SynchronousComputeService):
    """Asynchronous compute service.

    This service can be used in production cases, though it does not make use
    of Folding@Home.

    """

    def __init__(self, api_url):
        self.scheduler = sched.scheduler(time.monotonic, time.sleep)
        # self.loop = asyncio.get_event_loop()

        self._stop = False

    def get_new_tasks(self): ...

    def start(self):
        """Start the service; will keep going until told to stop."""
        while True:
            if self._stop:
                return

    def stop(self):
        self._stop = True
