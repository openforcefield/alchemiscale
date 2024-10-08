"""
:mod:`alchemiscale.interface.client` --- client for interacting with user-facing API
====================================================================================

"""

import asyncio
from typing import Union, List, Dict, Optional, Tuple, Any, Iterable
import json
from itertools import chain
from functools import lru_cache

from async_lru import alru_cache
import networkx as nx
from gufe import AlchemicalNetwork, Transformation, ChemicalSystem
from gufe.tokenization import GufeTokenizable, JSON_HANDLER
from gufe.protocols import ProtocolResult, ProtocolDAGResult


from ..base.client import (
    AlchemiscaleBaseClient,
    AlchemiscaleBaseClientError,
    use_session,
)
from ..models import Scope, ScopedKey
from ..storage.models import (
    TaskStatusEnum,
    NetworkStateEnum,
)
from ..strategies import Strategy
from ..validators import validate_network_nonself
from ..keyedchain import KeyedChain

from warnings import warn


class AlchemiscaleClientError(AlchemiscaleBaseClientError): ...


def _get_transformation_results(client_settings, tf_sk, ok: bool, kwargs):
    client = AlchemiscaleClient(**client_settings)
    if ok:
        return tf_sk, client.get_transformation_results(tf_sk, **kwargs)
    else:
        return tf_sk, client.get_transformation_failures(tf_sk, **kwargs)


class AlchemiscaleClient(AlchemiscaleBaseClient):
    """Client for user interaction with API service."""

    _exception = AlchemiscaleClientError

    def get_scopes(self) -> List[Scope]:
        scopes = self._get_resource(
            f"/identities/{self.identifier}/scopes",
        )
        return sorted([Scope.from_str(s) for s in scopes])

    def list_scopes(self) -> List[Scope]:
        return self.get_scopes()

    ### inputs

    def get_scoped_key(self, obj: GufeTokenizable, scope: Scope) -> ScopedKey:
        """Given any gufe object and a fully-specified Scope, return corresponding ScopedKey.

        This method does not check that this ScopedKey is represented in the database.
        It is only a convenience for properly constructing a ScopedKey from a
        gufe object and a Scope.

        """

        msg = """`get_scoped_key` will be removed in v0.5.0

        For accurate server-side ScopedKeys, use the relevant methods for the GufeTokenizable of interest.
        For instance, `get_network_transformations` should be used to get Transformation ScopedKeys for a given AlchemicalNetwork.

        """
        warn(
            msg,
            DeprecationWarning,
        )

        if scope.specific():
            return ScopedKey(gufe_key=obj.key, **scope.dict())
        else:
            raise ValueError(
                "Scope for a ScopedKey must be specific; it cannot contain wildcards."
            )

    def check_exists(self, scoped_key: ScopedKey) -> bool:
        """Returns ``True`` if the given ScopedKey represents an object in the database."""
        return self._get_resource(f"/exists/{scoped_key}")

    def create_network(
        self,
        network: AlchemicalNetwork,
        scope: Scope,
        state: Union[NetworkStateEnum, str] = NetworkStateEnum.active,
        compress: Union[bool, int] = True,
        visualize: bool = True,
    ) -> ScopedKey:
        """Submit an AlchemicalNetwork to a specific Scope.

        Parameters
        ----------
        network
            The AlchemicalNetwork to submit.
        scope
            The Scope in which to submit the AlchemicalNetwork.
            This must be a *specific* Scope; it must not contain wildcards.
        state
            The starting state of the AlchemicalNetwork in the database.
            See :meth:`AlchemiscaleClient.set_network_state` for valid states.
        compress
            If ``True``, compress the AlchemicalNetwork client-side before
            shipping to the API service. This can reduce submission time
            depending on the bandwidth of your connection to the API service.
            Set to ``False`` to submit without compressing. This is a
            performance optimization; it has no bearing on the result of this
            method call.

            Use an integer between 0 and 9 for finer control over
            the degree of compression; 0 means no compression, 9 means max
            compression. ``True`` is synonymous with level 5 compression.
        visualize
            If ``True``, show submission progress indicator.

        Returns
        -------
        ScopedKey
            The ScopedKey of the AlchemicalNetwork.

        """
        if not scope.specific():
            raise ValueError(
                f"`scope` '{scope}' contains wildcards ('*'); `scope` must be *specific*"
            )

        validate_network_nonself(network)

        state = NetworkStateEnum(state)

        sk = ScopedKey(gufe_key=network.key, **scope.dict())

        def post():
            keyed_chain = KeyedChain.gufe_to_keyed_chain_rep(network)
            data = dict(network=keyed_chain, scope=scope.dict(), state=state.value)
            return self._post_resource("/networks", data, compress=compress)

        if visualize:
            from rich.progress import Progress

            with Progress(*self._rich_waiting_columns(), transient=False) as progress:
                task = progress.add_task(
                    f"Submitting [bold]'{sk}'[/bold]...", total=None
                )

                scoped_key = post()
                progress.start_task(task)
                progress.update(task, total=1, completed=1)
        else:
            scoped_key = post()

        return ScopedKey.from_dict(scoped_key)

    def set_network_state(
        self, network: ScopedKey, state: Union[NetworkStateEnum, str]
    ) -> Optional[ScopedKey]:
        """Set the state of an AlchemicalNetwork.

        Parameters
        ----------
        network
            The network whose state will be updated.
        state
            The state to set the network to. Valid options include "active",
            "inactive", "invalid", and "deleted".

        Returns
        -------
        Optional[ScopedKey]
            The ScopedKey of the updated network. If the network was not found, ``None``
            is returned.
        """
        return self.set_networks_state([network], [state])[0]

    def set_networks_state(
        self, networks: List[ScopedKey], states: List[Union[NetworkStateEnum, str]]
    ) -> List[Optional[ScopedKey]]:
        """Set the state of a list of AlchemicalNetworks.

        Parameters
        ----------
        networks
            The networks whose states will be updated.
        states
            The states to set the networks to, in the same order as `networks`.
            Values must be in a list of the same length as the networks
            parameter. Valid options include "active", "inactive", "invalid",
            and "deleted".

        Returns
        -------
        List[Optional[ScopedKey]]
            The ScopedKeys of the updated networks. If a network was not found, ``None``
            is returned at the corresponding index.
        """
        data = dict(networks=list(map(str, networks)), states=states)
        networks_updated = self._post_resource("/bulk/networks/state/set", data=data)
        return [
            ScopedKey.from_str(network_sk) if network_sk is not None else None
            for network_sk in networks_updated
        ]

    def get_network_state(self, network: ScopedKey) -> Optional[str]:
        """Get the state of an AlchemicalNetwork.

        Parameters
        ----------
        network
            The ScopedKey of the AlchemicalNetwork to get the state for.

        Returns
        -------
        Optional[str]
            The state of the AlchemicalNetwork. If the network was not found in
            the database, ``None`` is returned instead.
        """
        return self.get_networks_state([network])[0]

    def get_networks_state(self, networks: List[ScopedKey]) -> List[Optional[str]]:
        """Get the states for a list of AlchemicalNetworks.

        Parameters
        ----------
        networks
            A list of ScopedKeys for the AlchemicalNetworks to get the states
            of.

        Returns
        -------
        List[Optional[str]]
            A list of network states, in the same order as the specified
            networks. If a network was not found in the database, the
            corresponding entry in this list is ``None``.
        """
        data = dict(networks=list(map(str, networks)))
        states = self._post_resource("/bulk/networks/state/get", data=data)
        return states

    def query_networks(
        self,
        name: Optional[str] = None,
        scope: Optional[Scope] = None,
        state: Optional[Union[NetworkStateEnum, str]] = NetworkStateEnum.active,
    ) -> List[ScopedKey]:
        """Query for AlchemicalNetworks, optionally by name or Scope.

        Calling this method with no query arguments will return ScopedKeys for
        all AlchemicalNetworks that are within the Scopes this user has access
        to.

        Parameters
        ----------
        name : optional
            Regex expression for the network names. Defaults to a wildcard.
        scope : optional
            A Scope to filter AlchemicalNetworks on.
        state : optional
            Regex expression for the network states. Nonexistent state values
            entered will not raise any warnings. Use ``None`` to get networks
            regardless of state. Defaults to the "active" state.

        Returns
        -------
        List[ScopedKey]
            A list of ScopedKeys for the networks matching the query
            parameters.
        """
        if scope is None:
            scope = Scope()

        if isinstance(state, NetworkStateEnum):
            state = state.value

        params = dict(name=name, **scope.dict(), state=state)

        return self._query_resource("/networks", params=params)

    def query_transformations(
        self,
        name: Optional[str] = None,
        scope: Optional[Scope] = None,
    ) -> List[ScopedKey]:
        """Query for Transformations, optionally by name or Scope.

        Calling this method with no query arguments will return ScopedKeys for
        all Transformations that are within the Scopes this user has access to.

        """
        if scope is None:
            scope = Scope()

        params = dict(name=name, **scope.dict())

        return self._query_resource("/transformations", params=params)

    def query_chemicalsystems(
        self,
        name: Optional[str] = None,
        scope: Optional[Scope] = None,
    ) -> List[ScopedKey]:
        """Query for ChemicalSystems, optionally by name or Scope.

        Calling this method with no query arguments will return ScopedKeys for
        all ChemicalSystems that are within the Scopes this user has access to.

        """
        if scope is None:
            scope = Scope()

        params = dict(name=name, **scope.dict())

        return self._query_resource("/chemicalsystems", params=params)

    def get_network_transformations(self, network: ScopedKey) -> List[ScopedKey]:
        """List ScopedKeys for Transformations associated with the given AlchemicalNetwork."""
        return self._query_resource(f"/networks/{network}/transformations")

    def get_network_weight(self, network: ScopedKey) -> float:
        """Get the weight of the TaskHub associated with the given AlchemicalNetwork.

        Compute services perform a weighted selection of the AlchemicalNetworks
        visible to them before claiming Tasks actioned on those networks.
        Networks with higher weight are more likely to be selected than those
        with lower weight, and so will generally get more compute attention
        over time.

        A weight of ``0`` means the AlchemicalNetwork will not receive any
        compute for its actioned Tasks.

        """
        return self._get_resource(f"/networks/{network}/weight")

    async def _get_network_weight(self, networks: List[ScopedKey]) -> List[float]:
        data = dict(networks=[str(network) for network in networks])
        weights = await self._post_resource_async(
            "/bulk/networks/weight/get", data=data
        )
        return weights

    def get_networks_weight(
        self, networks: List[ScopedKey], batch_size: int = 1000
    ) -> List[float]:
        """Get the weight of the TaskHubs associated with the given AlchemicalNetworks.

        Compute services perform a weighted selection of the AlchemicalNetworks
        visible to them before claiming Tasks actioned on those networks.
        Networks with higher weight are more likely to be selected than those
        with lower weight, and so will generally get more compute attention
        over time.

        A weight of ``0`` means the AlchemicalNetwork will not receive any
        compute for its actioned Tasks.

        Parameters
        ----------
        networks
            A list of AlchemicalNetwork ScopedKeys.

        Returns
        -------
        List[float]
            The weights of the TaskHubs associated with the specified AlchemicalNetworks.
            If the network was not found in the database, then None is returned in the
            corresponding index.
        """
        return self._batched_attribute_getter(
            networks, self._get_network_weight, batch_size
        )

    def set_network_weight(
        self, network: ScopedKey, weight: float
    ) -> Optional[ScopedKey]:
        """Set the weight of the TaskHub associated with the given AlchemicalNetwork.

        Compute services perform a weighted selection of the AlchemicalNetworks
        visible to them before claiming Tasks actioned on those networks.
        Networks with higher weight are more likely to be selected than those
        with lower weight, and so will generally get more compute attention
        over time.

        A weight of ``0`` means the AlchemicalNetwork will not receive any
        compute for its actioned Tasks.

        Parameters
        ----------
        network
            The ScopedKey of the AlchemicalNetwork to set the weight for.
        weight
            The weight to set for the network. This must be between 0 and 1
            (inclusive). Setting the value to 0 will effectively disable
            compute on this network without cancelling its actioned Tasks.
        """
        network_sk = self._post_resource(f"/networks/{network}/weight", weight)
        return ScopedKey.from_str(network_sk) if network_sk else None

    async def _set_network_weight(
        self,
        items: List[Tuple[ScopedKey, float]],
    ) -> List[Optional[ScopedKey]]:

        networks = []
        weights = []
        for item in items:
            networks.append(str(item[0]))
            weights.append(item[1])

        data = dict(networks=networks, weights=weights)
        return await self._post_resource_async("/bulk/networks/weight/set", data=data)

    def set_networks_weight(
        self,
        networks: List[ScopedKey],
        weights: List[float],
        batch_size: int = 1000,
    ) -> List[Optional[ScopedKey]]:
        """Set the weights of the TaskHubs associated with the given
        AlchemicalNetworks.

        Compute services perform a weighted selection of the AlchemicalNetworks
        visible to them before claiming Tasks actioned on those networks.
        Networks with higher weight are more likely to be selected than those
        with lower weight, and so will generally get more compute attention
        over time.

        A weight of ``0`` means the AlchemicalNetwork will not receive any
        compute for its actioned Tasks.

        Parameters
        ----------
        networks
            The ScopedKeys of the AlchemicalNetworks to set the weights for.
        weight
            The weights to set for the `networks`, in the same order. Each must
            be between 0 and 1 (inclusive). Setting the value to 0 will
            effectively disable compute on the networks without cancelling its
            actioned Tasks. Should be a list of the same length as `networks`.

        Returns
        -------
        List[Optional[ScopedKey]]
            The ScopedKeys of the TaskHubs whose weight changed, in the order
            that the AlchemicalNetworks ScopedKeys were provided. If one of
            the specified networks could not be found, a None is returned
            at its corresponding index.
        """
        values = self._batched_attribute_setter(
            [(network, weight) for network, weight in zip(networks, weights)],
            self._set_network_weight,
            batch_size=batch_size,
        )
        return [ScopedKey.from_str(value) if value else None for value in values]

    def get_transformation_networks(self, transformation: ScopedKey) -> List[ScopedKey]:
        """List ScopedKeys for AlchemicalNetworks associated with the given Transformation."""
        return self._query_resource(f"/transformations/{transformation}/networks")

    def get_network_chemicalsystems(self, network: ScopedKey) -> List[ScopedKey]:
        """List ScopedKeys for the ChemicalSystems associated with the given AlchemicalNetwork."""
        return self._query_resource(f"/networks/{network}/chemicalsystems")

    def get_chemicalsystem_networks(self, chemicalsystem: ScopedKey) -> List[ScopedKey]:
        """List ScopedKeys for the AlchemicalNetworks associated with the given ChemicalSystem."""
        return self._query_resource(f"/chemicalsystems/{chemicalsystem}/networks")

    def get_transformation_chemicalsystems(
        self, transformation: ScopedKey
    ) -> List[ScopedKey]:
        """List ScopedKeys for the ChemicalSystems associated with the given Transformation."""
        return self._query_resource(
            f"/transformations/{transformation}/chemicalsystems"
        )

    def get_chemicalsystem_transformations(
        self, chemicalsystem: ScopedKey
    ) -> List[ScopedKey]:
        """List ScopedKeys for the Transformations associated with the given ChemicalSystem."""
        return self._query_resource(
            f"/chemicalsystems/{chemicalsystem}/transformations"
        )

    @lru_cache(maxsize=100)
    def get_network(
        self,
        network: Union[ScopedKey, str],
        compress: bool = True,
        visualize: bool = True,
    ) -> AlchemicalNetwork:
        """Retrieve an AlchemicalNetwork given its ScopedKey.

        Parameters
        ----------
        network
            The ScopedKey of the AlchemicalNetwork to retrieve.
        compress
            If ``True``, compress the AlchemicalNetwork server-side before
            shipping it to the client. This can reduce retrieval time depending
            on the bandwidth of your connection to the API service. Set to
            ``False`` to retrieve without compressing. This is a performance
            optimization; it has no bearing on the result of this method call.
        visualize
            If ``True``, show retrieval progress indicator.

        Returns
        -------
        AlchemicalNetwork
            The retrieved AlchemicalNetwork.

        """

        def _get_network():
            content = self._get_resource(f"/networks/{network}", compress=compress)
            return KeyedChain(content).to_gufe()

        if visualize:
            from rich.progress import Progress

            with Progress(*self._rich_waiting_columns(), transient=False) as progress:
                task = progress.add_task(
                    f"Retrieving [bold]'{network}'[/bold]...", total=None
                )

                an = _get_network()

                progress.start_task(task)
                progress.update(task, total=1, completed=1)
        else:
            an = _get_network()
        return an

    @lru_cache(maxsize=10000)
    def get_transformation(
        self,
        transformation: Union[ScopedKey, str],
        compress: bool = True,
        visualize: bool = True,
    ) -> Transformation:
        """Retrieve a Transformation given its ScopedKey.

        Parameters
        ----------
        transformation
            The ScopedKey of the Transformation to retrieve.
        compress
            If ``True``, compress the Transformation server-side before
            shipping it to the client. This can reduce retrieval time depending
            on the bandwidth of your connection to the API service. Set to
            ``False`` to retrieve without compressing. This is a performance
            optimization; it has no bearing on the result of this method call.
        visualize
            If ``True``, show retrieval progress indicator.

        Returns
        -------
        Transformation
            The retrieved Transformation.

        """

        def _get_transformation():
            content = self._get_resource(
                f"/transformations/{transformation}", compress=compress
            )
            return KeyedChain(content).to_gufe()

        if visualize:
            from rich.progress import Progress

            with Progress(*self._rich_waiting_columns(), transient=False) as progress:
                task = progress.add_task(
                    f"Retrieving [bold]'{transformation}'[/bold]...", total=None
                )

                tf = _get_transformation()
                progress.start_task(task)
                progress.update(task, total=1, completed=1)
        else:
            tf = _get_transformation()

        return tf

    @lru_cache(maxsize=1000)
    def get_chemicalsystem(
        self,
        chemicalsystem: Union[ScopedKey, str],
        compress: bool = True,
        visualize: bool = True,
    ) -> ChemicalSystem:
        """Retrieve a ChemicalSystem given its ScopedKey.

        Parameters
        ----------
        chemicalsystem
            The ScopedKey of the ChemicalSystem to retrieve.
        compress
            If ``True``, compress the ChemicalSystem server-side before
            shipping it to the client. This can reduce retrieval time depending
            on the bandwidth of your connection to the API service. Set to
            ``False`` to retrieve without compressing. This is a performance
            optimization; it has no bearing on the result of this method call.
        visualize
            If ``True``, show retrieval progress indicator.

        Returns
        -------
        ChemicalSystem
            The retrieved ChemicalSystem.

        """

        def _get_chemicalsystem():
            content = self._get_resource(
                f"/chemicalsystems/{chemicalsystem}", compress=compress
            )
            return KeyedChain(content).to_gufe()

        if visualize:
            from rich.progress import Progress

            with Progress(*self._rich_waiting_columns(), transient=False) as progress:
                task = progress.add_task(
                    f"Retrieving [bold]'{chemicalsystem}'[/bold]...", total=None
                )

                cs = _get_chemicalsystem()

                progress.start_task(task)
                progress.update(task, total=1, completed=1)
        else:
            cs = _get_chemicalsystem()

        return cs

    ### compute

    def set_strategy(self, network: ScopedKey, strategy: Strategy):
        """Set the Strategy for evaluating the given AlchemicalNetwork.

        The Strategy will be applied to create and action tasks for the
        Transformations in the AlchemicalNetwork without user interaction.

        """
        raise NotImplementedError

    def create_tasks(
        self,
        transformation: ScopedKey,
        extends: Optional[ScopedKey] = None,
        count: int = 1,
    ) -> List[ScopedKey]:
        """Create Tasks for the given Transformation.

        Parameters
        ----------
        transformation
            The ScopedKey of the Transformation to create Tasks for.
        extends
            The ScopedKey for the Task to use as a starting point for the Tasks created.
        count
            The number of new Tasks to create.

        Returns
        -------
        List[ScopedKey]
            A list giving the ScopedKeys of the new Tasks created.

        """
        if extends:
            extends = extends.dict()

        data = dict(extends=extends, count=count)
        task_sks = self._post_resource(f"/transformations/{transformation}/tasks", data)
        return [ScopedKey.from_str(i) for i in task_sks]

    def create_transformations_tasks(
        self,
        transformations: List[ScopedKey],
        extends: Optional[List[Optional[ScopedKey]]] = None,
    ) -> List[ScopedKey]:
        """Create Tasks for multiple Transformations.

        Unlike `create_tasks`, this method can create Tasks for many
        Transformations. This method should be used instead of `create_tasks`
        whenever creating Tasks for more than one unique Transformation since it
        minimizes the number of API requests to the alchemiscale server.

        Parameters
        ----------
        transformations
            A list of ScopedKeys of Transformations to create Tasks for. The
            same ScopedKey can be repeated to create multiple Tasks for the
            same Transformation.
        extends
            A list of ScopedKeys for the Tasks to be extended. When not `None`,
            `extends` must be a list of the same length as `transformations`. If
            a transformation in `transformations` should not extend a Task, use
            a `None` as a placeholder in the `extends` list.

        Returns
        -------
        List[ScopedKey]
            A list giving the ScopedKeys of the new Tasks created.

        Examples
        --------

        Instead of looping over Transformations and calling `create_tasks`, make
        one call to `create_transformations_tasks`.

        >>> client.create_transformations_tasks([transformation_1_sk, transformation_2_sk])

        The behavior of the `count` keyword argument from `create_tasks` can be
        recreated by repeating the same transformation in the list while also
        allowing the addition of other transformtions.

        >>> client.create_transformations_tasks([transformation_1_sk] * 3 + [transformation_2_sk] * 2)

        """

        data = dict(
            transformations=[str(transformation) for transformation in transformations],
            extends=(
                None
                if not extends
                else [
                    str(task_sk) if task_sk is not None else None for task_sk in extends
                ]
            ),
        )
        task_sks = self._post_resource("/bulk/transformations/tasks/create", data)
        return [ScopedKey.from_str(i) for i in task_sks]

    def query_tasks(
        self,
        scope: Optional[Scope] = None,
        status: Optional[str] = None,
    ) -> List[ScopedKey]:
        """Query for Tasks, optionally by status or Scope.

        Calling this method with no query arguments will return ScopedKeys for
        all Tasks that are within the Scopes this user has access to.

        """
        if scope is None:
            scope = Scope()

        params = dict(status=status, **scope.dict())

        return self._query_resource("/tasks", params=params)

    def get_network_tasks(self, network: ScopedKey, status: Optional[str] = None):
        """List ScopedKeys for all Tasks associated with the given AlchemicalNetwork."""
        params = {"status": status}
        return self._query_resource(f"/networks/{network}/tasks", params=params)

    def get_task_networks(self, task: ScopedKey):
        """List ScopedKeys for all AlchemicalNetworks associated with the given Task."""
        return self._query_resource(f"/tasks/{task}/networks")

    def get_transformation_tasks(
        self,
        transformation: ScopedKey,
        extends: Optional[ScopedKey] = None,
        return_as: str = "list",
        status: Optional[str] = None,
    ) -> Union[List[ScopedKey], nx.DiGraph]:
        """Return the Tasks associated with the given Transformation.

        Parameters
        ----------
        transformation
            The ScopedKey of the Transformation to get Tasks for.
        extends
            If given, only return Tasks that extend from the given Task's ScopedKey.
            This will also give any Tasks that extend from those Tasks, recursively.
            Using this keyword argument amounts to choosing the tree of Tasks that
            extend from the given Task.
        return_as : ['list', 'graph']
            If 'list', Tasks will be returned in no particular order.
            If 'graph', Tasks will be returned in a `networkx.DiGraph`, with a
            directed edge pointing from a given Task to the Task it extends.

        """
        if extends:
            extends = str(extends)

        params = dict(extends=extends, return_as=return_as, status=status)
        task_sks = self._get_resource(
            f"/transformations/{transformation}/tasks", params
        )

        if return_as == "list":
            return [ScopedKey.from_str(i) for i in task_sks]
        elif return_as == "graph":
            g = nx.DiGraph()
            for task, extends in task_sks.items():
                g.add_node(ScopedKey.from_str(task))
                if extends is not None:
                    g.add_edge(ScopedKey.from_str(task), ScopedKey.from_str(extends))

            return g
        else:
            raise ValueError(f"`return_as` takes 'list' or 'graph', not '{return_as}'")

    def get_task_transformation(self, task: ScopedKey) -> ScopedKey:
        """Get the Transformation associated with the given Task."""
        transformation = self._get_resource(f"/tasks/{task}/transformation")
        return ScopedKey.from_str(transformation)

    def _visualize_status(self, status_counts, status_object):
        from rich import print as rprint
        from rich.table import Table

        title = f"{status_object}"
        table = Table(title=title, title_justify="left", expand=True)
        # table = Table()

        table.add_column("status", justify="left", no_wrap=True)
        table.add_column("count", justify="right")

        table.add_row("complete", f"{status_counts.get('complete', 0)}", style="green")
        table.add_row("running", f"{status_counts.get('running', 0)}", style="orange3")
        table.add_row("waiting", f"{status_counts.get('waiting', 0)}", style="#1793d0")
        table.add_row("error", f"{status_counts.get('error', 0)}", style="#ff073a")
        table.add_row("invalid", f"{status_counts.get('invalid', 0)}", style="magenta1")
        table.add_row("deleted", f"{status_counts.get('deleted', 0)}", style="purple")

        rprint(table)

    def get_scope_status(
        self,
        scope: Optional[Scope] = None,
        visualize: Optional[bool] = True,
        network_state: Optional[Union[NetworkStateEnum, str]] = NetworkStateEnum.active,
    ) -> Dict[str, int]:
        """Return status counts for all Tasks within the given Scope.

        Parameters
        ----------
        scope
            Scope to use for querying status. Non-specific Scopes are allowed,
            and will give back counts for all Tasks within that this user has
            Scope access to. Defaults to all Scopes.
        visualize
            If ``True``, print a table of status counts.
        network_state
            Regex expression for the network states. Nonexistent state values
            entered will not raise any warnings. Use ``None`` to get networks
            regardless of state. Defaults to the "active" state.

        Returns
        -------
        status_counts
            Dict giving statuses as keys, Task counts as values.
        """
        if scope is None:
            scope = Scope()

        if isinstance(network_state, NetworkStateEnum):
            network_state = network_state.value

        params = dict(network_state=network_state)

        status_counts = self._get_resource(f"/scopes/{scope}/status", params=params)

        if visualize:
            self._visualize_status(status_counts, scope)

        return status_counts

    def get_network_status(
        self,
        network: ScopedKey,
        visualize: Optional[bool] = True,
    ) -> Dict[str, int]:
        """Return status counts for all Tasks associated with the given AlchemicalNetwork.

        Parameters
        ----------
        network
            ScopedKey for the AlchemicalNetwork to obtain status counts for.
        visualize
            If ``True``, print a table of status counts.

        Returns
        -------
        status_counts
            Dict giving statuses as keys, Task counts as values.
        """
        status_counts = self._get_resource(f"/networks/{network}/status")

        if visualize:
            self._visualize_status(status_counts, network)

        return status_counts

    def get_networks_status(
        self,
        networks: List[ScopedKey],
    ) -> List[Dict[str, int]]:
        """Get the status counts of Tasks for a list of AlchemicalNetworks.

        Parameters
        ----------
        networks
            List of AlchemicalNetwork ScopedKeys to obtain status counts for.

        Returns
        -------
        List[Dict[str, int]]
            A list of dictionaries, in the same order as the provided networks,
            containing the Task status counts for all Tasks in each network.
            The dictionary keys are the statuses and the values are the number
            of Tasks with that status. If either no tasks exist for the
            Transformations in a network, or the network does not exist in the
            database, a empty dictionary is returned at the corresponding index.
        """
        data = {"networks": [str(network) for network in networks]}
        status_counts = self._post_resource("/bulk/networks/status", data=data)
        return status_counts

    def get_transformation_status(
        self,
        transformation: ScopedKey,
        visualize: Optional[bool] = True,
    ) -> Dict[str, int]:
        """Return status counts for all Tasks associated with the given
        Transformation.

        Parameters
        ----------
        transformation
            ScopedKey for the Transformation to obtain status counts for.
        visualize
            If ``True``, print a table of status counts.

        Returns
        -------
        status_counts
            Dict giving statuses as keys, Task counts as values.
        """
        status_counts = self._get_resource(f"/transformations/{transformation}/status")

        if visualize:
            self._visualize_status(status_counts, transformation)

        return status_counts

    def get_network_actioned_tasks(
        self,
        network: ScopedKey,
        task_weights: bool = False,
    ) -> Union[Dict[ScopedKey, float], List[ScopedKey]]:
        """Return all actioned Tasks for a given AlchemicalNetwork.

        Parameters
        ----------
        network
            The ScopedKey for the AlchemicalNetwork to get actioned Tasks for.
        task_weights
            If ``True``, return a dict with Task ScopedKeys as keys, Task
            weights on the AlchemicalNetwork as values.

        Returns
        -------
        tasks
            A list of Task ScopedKeys actioned on the given AlchemicalNetwork.
            If ``task_weights`` is ``True``, a dict is returned with Task
            ScopedKeys as keys, Task weights as values.
        """
        data = dict(task_weights=task_weights)
        tasks = self._post_resource(f"/networks/{network}/tasks/actioned", data)

        if task_weights:
            return {ScopedKey.from_str(t): w for t, w in tasks.items()}

        return [ScopedKey.from_str(t) for t in tasks]

    def get_networks_actioned_tasks(
        self,
        networks: List[ScopedKey],
        task_weights: bool = False,
    ) -> List[Union[Dict[ScopedKey, float], List[ScopedKey]]]:
        """Get all actioned Tasks for a list of AlchemicalNetwork ScopedKeys.

        Parameters
        ----------
        networks
            A list of AlchemicalNetwork ScopedKeys to retrieve actioned
            Tasks for.

        Returns
        -------
        List[Union[Dict[ScopedKey, float], List[ScopedKey]]]
            If task_weights is True, a list of dictionaries is returned with
            the same length as the specified network list. The keys and values
            of the contained dictionaries are the ScopedKeys and weights of
            the actioned Tasks, respectively.

            If task_weights is False, only a list of actioned Task ScopedKeys
            is returned for each network in the specified list.
        """
        data = dict(
            networks=[str(network) for network in networks], task_weights=task_weights
        )
        grouped_tasks = self._post_resource("/bulk/networks/tasks/actioned", data=data)

        return_data = []
        for tasks in grouped_tasks:
            if task_weights:
                return_data.append({ScopedKey.from_str(t): w for t, w in tasks.items()})
            else:
                return_data.append([ScopedKey.from_str(t) for t in tasks])

        return return_data

    def get_task_actioned_networks(
        self, task: ScopedKey, task_weights: bool = False
    ) -> Union[Dict[ScopedKey, float], List[ScopedKey]]:
        """Return all AlchemicalNetworks the given Task is actioned on.

        Parameters
        ----------
        task
            The ScopedKey for the Task to get actioned AlchemicalNetworks for.
        task_weights
            If ``True``, return a dict with AlchemicalNetwork ScopedKeys as
            keys, the given Task's weights on each AlchemicalNetwork as values.

        Returns
        -------
        networks
            A list of AlchemicalNetwork ScopedKeys which action the given Task.
            If task_weights is ``True``, a dict is returned with
            AlchemicalNetwork ScopedKeys as keys, Task weights as values.

        """
        data = dict(task_weights=task_weights)
        networks = self._post_resource(f"/tasks/{task}/networks/actioned", data)

        if task_weights:
            return {ScopedKey.from_str(n): weight for n, weight in networks.items()}

        return [ScopedKey.from_str(n) for n in networks]

    def action_tasks(
        self,
        tasks: List[ScopedKey],
        network: ScopedKey,
        weight: Optional[Union[float, List[float]]] = None,
    ) -> List[Optional[ScopedKey]]:
        """Action Tasks for execution via the given AlchemicalNetwork's
        TaskHub.

        A Task cannot be actioned:
            - to an AlchemicalNetwork in a different Scope
            - if it has any status other than 'waiting', 'running', or 'error'

        Parameters
        ----------
        tasks
            Task ScopedKeys to action for execution.
        network
            The AlchemicalNetwork ScopedKey to action the Tasks for.
            The Tasks will be added to the network's associated TaskHub.
        weight
            Weight to be applied to the actioned Tasks. Only values between 0
            and 1 are valid weights. Weights can also be provided as a list of
            floats with the same length as `tasks`.

            Setting `weight` to ``None`` will apply the default weight of 0.5
            to newly actioned Tasks, while leaving the weights of any previously
            actioned Tasks unchanged. Setting `weight` to anything other than
            ``None`` will change the weights of previously actioned Tasks
            included in `tasks`.

        Returns
        -------
        List[Optional[ScopedKey]]
            ScopedKeys for Tasks actioned, in the same order as given as
            `tasks` on input. If a Task couldn't be actioned, then ``None``
            will be returned in its place.

        """
        data = dict(tasks=[t.dict() for t in tasks], weight=weight)
        actioned_sks = self._post_resource(f"/networks/{network}/tasks/action", data)

        return [ScopedKey.from_str(i) if i is not None else None for i in actioned_sks]

    def cancel_tasks(
        self, tasks: List[ScopedKey], network: ScopedKey
    ) -> List[ScopedKey]:
        """Cancel Tasks for execution via the given AlchemicalNetwork's
        TaskHub.

        A Task cannot be canceled:
            - if it is not present in the AlchemicalNetwork's TaskHub.

        Parameters
        ----------
        tasks
            Task ScopedKeys to cancel for execution.
        network
            The AlchemicalNetwork ScopedKey to cancel the Tasks for.
            The Tasks will be removed from the network's associated TaskHub.

        Returns
        -------
        List[Optional[ScopedKey]]
            ScopedKeys for Tasks canceled, in the same order as given as
            `tasks` on input. If a Task couldn't be canceled, then ``None`` will
            be returned in its place.

        """
        data = dict(tasks=[t.dict() for t in tasks])
        canceled_sks = self._post_resource(f"/networks/{network}/tasks/cancel", data)

        return [ScopedKey.from_str(i) if i is not None else None for i in canceled_sks]

    def _batched_attribute_getter(
        self,
        batchables: List[Any],
        getter_function,
        batch_size,
    ) -> List[Any]:

        @use_session
        async def async_request(self):
            values = await asyncio.gather(
                *[
                    getter_function(batch)
                    for batch in self._batched(batchables, batch_size)
                ]
            )

            return list(chain.from_iterable(values))

        coro = async_request(self)

        try:
            return asyncio.run(coro)
        except RuntimeError:
            # we use nest_asyncio to support environments where an event loop
            # is already running, such as in a Jupyter notebook
            import nest_asyncio

            nest_asyncio.apply()
            return asyncio.run(coro)

    def _batched_attribute_setter(
        self,
        batchables: List[Any],
        setter_function,
        setter_args: Iterable[Any] = None,
        batch_size: int = 1000,
        should_return=True,
    ) -> List[Optional[ScopedKey]]:

        if setter_args is None:
            setter_args = []

        @use_session
        async def async_request(self):
            scoped_keys = await asyncio.gather(
                *[
                    setter_function(batch, *setter_args)
                    for batch in self._batched(batchables, batch_size)
                ]
            )

            if not should_return:
                return None

            return list(chain.from_iterable(scoped_keys))

        coro = async_request(self)

        try:
            return asyncio.run(coro)
        except RuntimeError:
            # we use nest_asyncio to support environments where an event loop
            # is already running, such as in a Jupyter notebook
            import nest_asyncio

            nest_asyncio.apply()
            return asyncio.run(coro)

    async def _set_task_status(
        self, tasks: List[ScopedKey], status: TaskStatusEnum
    ) -> List[Optional[ScopedKey]]:
        """Set the statuses for many Tasks"""
        data = dict(tasks=[t.dict() for t in tasks], status=status.value)
        tasks_updated = await self._post_resource_async(
            "/bulk/tasks/status/set", data=data
        )
        return [
            ScopedKey.from_str(task_sk) if task_sk is not None else None
            for task_sk in tasks_updated
        ]

    def set_tasks_status(
        self,
        tasks: List[ScopedKey],
        status: Union[TaskStatusEnum, str],
        batch_size: int = 1000,
    ) -> List[Optional[ScopedKey]]:
        """Set the status of one or multiple Tasks.

        Task status can be set to 'waiting' if currently 'error'.
        Status can be set to 'invalid' or 'deleted' from any other status.

        Parameters
        ----------
        tasks
            The Tasks to set the status of.
        status
            The status to set the Tasks to. Can be one of
            'waiting', 'invalid', or 'deleted'.

        Returns
        -------
        updated
            The ScopedKeys of the Tasks that were updated, in the same order
            as given in `tasks`. If a given Task doesn't exist, ``None`` will
            be returned in its place.

        """
        status = TaskStatusEnum(status)

        return self._batched_attribute_setter(
            tasks, self._set_task_status, (status,), batch_size
        )

    async def _get_task_status(self, tasks: List[ScopedKey]) -> List[TaskStatusEnum]:
        """Get the statuses for many Tasks"""
        data = dict(tasks=[t.dict() for t in tasks])
        statuses = await self._post_resource_async("/bulk/tasks/status/get", data=data)
        return statuses

    def get_tasks_status(
        self, tasks: List[ScopedKey], batch_size: int = 1000
    ) -> List[str]:
        """Get the status of multiple Tasks.

        Parameters
        ----------
        tasks
            The Tasks to get the status of.
        batch_size
            The number of Tasks to include in a single request; use to tune
            method call speed when requesting many statuses at once.

        Returns
        -------
        statuses
            The status of each Task in the same order as given in `tasks`. If a
            given Task doesn't exist, ``None`` will be returned in its place.

        """
        return self._batched_attribute_getter(tasks, self._get_task_status, batch_size)

    async def _set_task_priority(
        self, tasks: List[ScopedKey], priority: int
    ) -> List[Optional[ScopedKey]]:
        data = dict(tasks=[t.dict() for t in tasks], priority=priority)
        tasks_updated = await self._post_resource_async(
            "/bulk/tasks/priority/set", data=data
        )
        return [
            ScopedKey.from_str(task_sk) if task_sk is not None else None
            for task_sk in tasks_updated
        ]

    def set_tasks_priority(
        self,
        tasks: List[ScopedKey],
        priority: int,
        batch_size: int = 1000,
    ) -> List[Optional[ScopedKey]]:
        """Set the priority of multiple Tasks.

        Parameters
        ----------
        tasks
            The Tasks to set the priority of.
        priority
            The priority to set for the Task. This value must be between 1 and
            2**63 - 1, with lower values indicating an increased priority.
        batch_size
            The number of Tasks to include in a single request; use to tune
            method call speed when requesting many priorities at once.

        Returns
        -------
        updated
            The ScopedKeys of the Tasks that were updated, in the same order
            as given in `tasks`. If a given Task doesn't exist, ``None`` will
            be returned in its place.
        """
        return self._batched_attribute_setter(
            tasks, self._set_task_priority, (priority,), batch_size
        )

    async def _get_task_priority(self, tasks: List[ScopedKey]) -> List[int]:
        """Get the priority for many Tasks"""
        data = dict(tasks=[t.dict() for t in tasks])
        priorities = await self._post_resource_async(
            "/bulk/tasks/priority/get", data=data
        )
        return priorities

    def get_tasks_priority(
        self,
        tasks: List[ScopedKey],
        batch_size: int = 1000,
    ) -> List[int]:
        """Get the priority of multiple Tasks.

        Parameters
        ----------
        tasks
            The Tasks to get the priority of.
        batch_size
            The number of Tasks to include in a single request; use to tune
            method call speed when requesting many priorities at once.

        Returns
        -------
        priorities
            The priority of each Task in the same order as given in `tasks`. If a
            given Task doesn't exist, ``None`` will be returned in its place.

        """
        return self._batched_attribute_getter(
            tasks, self._get_task_priority, batch_size
        )

    ### results

    @alru_cache(maxsize=10000)
    async def _async_get_protocoldagresult(
        self, protocoldagresultref, transformation, route, compress
    ):
        pdr_json = await self._get_resource_async(
            f"/transformations/{transformation}/{route}/{protocoldagresultref}",
            compress=compress,
        )

        pdr = GufeTokenizable.from_dict(
            json.loads(pdr_json[0], cls=JSON_HANDLER.decoder)
        )

        return pdr

    def _get_protocoldagresults(
        self,
        protocoldagresultrefs: List[ScopedKey],
        transformation: ScopedKey,
        ok: bool,
        compress: bool = True,
        visualize: bool = True,
    ):
        if ok:
            route = "results"
        else:
            route = "failures"

        @use_session
        async def async_request(self):
            if visualize:
                from rich.progress import Progress

                with Progress(
                    *self._rich_progress_columns(), transient=False
                ) as progress:
                    task = progress.add_task(
                        "Retrieving [bold]ProtocolDAGResult[/bold]s",
                        total=len(protocoldagresultrefs),
                    )

                    coros = [
                        self._async_get_protocoldagresult(
                            protocoldagresultref,
                            transformation,
                            route,
                            compress,
                        )
                        for protocoldagresultref in protocoldagresultrefs
                    ]
                    pdrs = []
                    for coro in asyncio.as_completed(coros):
                        pdr = await coro
                        pdrs.append(pdr)
                        progress.update(task, advance=1)
                    progress.refresh()
            else:
                coros = [
                    self._async_get_protocoldagresult(
                        protocoldagresultref,
                        transformation,
                        route,
                        compress,
                    )
                    for protocoldagresultref in protocoldagresultrefs
                ]
                pdrs = await asyncio.gather(*coros)

            return pdrs

        coro = async_request(self)

        try:
            return asyncio.run(coro)
        except RuntimeError:
            # we use nest_asyncio to support environments where an event loop
            # is already running, such as in a Jupyter notebook
            import nest_asyncio

            nest_asyncio.apply()
            return asyncio.run(coro)

    def _get_network_results(
        self,
        network: ScopedKey,
        ok: bool = True,
        return_protocoldagresults: bool = False,
        compress: bool = True,
        visualize: bool = True,
    ) -> Dict[str, Union[Optional[ProtocolResult], List[ProtocolDAGResult]]]:
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor, as_completed

        ctx = mp.get_context("spawn")

        if ok:
            kwargs = dict(
                return_protocoldagresults=return_protocoldagresults,
                compress=compress,
                visualize=False,
            )
            route = "results"
        else:
            kwargs = dict(compress=compress, visualize=False)
            route = "failures"

        with ProcessPoolExecutor(mp_context=ctx) as executor:
            futures = []
            tf_sks = self.get_network_transformations(network)
            for tf_sk in tf_sks:
                futures.append(
                    executor.submit(
                        _get_transformation_results,
                        self._settings(),
                        tf_sk,
                        ok,
                        kwargs,
                    )
                )

            results = {}
            if visualize:
                from rich.progress import Progress

                with Progress(
                    *self._rich_progress_columns(), transient=False
                ) as progress:
                    task = progress.add_task(
                        f"Retrieving [bold]Transformation[/bold] {route}",
                        total=len(tf_sks),
                    )

                    for future in as_completed(futures):
                        tf_sk, result = future.result()
                        results[tf_sk] = result
                        progress.update(task, advance=1)
                    progress.refresh()
            else:
                for future in as_completed(futures):
                    tf_sk, result = future.result()
                    results[tf_sk] = result

        return results

    def get_network_results(
        self,
        network: ScopedKey,
        return_protocoldagresults: bool = False,
        compress: bool = True,
        visualize: bool = True,
    ) -> Dict[str, Union[Optional[ProtocolResult], List[ProtocolDAGResult]]]:
        """Get a `ProtocolResult` for every `Transformation` in the given
        `AlchemicalNetwork`.

        A dict giving the `ScopedKey` of each `Transformation` in the network
        as keys, `ProtocolResult` as values, is returned. If no
        `ProtocolDAGResult`\s exist for a given `Transformation`, ``None`` is
        given for its value.

        If `return_protocoldagresults` is ``True``, then a list of the
        `ProtocolDAGResult`\s themselves is given as values instead of
        `ProtocolResult`\s.

        Parameters
        ----------
        network
            The `ScopedKey` of the `AlchemicalNetwork` to retrieve results for.
        return_protocoldagresults
            If ``True``, return the raw `ProtocolDAGResult`s instead of returning
            a processed `ProtocolResult`. Only successful `ProtocolDAGResult`\s
            are returned.
        compress
            If ``True``, compress the ProtocolDAGResults server-side before
            shipping them to the client. This can reduce retrieval time depending
            on the bandwidth of your connection to the API service. Set to
            ``False`` to retrieve without compressing. This is a performance
            optimization; it has no bearing on the result of this method call.
        visualize
            If ``True``, show retrieval progress indicators.

        """
        return self._get_network_results(
            network=network,
            ok=True,
            return_protocoldagresults=return_protocoldagresults,
            compress=compress,
            visualize=visualize,
        )

    def get_network_failures(
        self,
        network: ScopedKey,
        compress: bool = True,
        visualize: bool = True,
    ) -> Dict[str, List[ProtocolDAGResult]]:
        """Get all failed `ProtocolDAGResult`s for every `Transformation` in
        the given `AlchemicalNetwork`.

        A dict giving the `ScopedKey` of each `Transformation` in the network
        as keys, a list of the `ProtocolDAGResult`\s as values, is returned.

        Parameters
        ----------
        network
            The `ScopedKey` of the `AlchemicalNetwork` to retrieve results for.
        compress
            If ``True``, compress the ProtocolDAGResults server-side before
            shipping them to the client. This can reduce retrieval time depending
            on the bandwidth of your connection to the API service. Set to
            ``False`` to retrieve without compressing. This is a performance
            optimization; it has no bearing on the result of this method call.
        visualize
            If ``True``, show retrieval progress indicators.

        """
        return self._get_network_results(
            network=network, ok=False, compress=compress, visualize=visualize
        )

    def get_transformation_results(
        self,
        transformation: ScopedKey,
        return_protocoldagresults: bool = False,
        compress: bool = True,
        visualize: bool = True,
    ) -> Union[Optional[ProtocolResult], List[ProtocolDAGResult]]:
        """Get a `ProtocolResult` for the given `Transformation`.

        A `ProtocolResult` object corresponds to the `Protocol` used for this
        `Transformation`. This is constructed from the available
        `ProtocolDAGResult`\s for this `Transformation` via
        `Transformation.gather`. If no `ProtocolDAGResult`\s exist for this
        `Transformation`, ``None`` is returned.

        If `return_protocoldagresults` is ``True``, then a list of the
        `ProtocolDAGResult`\s themselves is returned instead.

        Parameters
        ----------
        transformation
            The `ScopedKey` of the `Transformation` to retrieve results for.
        return_protocoldagresults
            If ``True``, return the raw `ProtocolDAGResult`s instead of returning
            a processed `ProtocolResult`. Only successful `ProtocolDAGResult`\s
            are returned.
        compress
            If ``True``, compress the ProtocolDAGResults server-side before
            shipping them to the client. This can reduce retrieval time depending
            on the bandwidth of your connection to the API service. Set to
            ``False`` to retrieve without compressing. This is a performance
            optimization; it has no bearing on the result of this method call.
        visualize
            If ``True``, show retrieval progress indicators.


        """

        if not return_protocoldagresults:
            # get the transformation if we intend to return a ProtocolResult
            tf: Transformation = self.get_transformation(
                transformation, visualize=visualize
            )

        # get all protocoldagresultrefs for the given transformation
        protocoldagresultrefs = self._get_resource(
            f"/transformations/{transformation}/results",
        )

        pdrs = self._get_protocoldagresults(
            protocoldagresultrefs,
            transformation,
            ok=True,
            compress=compress,
            visualize=visualize,
        )

        if return_protocoldagresults:
            return pdrs
        else:
            if len(pdrs) != 0:
                return tf.gather(pdrs)
            else:
                return None

    def get_transformation_failures(
        self, transformation: ScopedKey, compress: bool = True, visualize: bool = True
    ) -> List[ProtocolDAGResult]:
        """Get failed `ProtocolDAGResult`\s for the given `Transformation`.

        Parameters
        ----------
        transformation
            The `ScopedKey` of the `Transformation` to retrieve failures for.
        compress
            If ``True``, compress the ProtocolDAGResults server-side before
            shipping them to the client. This can reduce retrieval time depending
            on the bandwidth of your connection to the API service. Set to
            ``False`` to retrieve without compressing. This is a performance
            optimization; it has no bearing on the result of this method call.
        visualize
            If ``True``, show retrieval progress indicators.

        """
        # get all protocoldagresultrefs for the given transformation
        protocoldagresultrefs = self._get_resource(
            f"/transformations/{transformation}/failures",
        )

        pdrs = self._get_protocoldagresults(
            protocoldagresultrefs,
            transformation,
            ok=False,
            compress=compress,
            visualize=visualize,
        )

        return pdrs

    def get_task_results(
        self, task: ScopedKey, compress: bool = True, visualize: bool = True
    ) -> List[ProtocolDAGResult]:
        """Get successful `ProtocolDAGResult`s for the given `Task`.

        Parameters
        ----------
        task
            The `ScopedKey` of the `Task` to retrieve results for.
        compress
            If ``True``, compress the ProtocolDAGResults server-side before
            shipping them to the client. This can reduce retrieval time depending
            on the bandwidth of your connection to the API service. Set to
            ``False`` to retrieve without compressing. This is a performance
            optimization; it has no bearing on the result of this method call.
        visualize
            If ``True``, show retrieval progress indicators.

        """
        # first, get the transformation; also confirms it exists
        transformation: ScopedKey = self.get_task_transformation(task)

        # get all protocoldagresultrefs for the given transformation
        protocoldagresultrefs = self._get_resource(
            f"/tasks/{task}/results",
        )

        pdrs = self._get_protocoldagresults(
            protocoldagresultrefs,
            transformation,
            ok=True,
            compress=compress,
            visualize=visualize,
        )

        return pdrs

    def get_task_failures(
        self, task: ScopedKey, compress: bool = True, visualize: bool = True
    ) -> List[ProtocolDAGResult]:
        """Get failed `ProtocolDAGResult`s for the given `Task`.

        Parameters
        ----------
        task
            The `ScopedKey` of the `Task` to retrieve failures for.
        compress
            If ``True``, compress the ProtocolDAGResults server-side before
            shipping them to the client. This can reduce retrieval time depending
            on the bandwidth of your connection to the API service. Set to
            ``False`` to retrieve without compressing. This is a performance
            optimization; it has no bearing on the result of this method call.
        visualize
            If ``True``, show retrieval progress indicators.

        """
        # first, get the transformation; also confirms it exists
        transformation: ScopedKey = self.get_task_transformation(task)

        # get all protocoldagresultrefs for the given transformation
        protocoldagresultrefs = self._get_resource(
            f"/tasks/{task}/failures",
        )

        pdrs = self._get_protocoldagresults(
            protocoldagresultrefs,
            transformation,
            ok=False,
            compress=compress,
            visualize=visualize,
        )

        return pdrs

    def add_task_restart_patterns(
        self,
        network_scoped_key: ScopedKey,
        patterns: list[str],
        num_allowed_restarts: int,
    ) -> ScopedKey:
        data = {"patterns": patterns, "number_of_retries": num_allowed_restarts}
        self._post_resource(f"/networks/{network_scoped_key}/restartpolicy/add", data)
        return network_scoped_key

    def get_task_restart_patterns(
        self, network_scoped_key: ScopedKey
    ) -> dict[str, int]:
        data = {"networks": [str(network_scoped_key)]}
        mapped_patterns = self._post_resource(
            "/bulk/networks/restartpolicy/get", data=data
        )
        network_patterns = mapped_patterns[str(network_scoped_key)]
        patterns_with_retries = {pattern: retry for pattern, retry in network_patterns}
        return patterns_with_retries

    def set_task_restart_patterns_allowed_restarts(
        self,
        network_scoped_key: ScopedKey,
        patterns: list[str],
        num_allowed_restarts: int,
    ):
        data = {"patterns": patterns, "max_retries": num_allowed_restarts}
        self._post_resource(
            f"/networks/{network_scoped_key}/restartpolicy/maxretries", data
        )

    def remove_task_restart_patterns(
        self, network_scoped_key: ScopedKey, patterns: list[str]
    ):
        data = {"patterns": patterns}
        self._post_resource(
            f"/networks/{network_scoped_key}/restartpolicy/remove", data
        )

    def clear_task_restart_patterns(self, network_scoped_key: ScopedKey):
        self._query_resource(f"/networks/{network_scoped_key}/restartpolicy/clear")
