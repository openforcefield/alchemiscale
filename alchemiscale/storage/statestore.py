"""
:mod:`alchemiscale.storage.statestore` --- state store interface
================================================================

"""

import abc
from datetime import datetime
from contextlib import contextmanager
import json
import re
from functools import lru_cache, update_wrapper
from typing import Dict, List, Optional, Union, Tuple, Set
from collections import defaultdict
from collections.abc import Iterable
import weakref
import numpy as np

import networkx as nx
from gufe import (
    AlchemicalNetwork,
    Transformation,
    NonTransformation,
    Settings,
    Protocol,
)
from gufe.tokenization import GufeTokenizable, GufeKey, JSON_HANDLER
from gufe.protocols import ProtocolUnitFailure

from neo4j import Transaction, GraphDatabase, Driver

from .models import (
    ComputeServiceID,
    ComputeServiceRegistration,
    NetworkMark,
    NetworkStateEnum,
    ProtocolDAGResultRef,
    Task,
    TaskHub,
    TaskRestartPattern,
    TaskStatusEnum,
    Tracebacks,
)
from ..strategies import Strategy
from ..models import Scope, ScopedKey
from .cypher import cypher_list_from_scoped_keys, cypher_or

from ..security.models import CredentialedEntity
from ..settings import Neo4jStoreSettings
from ..validators import validate_network_nonself

from .subgraph import (
    Node,
    Relationship,
    Subgraph,
    create_subgraph,
    merge_subgraph,
    record_data_to_node,
    subgraph_from_path_record,
)


@lru_cache()
def get_n4js(settings: Neo4jStoreSettings):
    """Convenience function for getting a Neo4jStore directly from settings."""

    graph = GraphDatabase.driver(
        settings.NEO4J_URL, auth=(settings.NEO4J_USER, settings.NEO4J_PASS)
    )
    return Neo4jStore(graph, db_name=settings.NEO4J_DBNAME)


class Neo4JStoreError(Exception): ...


class AlchemiscaleStateStore(abc.ABC): ...


def _select_tasks_from_taskpool(taskpool: List[Tuple[str, float]], count) -> List[str]:
    """Select Tasks from a pool of tasks according to the following scheme:

    1. Randomly select N=`count` tasks from the TaskPool based on weighting
    2. Return the string representation of the Task ScopedKeys.

    Parameters
    ----------
    taskpool: List[Tuple[str, float]]
        A list of tuples containing Tasks (string represtnation of their ScopedKeys) of
        equal priority with the weights of their ACTIONS relationships.

    Returns
    -------
    sk: List[str]
        The string representations of the ScopedKeys of the Tasks selected from the taskpool.
    """
    weights = []
    tasks = []
    for t, w in taskpool:
        tasks.append(t)
        weights.append(w)

    weights = np.array(weights)
    prob = weights / weights.sum()

    return list(np.random.choice(tasks, count, replace=False, p=prob))


def _generate_claim_query(
    task_sks: List[ScopedKey], compute_service_id: ComputeServiceID
) -> str:
    """Generate a query to claim a list of Tasks.

    Parameters
    ----------
    task_sks
        A list of ScopedKeys of Tasks to claim.
    compute_service_id
        ComputeServiceID of the claiming service.

    Returns
    -------
    query: str
        The Cypher query to claim the Task.
    """

    task_data = cypher_list_from_scoped_keys(task_sks)

    query = f"""
    // only match the task if it doesn't have an existing CLAIMS relationship
    UNWIND {task_data} AS task_sk
    MATCH (t:Task {{_scoped_key: task_sk}})
    WHERE NOT (t)<-[:CLAIMS]-(:ComputeServiceRegistration)

    WITH t

    // create CLAIMS relationship with given compute service
    MATCH (csreg:ComputeServiceRegistration {{identifier: '{compute_service_id}'}})
    CREATE (t)<-[cl:CLAIMS {{claimed: localdatetime('{datetime.utcnow().isoformat()}')}}]-(csreg)

    SET t.status = '{TaskStatusEnum.running.value}'

    RETURN t
    """
    return query


class Neo4jStore(AlchemiscaleStateStore):
    # uniqueness constraints applied to the database; key is node label,
    # 'property' is the property on which uniqueness is guaranteed for nodes
    # with that label
    constraints = {
        "GufeTokenizable": {"name": "scoped_key", "property": "_scoped_key"},
        "CredentialedUserIdentity": {
            "name": "user_identifier",
            "property": "identifier",
        },
        "CredentialedComputeIdentity": {
            "name": "compute_identifier",
            "property": "identifier",
        },
        "ComputeServiceRegistration": {
            "name": "compute_service_registration_identifier",
            "property": "identifier",
        },
    }

    def __init__(self, graph: Driver, db_name: str = "neo4j"):
        self.graph: Driver = graph
        self.db_name = db_name
        self.gufe_nodes = weakref.WeakValueDictionary()

    @contextmanager
    def transaction(self, ignore_exceptions=False) -> Transaction:
        """Context manager for a Neo4j Transaction."""
        with self.graph.session(database=self.db_name) as session:
            tx = session.begin_transaction()
            try:
                yield tx
            except:
                tx.rollback()
                if not ignore_exceptions:
                    raise

            else:
                tx.commit()

    def chainable(func):
        def inner(self, *args, **kwargs):
            if kwargs.get("tx") is not None:
                return func(self, *args, **kwargs)

            with self.transaction() as tx:
                kwargs.update(tx=tx)
                return func(self, *args, **kwargs)

        update_wrapper(inner, func)

        return inner

    def execute_query(self, *args, **kwargs):
        kwargs.update({"database_": self.db_name})
        return self.graph.execute_query(*args, **kwargs)

    def initialize(self):
        """Initialize database.

        Ensures that constraints and any other required structures are in place.
        Should be used on any Neo4j database prior to use for Alchemiscale.

        """
        for label, values in self.constraints.items():
            self.execute_query(
                f"""
                CREATE CONSTRAINT {values['name']} IF NOT EXISTS
                FOR (n:{label}) REQUIRE n.{values['property']} is unique
            """
            )

    def check(self):
        """Check consistency of database.

        Will raise `Neo4JStoreError` if any state check fails.
        If no check fails, will return without any exception.

        """
        constraints = {
            rec["name"]: rec for rec in self.execute_query("show constraints").records
        }

        if len(constraints) != len(self.constraints):
            raise Neo4JStoreError(
                f"Number of constraints in database is {len(constraints)}; expected {len(self.constraints)}"
            )

        for label, values in self.constraints.items():
            constraint = constraints[values["name"]]
            if not (
                constraint["labelsOrTypes"] == [label]
                and constraint["properties"] == [values["property"]]
            ):
                raise Neo4JStoreError(
                    f"Constraint {constraint['name']} does not have expected form"
                )

    def _store_check(self):
        """Check that the database is in a state that can be used by the API."""
        try:
            # just list available functions to see if database is working
            self.execute_query("SHOW FUNCTIONS YIELD *")
        except Exception:
            return False
        return True

    def reset(self):
        """Remove all data from database; undo all components in `initialize`."""
        self.execute_query("MATCH (n) DETACH DELETE n")

        for label, values in self.constraints.items():
            self.execute_query(
                f"""
                DROP CONSTRAINT {values['name']} IF EXISTS
            """
            )

    ## gufe object handling

    def _gufe_to_subgraph(
        self, sdct: Dict, labels: List[str], gufe_key: GufeKey, scope: Scope
    ) -> Tuple[Subgraph, Node, str]:
        subgraph = Subgraph()
        node = Node(*labels)

        # used to keep track of which properties we json-encoded so we can
        # apply decoding efficiently
        node["_json_props"] = []
        node["_gufe_key"] = str(gufe_key)
        node.update(
            {"_org": scope.org, "_campaign": scope.campaign, "_project": scope.project}
        )

        scoped_key = ScopedKey(gufe_key=node["_gufe_key"], **scope.dict())
        node["_scoped_key"] = str(scoped_key)

        for key, value in sdct.items():
            if isinstance(value, dict):
                if all([isinstance(x, GufeTokenizable) for x in value.values()]):
                    for k, v in value.items():
                        node_ = subgraph_ = self.gufe_nodes.get(
                            (v.key, scope.org, scope.campaign, scope.project)
                        )
                        if node_ is None:
                            subgraph_, node_, scoped_key_ = self._gufe_to_subgraph(
                                v.to_shallow_dict(),
                                labels=["GufeTokenizable", v.__class__.__name__],
                                gufe_key=v.key,
                                scope=scope,
                            )
                            self.gufe_nodes[
                                (str(v.key), scope.org, scope.campaign, scope.project)
                            ] = node_
                        subgraph = (
                            subgraph
                            | Relationship.type("DEPENDS_ON")(
                                node,
                                node_,
                                attribute=key,
                                key=k,
                                _org=scope.org,
                                _campaign=scope.campaign,
                                _project=scope.project,
                            )
                            | subgraph_
                        )
                else:
                    node[key] = json.dumps(value, cls=JSON_HANDLER.encoder)
                    node["_json_props"].append(key)
            elif isinstance(value, list):
                # lists can only be made of a single, primitive data type
                # we encode these as strings with a special starting indicator
                if isinstance(value[0], (int, float, str)) and all(
                    [isinstance(x, type(value[0])) for x in value]
                ):
                    node[key] = value
                elif all([isinstance(x, GufeTokenizable) for x in value]):
                    for i, x in enumerate(value):
                        node_ = subgraph_ = self.gufe_nodes.get(
                            (x.key, scope.org, scope.campaign, scope.project)
                        )
                        if node_ is None:
                            subgraph_, node_, scoped_key_ = self._gufe_to_subgraph(
                                x.to_shallow_dict(),
                                labels=["GufeTokenizable", x.__class__.__name__],
                                gufe_key=x.key,
                                scope=scope,
                            )
                            self.gufe_nodes[
                                (x.key, scope.org, scope.campaign, scope.project)
                            ] = node_
                        subgraph = (
                            subgraph
                            | Relationship.type("DEPENDS_ON")(
                                node,
                                node_,
                                attribute=key,
                                index=i,
                                _org=scope.org,
                                _campaign=scope.campaign,
                                _project=scope.project,
                            )
                            | subgraph_
                        )
                else:
                    node[key] = json.dumps(value, cls=JSON_HANDLER.encoder)
                    node["_json_props"].append(key)
            elif isinstance(value, tuple):
                # lists can only be made of a single, primitive data type
                # we encode these as strings with a special starting indicator
                if not (
                    isinstance(value[0], (int, float, str))
                    and all([isinstance(x, type(value[0])) for x in value])
                ):
                    node[key] = json.dumps(value, cls=JSON_HANDLER.encoder)
                    node["_json_props"].append(key)
            elif isinstance(value, Settings):
                node[key] = json.dumps(value, cls=JSON_HANDLER.encoder, sort_keys=True)
                node["_json_props"].append(key)
            elif isinstance(value, GufeTokenizable):
                node_ = subgraph_ = self.gufe_nodes.get(
                    (value.key, scope.org, scope.campaign, scope.project)
                )
                if node_ is None:
                    subgraph_, node_, scoped_key_ = self._gufe_to_subgraph(
                        value.to_shallow_dict(),
                        labels=["GufeTokenizable", value.__class__.__name__],
                        gufe_key=value.key,
                        scope=scope,
                    )
                    self.gufe_nodes[
                        (value.key, scope.org, scope.campaign, scope.project)
                    ] = node_
                subgraph = (
                    subgraph
                    | Relationship.type("DEPENDS_ON")(
                        node,
                        node_,
                        attribute=key,
                        _org=scope.org,
                        _campaign=scope.campaign,
                        _project=scope.project,
                    )
                    | subgraph_
                )

            else:
                node[key] = value

        subgraph = subgraph | node

        return subgraph, node, scoped_key

    def _subgraph_to_gufe(
        self, nodes: List[Node], subgraph: Subgraph
    ) -> Dict[Node, GufeTokenizable]:
        """Get a Dict `GufeTokenizable` objects within the given subgraph.

        Any `GufeTokenizable` that requires nodes or relationships missing from
        the subgraph will not be returned.

        """
        nxg = self._subgraph_to_networkx(subgraph)
        nodes_to_gufe = {}
        gufe_objs = {}
        for node in nodes:
            gufe_objs[node] = self._node_to_gufe(node, nxg, nodes_to_gufe)

        return gufe_objs

    def _subgraph_to_networkx(self, subgraph: Subgraph):
        g = nx.DiGraph()

        for node in subgraph.nodes:
            g.add_node(node, **dict(node))

        for relationship in subgraph.relationships:
            g.add_edge(
                relationship.start_node, relationship.end_node, **dict(relationship)
            )

        return g

    def _node_to_gufe(
        self, node: Node, g: nx.DiGraph, mapping: Dict[Node, GufeTokenizable]
    ):
        # shortcut if we already have this object deserialized
        if gufe_obj := mapping.get(node):
            return gufe_obj

        dct = dict(node)
        # deserialize json-serialized attributes
        for key in dct["_json_props"]:
            dct[key] = json.loads(dct[key], cls=JSON_HANDLER.decoder)

        # inject dependencies
        dep_edges = g.edges(node)
        postprocess = set()
        for edge in dep_edges:
            u, v = edge
            edgedct = g.get_edge_data(u, v)
            if "attribute" in edgedct:
                if "key" in edgedct:
                    if not edgedct["attribute"] in dct:
                        dct[edgedct["attribute"]] = dict()
                    dct[edgedct["attribute"]][edgedct["key"]] = self._node_to_gufe(
                        v, g, mapping
                    )
                elif "index" in edgedct:
                    postprocess.add(edgedct["attribute"])
                    if not edgedct["attribute"] in dct:
                        dct[edgedct["attribute"]] = list()
                    dct[edgedct["attribute"]].append(
                        (edgedct["index"], self._node_to_gufe(v, g, mapping))
                    )
                else:
                    dct[edgedct["attribute"]] = self._node_to_gufe(v, g, mapping)

        # postprocess any attributes that are lists
        # needed because we don't control the order in which a list is built up
        # but can only order it post-hoc
        for attr in postprocess:
            dct[attr] = [j for i, j in sorted(dct[attr], key=lambda x: x[0])]

        # remove all neo4j-specific keys
        dct.pop("_json_props", None)
        dct.pop("_gufe_key", None)
        dct.pop("_org", None)
        dct.pop("_campaign", None)
        dct.pop("_project", None)
        dct.pop("_scoped_key", None)

        mapping[node] = res = GufeTokenizable.from_shallow_dict(dct)
        return res

    def _get_node(
        self,
        scoped_key: ScopedKey,
        return_subgraph=False,
    ) -> Union[Node, Tuple[Node, Subgraph]]:
        """
        If `return_subgraph = True`, also return subgraph for gufe object.

        """
        qualname = scoped_key.qualname

        properties = {"_scoped_key": str(scoped_key)}
        prop_string = ", ".join(
            "{}: '{}'".format(key, value) for key, value in properties.items()
        )

        prop_string = f" {{{prop_string}}}"

        q = f"""
        MATCH (n:{qualname}{prop_string})
        """

        if return_subgraph:
            q += """
            OPTIONAL MATCH p = (n)-[r:DEPENDS_ON*]->(m)
            WHERE NOT (m)-[:DEPENDS_ON]->()
            RETURN n,p
            """
        else:
            q += """
            RETURN n
            """

        nodes = set()
        subgraph = Subgraph()

        for record in self.execute_query(q).records:
            node = record_data_to_node(record["n"])
            nodes.add(node)
            if return_subgraph and record["p"] is not None:
                subgraph = subgraph | subgraph_from_path_record(record["p"])
            else:
                subgraph = node

        if len(nodes) == 0:
            raise KeyError("No such object in database")
        elif len(nodes) > 1:
            raise Neo4JStoreError(
                "More than one such object in database; this should not be possible"
            )

        if return_subgraph:
            return list(nodes)[0], subgraph
        else:
            return list(nodes)[0]

    def _query(
        self,
        *,
        qualname: str,
        additional: Dict = None,
        key: GufeKey = None,
        scope: Scope = Scope(),
        return_gufe=False,
    ):
        properties = {
            "_org": scope.org,
            "_campaign": scope.campaign,
            "_project": scope.project,
        }

        for k, v in list(properties.items()):
            if v is None:
                properties.pop(k)

        if key is not None:
            properties["_gufe_key"] = str(key)

        if additional is None:
            additional = {}
        properties.update({k: v for k, v in additional.items() if v is not None})

        if not properties:
            prop_string = ""
        else:
            prop_string = ", ".join(
                "{}: '{}'".format(key, value) for key, value in properties.items()
            )

            prop_string = f" {{{prop_string}}}"

        q = f"""
        MATCH (n:{qualname}{prop_string})
        """
        if return_gufe:
            q += """
            OPTIONAL MATCH p = (n)-[r:DEPENDS_ON*]->(m)
            WHERE NOT (m)-[:DEPENDS_ON]->()
            RETURN n,p
            """
        else:
            q += """
            RETURN DISTINCT n
            ORDER BY n._org, n._campaign, n._project, n._gufe_key
            """

        with self.transaction() as tx:
            res = tx.run(q).to_eager_result()

        nodes = list()
        subgraph = Subgraph()

        for record in res.records:
            node = record_data_to_node(record["n"])
            nodes.append(node)
            if return_gufe and record["p"] is not None:
                subgraph = subgraph | subgraph_from_path_record(record["p"])
            else:
                subgraph = node

        if return_gufe:
            return {
                ScopedKey.from_str(k["_scoped_key"]): v
                for k, v in self._subgraph_to_gufe(nodes, subgraph).items()
            }
        else:
            return [ScopedKey.from_str(i["_scoped_key"]) for i in nodes]

    def check_existence(self, scoped_key: ScopedKey):
        try:
            self._get_node(scoped_key=scoped_key)
        except KeyError:
            return False

        return True

    def get_scoped_key(self, obj: GufeTokenizable, scope: Scope):
        qualname = obj.__class__.__qualname__
        res = self._query(qualname=qualname, key=obj.key, scope=scope)

        if len(res) == 0:
            raise KeyError("No such object in database")
        elif len(res) > 1:
            raise Neo4JStoreError(
                "More than one such object in database; this should not be possible"
            )

        return res[0]

    def get_gufe(self, scoped_key: ScopedKey):
        node, subgraph = self._get_node(scoped_key=scoped_key, return_subgraph=True)
        return self._subgraph_to_gufe([node], subgraph)[node]

    def assemble_network(
        self,
        network: AlchemicalNetwork,
        scope: Scope,
        state: Union[NetworkStateEnum, str] = NetworkStateEnum.active,
    ):
        """Create all nodes and relationships needed for an AlchemicalNetwork
        represented in an alchemiscale state store.

        Parameters
        ----------
        network
            The AlchemicalNetwork to submit to the database.
        scope
            The Scope where the AlchemicalNetwork resides.
        state
            The starting state of the network as marked by the NetworkMark.

        Returns
        -------
        A tuple containing the AlchemicalNetwork ScopedKey, the TaskHub
        ScopedKey, and the NetworkMark ScopedKey.
        """

        nw_subgraph, nw_node, nw_sk = self.create_network_subgraph(network, scope)
        th_subgraph, th_node, th_sk = self.create_taskhub_subgraph(nw_node)
        nm_subgraph, nm_node, nm_sk = self.create_network_mark_subgraph(nw_node, state)

        subgraph = nw_subgraph | th_subgraph | nm_subgraph

        with self.transaction() as tx:
            merge_subgraph(tx, subgraph, "GufeTokenizable", "_scoped_key")

        return nw_sk, th_sk, nm_sk

    def create_network_subgraph(self, network: AlchemicalNetwork, scope: Scope):
        """Create a Subgraph for the given AlchemicalNetwork.

        Parameters
        ----------
        network
            An AlchemicalNetwork to generate a Subgraph form of.
        scope
            The Scope where the AlchemicalNetwork resides.

        Returns
        -------
        A tuple containing the AlchemicalNetwork Subgraph, the specific
        AlchemicalNetwork Node within the Subgraph, and the ScopedKey of the
        AlchemicalNetwork.
        """

        validate_network_nonself(network)

        ndict = network.to_shallow_dict()

        subgraph, node, scoped_key = self._gufe_to_subgraph(
            ndict,
            labels=["GufeTokenizable", network.__class__.__name__],
            gufe_key=network.key,
            scope=scope,
        )

        return subgraph, node, scoped_key

    def delete_network(
        self,
        network: ScopedKey,
    ) -> ScopedKey:
        """Delete the given `AlchemicalNetwork` from the database.

        This will not remove any `Transformation`\s or `ChemicalSystem`\s
        associated with the `AlchemicalNetwork`, since these may be associated
        with other `AlchemicalNetwork`s in the same `Scope`.

        """
        # note: something like the following could perhaps be used to delete everything that is *only*
        # associated with this network
        # not yet tested though
        """
        MATCH p = (n:AlchemicalNetwork {{_scoped_key: "{network_node['_scoped_key']}")-[r:DEPENDS_ON*]->(m),
              (n)-[:DEPENDS_ON]->(t:Transformation|NonTransformation),
              (n)-[:DEPENDS_ON]->(c:ChemicalSystem)
        OPTIONAL MATCH (o:AlchemicalNetwork)
        WHERE NOT o._scoped_key = "{network_node['_scoped_key']}"
          AND NOT (t)<-[:DEPENDS_ON]-(o)
          AND NOT (c)<-[:DEPENDS_ON]-(o)
        return distinct n,m
        """
        # first, delete the network's hub if present
        self.delete_taskhub(network)

        # then delete the network
        q = f"""
        MATCH (an:AlchemicalNetwork {{_scoped_key: "{network}"}})
        DETACH DELETE an
        """
        raise NotImplementedError

    def get_network_state(self, networks: List[ScopedKey]) -> List[Optional[str]]:
        """Get the states of a group of networks.

        Parameters
        ----------
        networks
            The list networks to get the states of.

        Returns
        -------
        List[Optional[str]]
            A list containing the states of the given networks, in the same
            order as they were provided. If a network was not found, ``None``
            is returned at the corresponding index.
        """

        q = """
            UNWIND $networks AS network
            MATCH (an:AlchemicalNetwork {`_scoped_key`: network})<-[:MARKS]-(nm:NetworkMark)
            RETURN an._scoped_key as sk, nm.state AS state
        """

        results = self.execute_query(q, networks=[str(network) for network in networks])

        state_results = {}
        for record in results.records:
            sk = record["sk"]
            state = record["state"]

            state_results[sk] = state

        return [state_results.get(str(network), None) for network in networks]

    def create_network_mark_subgraph(
        self, network_node: Node, state=NetworkStateEnum.active
    ):
        """Create the Subgraph for an AlchemicalNetwork's NetworkMark.

        Parameters
        ----------
        network_node
            A Node representing the target AlchemicalNetwork. This is
            a returned value from the :py:meth:`create_network_subgraph`
            method.
        state
            The starting state for the AlchemicalNetwork.

        Returns
        -------
        A tuple containing the NetworkMark Subgraph, the specific NetworkMark
        Node within the Subgraph, and the ScopedKey of the NetworkMark.
        """

        network_sk = ScopedKey.from_str(network_node["_scoped_key"])
        scope = network_sk.scope

        network_mark = NetworkMark(target=str(network_sk), state=state)

        _, network_mark_node, scoped_key = self._gufe_to_subgraph(
            network_mark.to_shallow_dict(),
            labels=["GufeTokenizable", network_mark.__class__.__name__],
            gufe_key=network_mark.key,
            scope=scope,
        )

        subgraph = Relationship.type("MARKS")(
            network_mark_node,
            network_node,
            _org=scope.org,
            _campaign=scope.campaign,
            _project=scope.project,
        )

        return subgraph, network_mark_node, scoped_key

    def set_network_state(
        self, networks: List[ScopedKey], states: List[str]
    ) -> List[Optional[ScopedKey]]:
        """Set the state of a group of AlchemicalNetworks.

        Parameters
        ----------
        networks
            A list networks to set the states for.
        states
            A list of states to set the networks to.

        Returns
        -------
        List[Optional[ScopedKey]]
            The list of ScopedKeys for networks that were updated. If the
            network could not be found in the database, ``None`` is returned at
            the corresponding index.
        """

        if len(networks) != len(states):
            msg = "networks and states must have the same length"
            raise ValueError(msg)

        for network, state in zip(networks, states):
            if network.qualname != "AlchemicalNetwork":
                raise ValueError(
                    "`network` ScopedKey does not correspond to an `AlchemicalNetwork`"
                )
            try:
                NetworkStateEnum(state)
            except ValueError:
                valid_states = [state.value for state in NetworkStateEnum]
                msg = f"'{state}' is not a valid state. Valid values include: {valid_states}"
                raise ValueError(msg)

        q = """
            WITH $inputs AS inputs
            UNWIND inputs AS x
            WITH x[0] as network, x[1] as state
            MATCH (:AlchemicalNetwork {`_scoped_key`: network})<-[:MARKS]-(nm:NetworkMark)
            SET nm.state = state
            RETURN network
        """
        inputs = [[str(network), state] for network, state in zip(networks, states)]

        results = self.execute_query(q, inputs=inputs)

        network_results = {}
        for record in results.records:
            network_sk_str = record["network"]
            network_results[network_sk_str] = ScopedKey.from_str(network_sk_str)

        return [network_results.get(str(network), None) for network in networks]

    def query_networks(
        self,
        *,
        name=None,
        key=None,
        scope: Optional[Scope] = Scope(),
        state: Optional[str] = None,
    ) -> List[ScopedKey]:
        """Query for `AlchemicalNetwork`\s matching given attributes."""

        query_params = dict(
            name_pattern=name,
            org_pattern=scope.org,
            campaign_pattern=scope.campaign,
            project_pattern=scope.project,
            state_pattern=state,
            gufe_key_pattern=None if key is None else str(key),
        )

        where_params = dict(
            name_pattern="an.name",
            org_pattern="an.`_org`",
            campaign_pattern="an.`_campaign`",
            project_pattern="an.`_project`",
            state_pattern="nm.state",
            gufe_key_pattern="an.`_gufe_key`",
        )

        conditions = []

        for k, v in query_params.items():
            if v is not None:
                conditions.append(f"{where_params[k]} =~ ${k}")

        where_clause = "WHERE " + " AND ".join(conditions) if len(conditions) else ""

        q = f"""
            MATCH (an:AlchemicalNetwork)<-[:MARKS]-(nm:NetworkMark)
            {where_clause}
            RETURN an._scoped_key as sk
        """

        results = self.execute_query(
            q,
            parameters_=query_params,
        )

        network_sks = []
        for record in results.records:
            sk = record["sk"]
            network_sks.append(ScopedKey.from_str(sk))

        return network_sks

    def query_transformations(self, *, name=None, key=None, scope: Scope = Scope()):
        """Query for `Transformation`\s matching given attributes."""
        additional = {"name": name}
        return self._query(
            qualname="Transformation|NonTransformation",
            additional=additional,
            key=key,
            scope=scope,
        )

    def query_chemicalsystems(self, *, name=None, key=None, scope: Scope = Scope()):
        """Query for `ChemicalSystem`\s matching given attributes."""
        additional = {"name": name}
        return self._query(
            qualname="ChemicalSystem", additional=additional, key=key, scope=scope
        )

    def get_network_transformations(self, network: ScopedKey) -> List[ScopedKey]:
        """List ScopedKeys for Transformations associated with the given AlchemicalNetwork."""
        q = f"""
        MATCH (:AlchemicalNetwork {{_scoped_key: '{network}'}})-[:DEPENDS_ON]->(t:Transformation|NonTransformation)
        WITH t._scoped_key as sk
        RETURN sk
        """
        sks = []
        with self.transaction() as tx:
            res = tx.run(q)
            for rec in res:
                sks.append(rec["sk"])

        return [ScopedKey.from_str(sk) for sk in sks]

    def get_transformation_networks(self, transformation: ScopedKey) -> List[ScopedKey]:
        """List ScopedKeys for AlchemicalNetworks associated with the given Transformation."""
        q = f"""
        MATCH (:Transformation|NonTransformation {{_scoped_key: '{transformation}'}})<-[:DEPENDS_ON]-(an:AlchemicalNetwork)
        WITH an._scoped_key as sk
        RETURN sk
        """
        sks = []
        with self.transaction() as tx:
            res = tx.run(q)
            for rec in res:
                sks.append(rec["sk"])

        return [ScopedKey.from_str(sk) for sk in sks]

    def get_network_chemicalsystems(self, network: ScopedKey) -> List[ScopedKey]:
        """List ScopedKeys for ChemicalSystems associated with the given AlchemicalNetwork."""
        q = f"""
        MATCH (:AlchemicalNetwork {{_scoped_key: '{network}'}})-[:DEPENDS_ON]->(cs:ChemicalSystem)
        WITH cs._scoped_key as sk
        RETURN sk
        """
        sks = []
        with self.transaction() as tx:
            res = tx.run(q)
            for rec in res:
                sks.append(rec["sk"])

        return [ScopedKey.from_str(sk) for sk in sks]

    def get_chemicalsystem_networks(self, chemicalsystem: ScopedKey) -> List[ScopedKey]:
        """List ScopedKeys for AlchemicalNetworks associated with the given ChemicalSystem."""
        q = f"""
        MATCH (:ChemicalSystem {{_scoped_key: '{chemicalsystem}'}})<-[:DEPENDS_ON]-(an:AlchemicalNetwork)
        WITH an._scoped_key as sk
        RETURN sk
        """
        sks = []
        with self.transaction() as tx:
            res = tx.run(q)
            for rec in res:
                sks.append(rec["sk"])

        return [ScopedKey.from_str(sk) for sk in sks]

    def get_transformation_chemicalsystems(
        self, transformation: ScopedKey
    ) -> List[ScopedKey]:
        """List ScopedKeys for the ChemicalSystems associated with the given Transformation."""
        q = f"""
        MATCH (:Transformation|NonTransformation {{_scoped_key: '{transformation}'}})-[:DEPENDS_ON]->(cs:ChemicalSystem)
        WITH cs._scoped_key as sk
        RETURN sk
        """
        sks = []
        with self.transaction() as tx:
            res = tx.run(q)
            for rec in res:
                sks.append(rec["sk"])

        return [ScopedKey.from_str(sk) for sk in sks]

    def get_chemicalsystem_transformations(
        self, chemicalsystem: ScopedKey
    ) -> List[ScopedKey]:
        """List ScopedKeys for the Transformations associated with the given ChemicalSystem."""
        q = f"""
        MATCH (:ChemicalSystem {{_scoped_key: '{chemicalsystem}'}})<-[:DEPENDS_ON]-(t:Transformation|NonTransformation)
        WITH t._scoped_key as sk
        RETURN sk
        """
        sks = []
        with self.transaction() as tx:
            res = tx.run(q)
            for rec in res:
                sks.append(rec["sk"])

        return [ScopedKey.from_str(sk) for sk in sks]

    def _get_protocoldagresultrefs(self, q: str, scoped_key: ScopedKey):
        sks = []
        with self.transaction() as tx:
            res = tx.run(q, scoped_key=str(scoped_key))
            for rec in res:
                sks.append(rec["sk"])

        return [ScopedKey.from_str(sk) for sk in sks]

    def get_transformation_results(
        self, transformation: ScopedKey
    ) -> List[ProtocolDAGResultRef]:
        # get all task result protocoldagresultrefs corresponding to given transformation
        # returned in no particular order
        q = """
        MATCH (trans:Transformation|NonTransformation {_scoped_key: $scoped_key}),
              (trans)<-[:PERFORMS]-(:Task)-[:RESULTS_IN]->(res:ProtocolDAGResultRef)
        WHERE res.ok = true
        WITH res._scoped_key as sk
        RETURN DISTINCT sk
        """
        return self._get_protocoldagresultrefs(q, transformation)

    def get_transformation_failures(
        self, transformation: ScopedKey
    ) -> List[ProtocolDAGResultRef]:
        # get all task failure protocoldagresultrefs corresponding to given transformation
        # returned in no particular order
        q = """
        MATCH (trans:Transformation|NonTransformation {_scoped_key: $scoped_key}),
              (trans)<-[:PERFORMS]-(:Task)-[:RESULTS_IN]->(res:ProtocolDAGResultRef)
        WHERE res.ok = false
        WITH res._scoped_key as sk
        RETURN DISTINCT sk
        """
        return self._get_protocoldagresultrefs(q, transformation)

    ## compute

    def set_strategy(
        self,
        strategy: Strategy,
        network: ScopedKey,
    ) -> ScopedKey:
        """Set the compute Strategy for the given AlchemicalNetwork."""

        if network.qualname != "AlchemicalNetwork":
            raise ValueError(
                "`network` ScopedKey does not correspond to an `AlchemicalNetwork`"
            )
        raise NotImplementedError

    def register_computeservice(
        self, compute_service_registration: ComputeServiceRegistration
    ):
        """Register a ComputeServiceRegistration uniquely identifying a running
        ComputeService.

        A ComputeServiceRegistration node is used for CLAIMS relationships on
        Tasks to avoid collisions in Task execution.

        """

        node = Node(
            "ComputeServiceRegistration", **compute_service_registration.to_dict()
        )

        with self.transaction() as tx:
            create_subgraph(tx, Subgraph() | node)

        return compute_service_registration.identifier

    def deregister_computeservice(self, compute_service_id: ComputeServiceID):
        """Remove the registration for the given ComputeServiceID from the
        state store.

        This wil remove the ComputeServiceRegistration node, and all its CLAIMS
        relationships to Tasks.

        All Tasks with CLAIMS relationships to the ComputeServiceRegistration
        and with status `running` will have their status set to `waiting`.

        """

        q = f"""
        MATCH (n:ComputeServiceRegistration {{identifier: '{compute_service_id}'}})

        OPTIONAL MATCH (n)-[cl:CLAIMS]->(t:Task {{status: 'running'}})
        SET t.status = 'waiting'

        WITH n, n.identifier as identifier

        DETACH DELETE n

        RETURN identifier
        """

        with self.transaction() as tx:
            res = tx.run(q)
            identifier = next(res)["identifier"]

        return ComputeServiceID(identifier)

    def heartbeat_computeservice(
        self, compute_service_id: ComputeServiceID, heartbeat: datetime
    ):
        """Update the heartbeat for the given ComputeServiceID."""

        q = f"""
        MATCH (n:ComputeServiceRegistration {{identifier: '{compute_service_id}'}})
        SET n.heartbeat = localdatetime('{heartbeat.isoformat()}')

        """
        with self.transaction() as tx:
            tx.run(q)

        return compute_service_id

    def expire_registrations(self, expire_time: datetime):
        """Remove all registrations with last heartbeat prior to the given `expire_time`."""
        q = f"""
        MATCH (n:ComputeServiceRegistration)
        WHERE n.heartbeat < localdatetime('{expire_time.isoformat()}')

        WITH n

        OPTIONAL MATCH (n)-[cl:CLAIMS]->(t:Task {{status: 'running'}})
        SET t.status = 'waiting'

        WITH n, n.identifier as ident

        DETACH DELETE n

        RETURN ident
        """
        with self.transaction() as tx:
            res = tx.run(q)

            identities = set()
            for rec in res:
                identities.add(rec["ident"])

        return [ComputeServiceID(i) for i in identities]

    ## task hubs

    def create_taskhub_subgraph(self, network_node: Node):
        """Create a Subgraph for an AlchemicalNetwork's TaskHub.

        Parameters
        ----------
        network_node
            A Node representing the target AlchemicalNetwork. This is
            a returned value from the :py:meth:`create_network_subgraph`
            method.

        Returns
        -------
        A tuple containing the TaskHub Subgraph, the specific TaskHub
        Node within the Subgraph, and the ScopedKey of the TaskHub.
        """
        network_sk = ScopedKey.from_str(network_node["_scoped_key"])
        scope = network_sk.scope

        taskhub = TaskHub(network=str(network_sk))
        _, taskhub_node, scoped_key = self._gufe_to_subgraph(
            taskhub.to_shallow_dict(),
            labels=["GufeTokenizable", taskhub.__class__.__name__],
            gufe_key=taskhub.key,
            scope=scope,
        )

        subgraph = Relationship.type("PERFORMS")(
            taskhub_node,
            network_node,
            _org=scope.org,
            _campaign=scope.campaign,
            _project=scope.project,
        )

        return subgraph, taskhub_node, scoped_key

    def query_taskhubs(
        self, scope: Optional[Scope] = Scope(), return_gufe: bool = False
    ) -> Union[List[ScopedKey], Dict[ScopedKey, TaskHub]]:
        """Query for `TaskHub`\s matching the given criteria.

        Parameters
        ----------
        return_gufe
            If True, return a dict with `ScopedKey`s as keys, `TaskHub`
            instances as values. Otherwise, return a list of `ScopedKey`\s.

        """
        return self._query(qualname="TaskHub", scope=scope, return_gufe=return_gufe)

    def get_taskhubs(
        self, network_scoped_keys: list[ScopedKey], return_gufe: bool = False
    ) -> list[Union[ScopedKey, TaskHub]]:
        """Get the TaskHubs for the given AlchemicalNetworks.

        Parameters
        ----------
        return_gufe
            If True, return `TaskHub` instances.
            Otherwise, return `ScopedKey`s.

        """

        # TODO: this could fail better, report all instances rather than first
        for network_scoped_key in network_scoped_keys:
            if network_scoped_key.qualname != "AlchemicalNetwork":
                raise ValueError(
                    "`network` ScopedKey does not correspond to an `AlchemicalNetwork`"
                )

        query = """
        UNWIND $network_scoped_keys AS network_scoped_key
        MATCH (th:TaskHub {network: network_scoped_key})-[:PERFORMS]->(an:AlchemicalNetwork)
        RETURN th, an
        """

        query_results = self.execute_query(
            query, network_scoped_keys=list(map(str, network_scoped_keys))
        )

        def _node_to_gufe(node):
            return self._subgraph_to_gufe([node], node)[node]

        def _node_to_scoped_key(node):
            return ScopedKey.from_str(node["_scoped_key"])

        transform_function = _node_to_gufe if return_gufe else _node_to_scoped_key
        transform_results = defaultdict(None)
        for record in query_results.records:
            node = record_data_to_node(record["th"])
            network_scoped_key = record["an"]["_scoped_key"]
            transform_results[network_scoped_key] = transform_function(node)

        return [
            transform_results[str(network_scoped_key)]
            for network_scoped_key in network_scoped_keys
        ]

    def get_taskhub(
        self, network: ScopedKey, return_gufe: bool = False
    ) -> Union[ScopedKey, TaskHub]:
        """Get the TaskHub for the given AlchemicalNetwork.

        Parameters
        ----------
        return_gufe
            If True, return a `TaskHub` instance.
            Otherwise, return a `ScopedKey`.

        """

        return self.get_taskhubs([network], return_gufe)[0]

    def delete_taskhub(
        self,
        network: ScopedKey,
    ) -> ScopedKey:
        """Delete a TaskHub for a given AlchemicalNetwork."""

        if network.qualname != "AlchemicalNetwork":
            raise ValueError(
                "`network` ScopedKey does not correspond to an `AlchemicalNetwork`"
            )

        taskhub = self.get_taskhub(network)

        q = f"""
        MATCH (th:TaskHub {{_scoped_key: '{taskhub}'}}),
        DETACH DELETE th
        """
        self.execute_query(q)

        return taskhub

    def set_taskhub_weight(
        self, networks: List[ScopedKey], weights: List[float]
    ) -> List[Optional[ScopedKey]]:
        """Set the weights for the TaskHubs associated with the given
        AlchemicalNetworks.

        """

        for weight in weights:
            if not 0 <= weight <= 1:
                raise ValueError("all `weights` must be between 0 and 1 (inclusive)")

        for network in networks:
            if network.qualname != "AlchemicalNetwork":
                raise ValueError(
                    "a `networks` ScopedKey does not correspond to an `AlchemicalNetwork`"
                )

        if len(networks) != len(weights):
            raise ValueError("length of `networks` and `weights` must be the same")

        q = """
        WITH $inputs AS inputs
        UNWIND inputs AS x
        WITH x[0] as network, x[1] as weight
        MATCH (th:TaskHub {network: network})
        SET th.weight = weight
        RETURN network
        """
        inputs = [[str(network), weight] for network, weight in zip(networks, weights)]

        results = self.execute_query(q, inputs=inputs)

        network_results = {}
        for record in results.records:
            network_sk_str = record["network"]
            network_results[network_sk_str] = ScopedKey.from_str(network_sk_str)

        return [network_results.get(str(network), None) for network in networks]

    def get_taskhub_actioned_tasks(
        self,
        taskhubs: List[ScopedKey],
    ) -> List[Dict[ScopedKey, float]]:
        """Get the Tasks that the given TaskHubs ACTIONS.

        Parameters
        ----------
        taskhubs
            The ScopedKeys of the TaskHubs to query.

        Returns
        -------
        tasks
            A list of dicts, one per TaskHub, which contains the Task ScopedKeys that are
            actioned on the given TaskHub as keys, with their weights as values.
        """

        q = f"""
           UNWIND {cypher_list_from_scoped_keys(taskhubs)} as th_sk
           MATCH (th: TaskHub {{_scoped_key: th_sk}})-[a:ACTIONS]->(t:Task)
           RETURN t._scoped_key, a.weight, th._scoped_key
        """

        results = self.execute_query(q)

        data = {taskhub: {} for taskhub in taskhubs}
        for record in results.records:
            th_sk = ScopedKey.from_str(record["th._scoped_key"])
            t_sk = ScopedKey.from_str(record["t._scoped_key"])
            weight = record["a.weight"]

            data[th_sk][t_sk] = weight

        return [data[taskhub] for taskhub in taskhubs]

    def get_task_actioned_networks(self, task: ScopedKey) -> Dict[ScopedKey, float]:
        """Get all AlchemicalNetwork ScopedKeys whose TaskHub ACTIONS a given Task.

        Parameters
        ----------
        task
            The ScopedKey of the Task to obtain actioned AlchemicalNetworks
            for.

        Returns
        -------
        networks
            A dict with AlchemicalNetwork ScopedKeys whose TaskHub actions a
            given Task as keys, Task weights as values.
        """

        q = """
           MATCH (an:AlchemicalNetwork)<-[:PERFORMS]-(TaskHub)-[a:ACTIONS]->(Task {_scoped_key: $scoped_key})
           RETURN an._scoped_key, a.weight
        """

        with self.transaction() as tx:
            results = tx.run(q, scoped_key=str(task)).to_eager_result()

        return {
            ScopedKey.from_str(record.get("an._scoped_key")): record.get("a.weight")
            for record in results.records
        }

    def get_taskhub_weight(self, networks: List[ScopedKey]) -> List[float]:
        """Get the weight for the TaskHubs associated with the given
        AlchemicalNetworks.

        """

        for network in networks:
            if network.qualname != "AlchemicalNetwork":
                raise ValueError(
                    "`network` ScopedKey does not correspond to an `AlchemicalNetwork`"
                )

        q = f"""
        UNWIND {cypher_list_from_scoped_keys(networks)} as network
        MATCH (th:TaskHub {{network: network}})
        RETURN network, th.weight
        """

        results = self.execute_query(q)

        network_weights = {str(network): None for network in networks}
        for record in results.records:
            weight = record["th.weight"]
            network_sk_str = record["network"]
            network_weights[network_sk_str] = weight

        return [network_weights[str(network)] for network in networks]

    def action_tasks(
        self,
        tasks: List[ScopedKey],
        taskhub: ScopedKey,
    ) -> List[Union[ScopedKey, None]]:
        """Add Tasks to the TaskHub for a given AlchemicalNetwork.

        Note: the Tasks must be within the same scope as the AlchemicalNetwork,
        and must correspond to a Transformation in the AlchemicalNetwork.

        A given compute task can be represented in any number of
        AlchemicalNetwork TaskHubs, or none at all.

        Only Tasks with status 'waiting', 'running', or 'error' can be
        actioned.

        """
        # since UNWIND doesn't guarantee order, we need to keep track manually
        # so we can properly return `None` if needed
        task_map = {str(task): None for task in tasks}

        query_safe_task_list = [str(task) for task in tasks if task]

        q = """
        // get our TaskHub
        UNWIND $query_safe_task_list AS task_sk
        MATCH (th:TaskHub {_scoped_key: $taskhub_scoped_key})-[:PERFORMS]->(an:AlchemicalNetwork)

        // get the task we want to add to the hub; check that it connects to same network
        MATCH (task:Task {_scoped_key: task_sk})-[:PERFORMS]->(:Transformation|NonTransformation)<-[:DEPENDS_ON]-(an)

        // only proceed for cases where task is not already actioned on hub
        // and where the task is either in 'waiting', 'running', or 'error' status
        WITH th, an, task
        WHERE NOT (th)-[:ACTIONS]->(task)
          AND task.status IN [$waiting, $running, $error]

        // create the connection
        CREATE (th)-[ar:ACTIONS {weight: 0.5}]->(task)

        // set the task property to the scoped key of the Task
        // this is a convenience for when we have to loop over relationships in Python
        SET ar.task = task._scoped_key

        // we want to preserve the list of tasks for the return, so we need to make a subquery
        // since the subsequent WHERE clause could reduce the records in task
        WITH task, th
        CALL {
            WITH task, th
            MATCH (trp: TaskRestartPattern)-[:ENFORCES]->(th)
            WHERE NOT (trp)-[:APPLIES]->(task)

            CREATE (trp)-[:APPLIES {num_retries: 0}]->(task)
        }

        RETURN task
        """

        results = self.execute_query(
            q,
            query_safe_task_list=query_safe_task_list,
            waiting=TaskStatusEnum.waiting.value,
            running=TaskStatusEnum.running.value,
            error=TaskStatusEnum.error.value,
            taskhub_scoped_key=str(taskhub),
        )

        # update our map with the results, leaving None for tasks that aren't found
        for task_record in results.records:
            sk = task_record["task"]["_scoped_key"]
            task_map[str(sk)] = ScopedKey.from_str(sk)

        return [task_map[str(t)] for t in tasks]

    def set_task_weights(
        self,
        tasks: Union[Dict[ScopedKey, float], List[ScopedKey]],
        taskhub: ScopedKey,
        weight: Optional[float] = None,
    ) -> List[Union[ScopedKey, None]]:
        """Sets weights for the ACTIONS relationship between a TaskHub and a Task.

        This is used to set the relative probabilistic execution order of a
        Task in a TaskHub. Note that this concept is orthogonal to priority in
        that tasks of higher priority will be executed before tasks of lower
        priority, but tasks of the same priority will be distributed according
        to their weights.

        The weights can be set by either a list and a scalar, or a dict of
        {ScopedKey: weight} pairs.

        Must be called after `action_tasks` to have any effect; otherwise, the
        TaskHub will not have an ACTIONS relationship to the Task.

        Parameters
        ----------
        tasks: Union[Dict[ScopedKey, float], List[ScopedKey]]
            If a dict, the keys are the ScopedKeys of the Tasks, and the values are the weights.
            If a list, the weights are set to the scalar value given by the `weight` argument.

        taskhub: ScopedKey
            The ScopedKey of the TaskHub associated with the Tasks.

        weight: Optional[float]
            If `tasks` is a list, this is the weight to set for each Task.

        Returns
        -------
        List[ScopedKey, None]
            A list of ScopedKeys for each Task whose weight was set.
            `None` is given for Tasks that weight was not set for; this could
            be because the TaskHub doesn't have an ACTIONS relationship with the Task,
            or the Task doesn't exist at all

        """
        results = []
        tasks_changed = []
        with self.transaction() as tx:
            if isinstance(tasks, dict):
                if weight is not None:
                    raise ValueError(
                        "Cannot set `weight` to a scalar if `tasks` is a dict"
                    )

                if not all([0 <= weight <= 1 for weight in tasks.values()]):
                    raise ValueError("weights must be between 0 and 1 (inclusive)")

                for t, w in tasks.items():
                    q = f"""
                    MATCH (th:TaskHub {{_scoped_key: '{taskhub}'}})-[ar:ACTIONS]->(task:Task {{_scoped_key: '{t}'}})
                    SET ar.weight = {w}
                    RETURN task, ar
                    """
                    results.append(tx.run(q).to_eager_result())

            elif isinstance(tasks, list):
                if weight is None:
                    raise ValueError(
                        "Must set `weight` to a scalar if `tasks` is a list"
                    )

                if not 0 <= weight <= 1:
                    raise ValueError("weight must be between 0 and 1 (inclusive)")

                # TODO: remove for loop with an unwind clause
                for t in tasks:
                    q = f"""
                    MATCH (th:TaskHub {{_scoped_key: '{taskhub}'}})-[ar:ACTIONS]->(task:Task {{_scoped_key: '{t}'}})
                    SET ar.weight = {weight}
                    RETURN task, ar
                    """
                    results.append(tx.run(q).to_eager_result())

        # return ScopedKeys for Tasks we changed; `None` for tasks we didn't
        for res in results:
            for record in res.records:
                task = record["task"]
                tasks_changed.append(
                    ScopedKey.from_str(task["_scoped_key"])
                    if task is not None
                    else None
                )

        return tasks_changed

    def get_task_weights(
        self,
        tasks: List[ScopedKey],
        taskhub: ScopedKey,
    ) -> List[Union[float, None]]:
        """Get weights for the ACTIONS relationship between a TaskHub and a Task.

        Parameters
        ----------
        tasks
            The ScopedKeys of the Tasks to get the weights for.
        taskhub
            The ScopedKey of the TaskHub associated with the Tasks.

        Returns
        -------
        weights
            Weights for the list of Tasks, in the same order.
        """
        weights = []
        with self.transaction() as tx:
            for t in tasks:
                q = f"""
                MATCH (th:TaskHub {{_scoped_key: '{taskhub}'}})-[ar:ACTIONS]->(task:Task {{_scoped_key: '{t}'}})
                RETURN ar.weight
                """
                result = tx.run(q)

                weight = [record.get("ar.weight") for record in result]

                # if no match for the given Task, we put a `None` as result
                if len(weight) == 0:
                    weights.append(None)
                else:
                    weights.extend(weight)

        return weights

    @chainable
    def cancel_tasks(
        self,
        tasks: List[ScopedKey],
        taskhub: ScopedKey,
        tx=None,
    ) -> List[Union[ScopedKey, None]]:
        """Remove Tasks from the TaskHub for a given AlchemicalNetwork.

        Note: Tasks must be within the same scope as the AlchemicalNetwork.

        A given Task can be represented in many AlchemicalNetwork TaskHubs, or
        none at all.

        """
        query = """
        UNWIND $task_scoped_keys AS task_scoped_key
        MATCH (th:TaskHub {_scoped_key: $taskhub_scoped_key})-[ar:ACTIONS]->(task:Task {_scoped_key: task_scoped_key})
        DELETE ar

        WITH task, th
        CALL {
            WITH task, th
            MATCH (task)<-[applies:APPLIES]-(:TaskRestartPattern)-[:ENFORCES]->(th)
            DELETE applies
        }

        RETURN task._scoped_key as task_scoped_key
        """
        results = tx.run(
            query,
            task_scoped_keys=list(map(str, tasks)),
            taskhub_scoped_key=str(taskhub),
        ).to_eager_result()

        returned_keys = {record["task_scoped_key"] for record in results.records}
        filtered_tasks = [
            task if str(task) in returned_keys else None for task in tasks
        ]

        return filtered_tasks

    def get_taskhub_tasks(
        self, taskhub: ScopedKey, return_gufe=False
    ) -> Union[List[ScopedKey], Dict[ScopedKey, Task]]:
        """Get a list of Tasks on the TaskHub."""

        q = f"""
        // get list of all tasks associated with the taskhub
        MATCH (th:TaskHub {{_scoped_key: '{taskhub}'}})-[:ACTIONS]->(task:Task)
        RETURN task
        """
        with self.transaction() as tx:
            res = tx.run(q).to_eager_result()

        tasks = []
        subgraph = Subgraph()
        for record in res.records:
            tasks.append(record_data_to_node(record["task"]))
            subgraph = subgraph | tasks[-1]

        if return_gufe:
            return {
                ScopedKey.from_str(k["_scoped_key"]): v
                for k, v in self._subgraph_to_gufe(tasks, subgraph).items()
            }
        else:
            return [ScopedKey.from_str(t["_scoped_key"]) for t in tasks]

    def get_taskhub_unclaimed_tasks(
        self, taskhub: ScopedKey, return_gufe=False
    ) -> Union[List[ScopedKey], Dict[ScopedKey, Task]]:
        """Get a list of unclaimed Tasks in the TaskHub."""

        q = f"""
        // get list of all unclaimed tasks in the hub
        MATCH (th:TaskHub {{_scoped_key: '{taskhub}'}})-[:ACTIONS]->(task:Task)
        WHERE NOT (task)<-[:CLAIMS]-(:ComputeServiceRegistration)
        RETURN task
        """
        with self.transaction() as tx:
            res = tx.run(q).to_eager_result()

        tasks = []
        subgraph = Subgraph()
        for record in res.records:
            node = record_data_to_node(record["task"])
            tasks.append(node)
            subgraph = subgraph | node

        if return_gufe:
            return {
                ScopedKey.from_str(k["_scoped_key"]): v
                for k, v in self._subgraph_to_gufe(tasks, subgraph).items()
            }
        else:
            return [ScopedKey.from_str(t["_scoped_key"]) for t in tasks]

    def claim_taskhub_tasks(
        self,
        taskhub: ScopedKey,
        compute_service_id: ComputeServiceID,
        count: int = 1,
        protocols: Optional[List[Union[Protocol, str]]] = None,
    ) -> List[Union[ScopedKey, None]]:
        """Claim a TaskHub Task.

        This method will claim Tasks from a TaskHub according to the following process:

        1. `waiting` Tasks with the highest priority are selected for consideration.
        2. Tasks with an `EXTENDS` relationship to an incomplete Task are dropped
           from consideration.
        3. Of those that remain, a Task is claimed stochastically based on the
           `weight` of its ACTIONS relationship with the TaskHub.

        This process is repeated until `count` Tasks have been claimed.
        If no Task is available, then `None` is given in its place.

        Parameters
        ----------
        compute_service_id
            Unique identifier for the compute service claiming the Tasks for execution.
        count
            Claim the given number of Tasks in a single transaction.
        protocols
            Protocols to restrict Task claiming to. `None` means no restriction.
            If an empty list, raises ValueError.

        """
        if protocols is not None and len(protocols) == 0:
            raise ValueError("`protocols` must be either `None` or not empty")

        q = f"""
            MATCH (th:TaskHub {{`_scoped_key`: '{taskhub}'}})-[actions:ACTIONS]-(task:Task)
            WHERE task.status = '{TaskStatusEnum.waiting.value}'
            AND actions.weight > 0
            OPTIONAL MATCH (task)-[:EXTENDS]->(other_task:Task)

            WITH task, other_task, actions
            """

        # filter down to `protocols`, if specified
        if protocols is not None:
            # need to extract qualnames if given protocol classes
            protocols = [
                protocol.__qualname__ if isinstance(protocol, Protocol) else protocol
                for protocol in protocols
            ]

            q += f"""
            MATCH (task)-[:PERFORMS]->(:Transformation|NonTransformation)-[:DEPENDS_ON]->(protocol:{cypher_or(protocols)})
            WITH task, other_task, actions
            """

        q += f"""
            WHERE other_task.status = '{TaskStatusEnum.complete.value}' OR other_task IS NULL

            RETURN task.`_scoped_key`, task.priority, actions.weight
            ORDER BY task.priority ASC
        """
        _tasks = {}
        with self.transaction() as tx:
            tx.run(
                f"""
            MATCH (th:TaskHub {{_scoped_key: '{taskhub}'}})

            // lock the TaskHub to avoid other queries from changing its state while we claim
            SET th._lock = True
            """
            )
            _taskpool = tx.run(q)

            def task_count(task_dict: dict):
                return sum(map(len, task_dict.values()))

            # directly use iterator to avoid pulling more tasks than we need
            # since we will likely stop early
            _task_iter = _taskpool.__iter__()
            while task_count(_tasks) < count:
                try:
                    candidate = next(_task_iter)
                    pr = candidate["task.priority"]

                    # get all tasks and their actions weights at each priority level
                    # until we've reached or surpassed `count`
                    _tasks[pr] = []
                    _tasks[pr].append(
                        (candidate["task.`_scoped_key`"], candidate["actions.weight"])
                    )
                    while True:

                        # if we've run out of tasks, stop immediately
                        if not (next_task := _taskpool.peek()):
                            raise StopIteration

                        # if next task has a different (lower) priority, stop consuming
                        if next_task["task.priority"] != pr:
                            break

                        candidate = next(_task_iter)
                        _tasks[candidate["task.priority"]].append(
                            (
                                candidate["task.`_scoped_key`"],
                                candidate["actions.weight"],
                            )
                        )

                except StopIteration:
                    break

            remaining = count
            tasks = []
            # for each group of tasks at each priority level
            for _, taskgroup in sorted(_tasks.items()):
                # if we want more tasks (or exactly as many tasks) as there are
                # in the group, just add them all
                if len(taskgroup) <= remaining:
                    tasks.extend(map(lambda x: ScopedKey.from_str(x[0]), taskgroup))

                    # immediately stop if we've reached our target count
                    if not (remaining := count - len(tasks)):
                        break

                # otherwise, perform a weighted random selection from the tasks
                # to fill out remaining
                else:
                    tasks.extend(
                        map(
                            ScopedKey.from_str,
                            _select_tasks_from_taskpool(taskgroup, remaining),
                        )
                    )

            # if tasks is not empty, proceed with claiming
            if tasks:
                q = _generate_claim_query(tasks, compute_service_id)
                tx.run(q)

            tx.run(
                f"""
            MATCH (th:TaskHub {{_scoped_key: '{taskhub}'}})

            // remove lock on the TaskHub now that we're done with it
            SET th._lock = null
            """
            )

        return tasks + [None] * (count - len(tasks))

    ## tasks

    def _validate_extends_tasks(self, task_list) -> Dict[str, Tuple[Node, str]]:

        if not task_list:
            return {}

        q = f"""
            UNWIND {cypher_list_from_scoped_keys(task_list)} as task
            MATCH (t:Task {{`_scoped_key`: task}})-[PERFORMS]->(tf:Transformation|NonTransformation)
            return t, tf._scoped_key as tf_sk
        """

        results = self.execute_query(q)

        nodes = {}

        for record in results.records:
            node = record_data_to_node(record["t"])
            transformation_sk = record["tf_sk"]

            status = node.get("status")

            if status in ("invalid", "deleted"):
                invalid_task_scoped_key = node["_scoped_key"]
                raise ValueError(
                    f"Cannot extend a `deleted` or `invalid` Task: {invalid_task_scoped_key}"
                )

            nodes[node["_scoped_key"]] = (node, transformation_sk)

        return nodes

    def create_tasks(
        self,
        transformations: List[ScopedKey],
        extends: Optional[List[Optional[ScopedKey]]] = None,
        creator: Optional[str] = None,
    ) -> List[ScopedKey]:
        """Create Tasks for the given Transformations.

        Note: this creates Tasks; it does not action them.

        Parameters
        ----------
        transformations
            The Transformations to create Tasks for.
            One Task is created for each Transformation ScopedKey given; to
            create multiple Tasks for a given Transformation, provide its
            ScopedKey multiple times.
        extends
            The ScopedKeys of the Tasks to use as a starting point for the
            created Tasks, in the same order as `transformations`. If ``None``
            given for a given Task, it will not extend any other Task.
            Will use the `ProtocolDAGResult` from the given Task as the
            `extends` input for the Task's eventual call to `Protocol.create`.
        creator (optional)
            The creator of the Tasks.
        """
        allowed_types = [Transformation.__qualname__, NonTransformation.__qualname__]

        # reshape data to a standard form
        if extends is None:
            extends = [None] * len(transformations)
        elif len(extends) != len(transformations):
            raise ValueError(
                "`extends` must either be `None` or have the same length as `transformations`"
            )

        for i, _extends in enumerate(extends):
            if _extends is not None:
                if not (extended_task_qualname := getattr(_extends, "qualname", None)):
                    raise ValueError(
                        f"`extends` entry for `Task` {transformations[i]} is not valid"
                    )
                if extended_task_qualname != "Task":
                    raise ValueError(
                        f"`extends` ScopedKey ({_extends}) does not correspond to a `Task`"
                    )

        transformation_map = {
            transformation_type: [[], []] for transformation_type in allowed_types
        }
        for i, transformation in enumerate(transformations):
            if transformation.qualname not in allowed_types:
                raise ValueError(
                    f"Got an unsupported `Transformation` type: {transformation.qualname}"
                )
            transformation_map[transformation.qualname][0].append(transformation)
            transformation_map[transformation.qualname][1].append(extends[i])

        extends_nodes = self._validate_extends_tasks(
            [_extends for _extends in extends if _extends is not None]
        )

        subgraph = Subgraph()

        sks = []
        # iterate over all allowed types, unpacking the transformations and extends subsets
        for node_type, (
            transformation_subset,
            extends_subset,
        ) in transformation_map.items():

            if not transformation_subset:
                continue

            q = f"""
            UNWIND {cypher_list_from_scoped_keys(transformation_subset)} as sk
            MATCH (n:{node_type} {{`_scoped_key`: sk}})
            RETURN n
            """

            results = self.execute_query(q)

            transformation_nodes = {}
            for record in results.records:
                node = record_data_to_node(record["n"])
                transformation_nodes[node["_scoped_key"]] = node

            for _transformation, _extends in zip(transformation_subset, extends_subset):

                scope = transformation.scope

                _task = Task(
                    creator=creator,
                    extends=str(_extends) if _extends is not None else None,
                )
                _, task_node, scoped_key = self._gufe_to_subgraph(
                    _task.to_shallow_dict(),
                    labels=["GufeTokenizable", _task.__class__.__name__],
                    gufe_key=_task.key,
                    scope=scope,
                )

                sks.append(scoped_key)

                if _extends is not None:

                    extends_task_node, extends_transformation_sk = extends_nodes[
                        str(_extends)
                    ]

                    if extends_transformation_sk != str(_transformation):
                        raise ValueError(
                            f"{_extends} extends a Transformation other than {_transformation}"
                        )

                    subgraph |= Relationship.type("EXTENDS")(
                        task_node,
                        extends_task_node,
                        _org=scope.org,
                        _campaign=scope.campaign,
                        _project=scope.project,
                    )

                subgraph |= Relationship.type("PERFORMS")(
                    task_node,
                    transformation_nodes[str(_transformation)],
                    _org=scope.org,
                    _campaign=scope.campaign,
                    _project=scope.project,
                )

        with self.transaction() as tx:
            merge_subgraph(tx, subgraph, "GufeTokenizable", "_scoped_key")

        return sks

    def create_task(
        self,
        transformation: ScopedKey,
        extends: Optional[ScopedKey] = None,
        creator: Optional[str] = None,
    ) -> ScopedKey:
        """Create a single Task for a Transformation.

        This is a convenience method that wraps around the more general
        `create_tasks` method.

        """
        return self.create_tasks(
            [transformation],
            extends=[extends] if extends is not None else [None],
            creator=creator,
        )[0]

    def query_tasks(self, *, status=None, key=None, scope: Scope = Scope()):
        """Query for `Task`\s matching given attributes."""
        additional = {"status": status}
        return self._query(qualname="Task", additional=additional, key=key, scope=scope)

    def get_network_tasks(
        self, network: ScopedKey, status: Optional[TaskStatusEnum] = None
    ) -> List[ScopedKey]:
        """List ScopedKeys for all Tasks associated with the given AlchemicalNetwork."""
        q = f"""
        MATCH (an:AlchemicalNetwork {{_scoped_key: "{network}"}})-[:DEPENDS_ON]->(tf:Transformation|NonTransformation),
              (tf)<-[:PERFORMS]-(t:Task)
        """

        if status is not None:
            q += f"""
            WHERE t.status = '{status.value}'
            """

        q += """
        WITH t._scoped_key as sk
        RETURN sk
        """
        sks = []
        with self.transaction() as tx:
            res = tx.run(q)
            for rec in res:
                sks.append(rec["sk"])

        return [ScopedKey.from_str(sk) for sk in sks]

    def get_task_networks(self, task: ScopedKey) -> List[ScopedKey]:
        """List ScopedKeys for AlchemicalNetworks associated with the given Task."""
        q = f"""
        MATCH (t:Task {{_scoped_key: '{task}'}})-[:PERFORMS]->(tf:Transformation|NonTransformation),
              (tf)<-[:DEPENDS_ON]-(an:AlchemicalNetwork)
        WITH an._scoped_key as sk
        RETURN sk
        """
        sks = []
        with self.transaction() as tx:
            res = tx.run(q)
            for rec in res:
                sks.append(rec["sk"])

        return [ScopedKey.from_str(sk) for sk in sks]

    def get_transformation_tasks(
        self,
        transformation: ScopedKey,
        extends: Optional[ScopedKey] = None,
        return_as: str = "list",
        status: Optional[TaskStatusEnum] = None,
    ) -> Union[List[ScopedKey], Dict[ScopedKey, Optional[ScopedKey]]]:
        """Get all Tasks that perform the given Transformation.

        If a Task ScopedKey is given for `extends`, then only those Tasks
        that follow via any number of EXTENDS relationships will be returned.

        `return_as` takes either `list` or `graph` as input.
        `graph` will yield a dict mapping each Task's ScopedKey (as keys) to
        the Task ScopedKey it extends (as values).

        Parameters
        ----------
        transformation
            ScopedKey of the Transformation to retrieve Tasks for.
        extends

        """
        q = f"""
        MATCH (trans:Transformation|NonTransformation {{_scoped_key: '{transformation}'}})<-[:PERFORMS]-(task:Task)
        """

        if status is not None:
            q += f"""
            WHERE task.status = '{status.value}'
            """

        if extends:
            q += f"""
            MATCH (trans)<-[:PERFORMS]-(extends:Task {{_scoped_key: '{extends}'}})
            WHERE (task)-[:EXTENDS*]->(extends)
            RETURN task
            """
        else:
            q += """
            RETURN task
            """

        with self.transaction() as tx:
            res = tx.run(q).to_eager_result()

        tasks = []
        for record in res.records:
            tasks.append(record["task"])

        if return_as == "list":
            return [ScopedKey.from_str(t["_scoped_key"]) for t in tasks]
        elif return_as == "graph":
            return {
                ScopedKey.from_str(t["_scoped_key"]): (
                    ScopedKey.from_str(t["extends"])
                    if t["extends"] is not None
                    else None
                )
                for t in tasks
            }

    def get_task_transformation(
        self,
        task: ScopedKey,
        return_gufe=True,
    ) -> Union[
        Tuple[Transformation, Optional[ProtocolDAGResultRef]],
        Tuple[ScopedKey, Optional[ScopedKey]],
    ]:
        """Get the `Transformation` and `ProtocolDAGResultRef` to extend from (if
        present) for the given `Task`.

        If `return_gufe` is `True`, returns actual `Transformation` and
        `ProtocolDAGResultRef` object (`None` if not present); if `False`, returns
        `ScopedKey`\s for these instead.

        """
        q = f"""
        MATCH (task:Task {{_scoped_key: "{task}"}})-[:PERFORMS]->(trans:Transformation|NonTransformation)
        OPTIONAL MATCH (task)-[:EXTENDS]->(prev:Task)-[:RESULTS_IN]->(result:ProtocolDAGResultRef)
        RETURN trans, result
        """

        with self.transaction() as tx:
            res = tx.run(q).to_eager_result()

        transformations = []
        results = []
        for record in res.records:
            transformations.append(record["trans"])
            results.append(record["result"])

        if len(transformations) == 0 or len(results) == 0:
            raise KeyError("No such object in database")
        elif len(transformations) > 1 or len(results) > 1:
            raise Neo4JStoreError(
                "More than one such object in database; this should not be possible"
            )

        transformation = ScopedKey.from_str(transformations[0]["_scoped_key"])

        protocoldagresultref = (
            ScopedKey.from_str(results[0]["_scoped_key"])
            if results[0] is not None
            else None
        )

        if return_gufe:
            return (
                self.get_gufe(transformation),
                (
                    self.get_gufe(protocoldagresultref)
                    if protocoldagresultref is not None
                    else None
                ),
            )

        return transformation, protocoldagresultref

    def set_tasks(
        self,
        transformation: ScopedKey,
        extends: Optional[Task] = None,
        count: int = 1,
    ) -> ScopedKey:
        """Set a fixed number of Tasks against the given Transformation if not
        already present.

        Note: Tasks created by this method are not added to any TaskHubs.

        Parameters
        ----------
        transformation
            The Transformation to compute.
        scope
            The scope the Transformation is in; ignored if `transformation` is a ScopedKey.
        extends
            The Task to use as a starting point for this Task.
            Will use the `ProtocolDAGResult` from the given Task as the
            `extends` input for the Task's eventual call to `Protocol.create`.
        count
            The total number of tasks that should exist corresponding to the
            specified `transformation`, `scope`, and `extends`.
        """
        raise NotImplementedError
        # TODO: finish this one out when we have a reasonable approach to locking
        # too hard to perform in a single Cypher query; unclear how to create many nodes in a loop
        transformation_node = self._get_node_from_obj_or_sk(
            transformation, Transformation, scope
        )

    def set_task_priority(
        self, tasks: List[ScopedKey], priority: int
    ) -> List[Optional[ScopedKey]]:
        """Set the priority of a list of Tasks.

        Parameters
        ----------
        tasks
            The list of Tasks to set the priority of.
        priority
            The priority to set the Tasks to.

        Returns
        -------
        List[Optional[ScopedKey]]
            A list of the Task ScopedKeys for which priority was changed; `None`
            is given for any Tasks for which the priority could not be changed.
        """
        if not (1 <= priority <= 2**63 - 1):
            raise ValueError("priority must be between 1 and 2**63 - 1, inclusive")

        with self.transaction() as tx:
            q = """
            WITH $scoped_keys AS batch
            UNWIND batch AS scoped_key

            OPTIONAL MATCH (t:Task {_scoped_key: scoped_key})
            SET t.priority = $priority

            RETURN scoped_key, t
            """
            res = tx.run(
                q, scoped_keys=[str(t) for t in tasks], priority=priority
            ).to_eager_result()

        task_results = []
        for record in res.records:
            task_i = record["t"]
            scoped_key = record["scoped_key"]

            # catch missing tasks
            if task_i is None:
                task_results.append(None)
            else:
                task_results.append(ScopedKey.from_str(scoped_key))
        return task_results

    def get_task_priority(self, tasks: List[ScopedKey]) -> List[Optional[int]]:
        """Get the priority of a list of Tasks.

        Parameters
        ----------
        tasks
            The list of Tasks to get the priority for.

        Returns
        -------
        List[Optional[int]]
            A list of priorities in the same order as the provided Tasks.
            If an element is ``None``, the Task could not be found.
        """
        with self.transaction() as tx:
            q = """
            WITH $scoped_keys AS batch
            UNWIND batch AS scoped_key
            OPTIONAL MATCH (t:Task)
            WHERE t._scoped_key = scoped_key
            RETURN t.priority as priority
            """
            res = tx.run(q, scoped_keys=[str(t) for t in tasks])
            priorities = [rec["priority"] for rec in res]

        return priorities

    def delete_task(
        self,
        task: ScopedKey,
    ) -> Task:
        """Remove a compute Task from a Transformation.

        This will also remove the Task from all TaskHubs it is a part of.

        This method is intended for administrator use; generally Tasks should
        instead have their tasks set to 'deleted' and retained.

        """
        raise NotImplementedError

    def get_scope_status(
        self,
        scope: Scope,
        network_state: Optional[Union[NetworkStateEnum, str]] = NetworkStateEnum.active,
    ) -> Dict[str, int]:
        """Return status counts for all Tasks within the given Scope.

        Parameters
        ----------
        scope
            Scope to get status for; may be non-specific.
        network_state
            Network state to restrict status returns for; may be a regex pattern.

        """

        properties = {
            "_org": scope.org,
            "_campaign": scope.campaign,
            "_project": scope.project,
        }

        prop_string = ", ".join(
            "{}: '{}'".format(key, value)
            for key, value in properties.items()
            if value is not None
        )

        if isinstance(network_state, NetworkStateEnum):
            network_state = network_state.value

        if network_state is None:
            network_state = ".*"

        q = f"""
        MATCH (n:Task {{{prop_string}}})-[:PERFORMS]->(:Transformation|NonTransformation)<-[:DEPENDS_ON]-(:AlchemicalNetwork)<-[:MARKS]-(nm:NetworkMark)
        WHERE nm.state =~ $state_pattern
        RETURN n.status AS status, count(DISTINCT n) as counts
        """
        with self.transaction() as tx:
            res = tx.run(q, state_pattern=network_state)
            counts = {rec["status"]: rec["counts"] for rec in res}

        return counts

    def get_network_status(self, networks: List[ScopedKey]) -> List[Dict[str, int]]:
        """Return status counts for all Tasks associated with the given AlchemicalNetworks."""
        q = f"""
        UNWIND {cypher_list_from_scoped_keys(networks)} as network
        MATCH (an:AlchemicalNetwork {{_scoped_key: network}})-[:DEPENDS_ON]->(tf:Transformation|NonTransformation),
              (tf)<-[:PERFORMS]-(t:Task)
        RETURN an._scoped_key AS sk, t.status AS status, count(t) as counts
        """

        network_data = {str(network_sk): {} for network_sk in networks}
        for rec in self.execute_query(q).records:
            sk = rec["sk"]
            status = rec["status"]
            counts = rec["counts"]
            network_data[sk][status] = counts

        return [network_data[str(an)] for an in networks]

    def get_transformation_status(self, transformation: ScopedKey) -> Dict[str, int]:
        """Return status counts for all Tasks associated with the given Transformation."""
        q = f"""
        MATCH (:Transformation|NonTransformation {{_scoped_key: "{transformation}"}})<-[:PERFORMS]-(t:Task)
        RETURN t.status AS status, count(t) as counts
        """
        with self.transaction() as tx:
            res = tx.run(q)
            counts = {rec["status"]: rec["counts"] for rec in res}

        return counts

    def set_task_result(
        self, task: ScopedKey, protocoldagresultref: ProtocolDAGResultRef
    ) -> ScopedKey:
        """Set a `ProtocolDAGResultRef` pointing to a `ProtocolDAGResult` for the given `Task`."""

        if task.qualname != "Task":
            raise ValueError("`task` ScopedKey does not correspond to a `Task`")

        scope = task.scope
        task_node = self._get_node(task)

        subgraph, protocoldagresultref_node, scoped_key = self._gufe_to_subgraph(
            protocoldagresultref.to_shallow_dict(),
            labels=["GufeTokenizable", protocoldagresultref.__class__.__name__],
            gufe_key=protocoldagresultref.key,
            scope=scope,
        )

        subgraph = subgraph | Relationship.type("RESULTS_IN")(
            task_node,
            protocoldagresultref_node,
            _org=scope.org,
            _campaign=scope.campaign,
            _project=scope.project,
        )

        with self.transaction() as tx:
            merge_subgraph(tx, subgraph, "GufeTokenizable", "_scoped_key")

        return scoped_key

    def get_task_results(self, task: ScopedKey) -> List[ProtocolDAGResultRef]:
        # get all task result protocoldagresultrefs corresponding to given task
        # returned in no particular order
        q = """
        MATCH (task:Task {_scoped_key: $scoped_key}),
              (task)-[:RESULTS_IN]->(res:ProtocolDAGResultRef)
        WHERE res.ok = true
        WITH res._scoped_key as sk
        RETURN DISTINCT sk
        """
        return self._get_protocoldagresultrefs(q, task)

    def get_task_failures(self, task: ScopedKey) -> List[ProtocolDAGResultRef]:
        # get all task failure protocoldagresultrefs corresponding to given task
        # returned in no particular order
        q = """
        MATCH (task:Task {_scoped_key: $scoped_key}),
              (task)-[:RESULTS_IN]->(res:ProtocolDAGResultRef)
        WHERE res.ok = false
        WITH res._scoped_key as sk
        RETURN DISTINCT sk
        """
        return self._get_protocoldagresultrefs(q, task)

    def add_protocol_dag_result_ref_tracebacks(
        self,
        protocol_unit_failures: List[ProtocolUnitFailure],
        protocol_dag_result_ref_scoped_key: ScopedKey,
    ):
        subgraph = Subgraph()

        with self.transaction() as tx:

            query = """
            MATCH (pdrr:ProtocolDAGResultRef {`_scoped_key`: $protocol_dag_result_ref_scoped_key})
            RETURN pdrr
            """

            pdrr_result = tx.run(
                query,
                protocol_dag_result_ref_scoped_key=str(
                    protocol_dag_result_ref_scoped_key
                ),
            ).to_eager_result()

            try:
                protocol_dag_result_ref_node = record_data_to_node(
                    pdrr_result.records[0]["pdrr"]
                )
            except IndexError:
                raise KeyError("Could not find ProtocolDAGResultRef in database.")

            failure_keys = []
            source_keys = []
            tracebacks = []

            for puf in protocol_unit_failures:
                failure_keys.append(puf.key)
                source_keys.append(puf.source_key)
                tracebacks.append(puf.traceback)

            traceback = Tracebacks(tracebacks, source_keys, failure_keys)

            _, traceback_node, _ = self._gufe_to_subgraph(
                traceback.to_shallow_dict(),
                labels=["GufeTokenizable", traceback.__class__.__name__],
                gufe_key=traceback.key,
                scope=protocol_dag_result_ref_scoped_key.scope,
            )

            subgraph |= Relationship.type("DETAILS")(
                traceback_node,
                protocol_dag_result_ref_node,
            )

            merge_subgraph(tx, subgraph, "GufeTokenizable", "_scoped_key")

    def set_task_status(
        self, tasks: List[ScopedKey], status: TaskStatusEnum, raise_error: bool = False
    ) -> List[Optional[ScopedKey]]:
        """Set the status of a list of Tasks.

        This is a master method that calls the appropriate method for the
        status.

        Parameters
        ----------
        tasks
            The list of Tasks to set the status of.
        status
            The status to set the Task to.
        raise_error
            If `True`, raise a `ValueError` if the status of a given Task cannot be changed.

        Returns
        -------
        List[Optional[ScopedKey]]
            A list of the Task ScopedKeys for which status was changed; `None`
            is given for any Tasks for which the status could not be changed.

        """
        method = getattr(self, f"set_task_{status.value}")
        return method(tasks, raise_error=raise_error)

    def get_task_status(self, tasks: List[ScopedKey]) -> List[TaskStatusEnum]:
        """Get the status of a list of Tasks.

        Parameters
        ----------
        tasks
            The list of Tasks to get the status for.

        Returns
        -------
        Dict[ScopedKey,TaskStatusEnum]
            A dictionary of Tasks and their statuses.
        """
        statuses = []
        with self.transaction() as tx:
            q = """
            WITH $scoped_keys AS batch
            UNWIND batch AS scoped_key
            OPTIONAL MATCH (t:Task)
            WHERE t._scoped_key = scoped_key
            RETURN t.status as status
            """
            res = tx.run(q, scoped_keys=[str(t) for t in tasks])

            for rec in res:
                status = rec["status"]
                statuses.append(TaskStatusEnum(status) if status is not None else None)

        return statuses

    def _set_task_status(
        self, tasks, q: str, err_msg_func, raise_error
    ) -> List[Optional[ScopedKey]]:
        tasks_statused = []
        with self.transaction() as tx:
            res = tx.run(q, scoped_keys=[str(t) for t in tasks])

            for record in res:
                task_i = record["t"]
                task_set = record["t_"]
                scoped_key = record["scoped_key"]

                if task_set is None:
                    if raise_error:
                        status = task_i["status"]
                        raise ValueError(err_msg_func(scoped_key, status))
                    tasks_statused.append(None)
                elif task_i is None:
                    if raise_error:
                        raise ValueError("No such task {t}")
                    tasks_statused.append(None)
                else:
                    tasks_statused.append(ScopedKey.from_str(scoped_key))

        return tasks_statused

    def set_task_waiting(
        self, tasks: List[ScopedKey], raise_error: bool = False
    ) -> List[Optional[ScopedKey]]:
        """Set the status of a list of Tasks to `waiting`.

        Only Tasks with status `error` or `running` can be set to `waiting`.

        """

        q = """
        WITH $scoped_keys AS batch
        UNWIND batch AS scoped_key

        OPTIONAL MATCH (t:Task {_scoped_key: scoped_key})

        OPTIONAL MATCH (t_:Task {_scoped_key: scoped_key})
        WHERE t_.status IN ['waiting', 'running', 'error']
        SET t_.status = 'waiting'

        WITH scoped_key, t, t_

        // if we changed the status to waiting,
        // drop CLAIMS relationship
        OPTIONAL MATCH (t_)<-[cl:CLAIMS]-(csreg:ComputeServiceRegistration)
        DELETE cl

        RETURN scoped_key, t, t_
        """

        def err_msg(t, status):
            return f"Cannot set task {t} with current status: {status} to `waiting` as it is not currently `error` or `running`."

        return self._set_task_status(tasks, q, err_msg, raise_error=raise_error)

    def set_task_running(
        self, tasks: List[ScopedKey], raise_error: bool = False
    ) -> List[Optional[ScopedKey]]:
        """Set the status of a list of Tasks to `running`.

        Only Tasks with status `waiting` can be set to `running`.

        """

        q = """
        WITH $scoped_keys AS batch
        UNWIND batch AS scoped_key

        OPTIONAL MATCH (t:Task {_scoped_key: scoped_key})

        OPTIONAL MATCH (t_:Task {_scoped_key: scoped_key})
        WHERE t_.status IN ['running', 'waiting']
        SET t_.status = 'running'

        RETURN scoped_key, t, t_
        """

        def err_msg(t, status):
            return f"Cannot set task {t} with current status: {status} to `running` as it is not currently `waiting`."

        return self._set_task_status(tasks, q, err_msg, raise_error=raise_error)

    def set_task_complete(
        self, tasks: List[ScopedKey], raise_error: bool = False
    ) -> List[Optional[ScopedKey]]:
        """Set the status of a list of Tasks to `complete`.

        Only `running` Tasks can be set to `complete`.

        """

        q = """
        WITH $scoped_keys AS batch
        UNWIND batch AS scoped_key

        OPTIONAL MATCH (t:Task {_scoped_key: scoped_key})

        OPTIONAL MATCH (t_:Task {_scoped_key: scoped_key})
        WHERE t_.status IN ['complete', 'running']
        SET t_.status = 'complete'

        WITH scoped_key, t, t_

        // if we changed the status to complete,
        // drop all ACTIONS relationships
        OPTIONAL MATCH (t_)<-[ar:ACTIONS]-(th:TaskHub)
        OPTIONAL MATCH (t_)<-[applies:APPLIES]-(:TaskRestartPattern)
        DELETE ar
        DELETE applies

        WITH scoped_key, t, t_

        // if we changed the status to complete,
        // drop CLAIMS relationship
        OPTIONAL MATCH (t_)<-[cl:CLAIMS]-(csreg:ComputeServiceRegistration)
        DELETE cl

        RETURN scoped_key, t, t_
        """

        def err_msg(t, status):
            return f"Cannot set task {t} with current status: {status} to `complete` as it is not currently `running`."

        return self._set_task_status(tasks, q, err_msg, raise_error=raise_error)

    def set_task_error(
        self, tasks: List[ScopedKey], raise_error: bool = False
    ) -> List[Optional[ScopedKey]]:
        """Set the status of a list of Tasks to `error`.

        Only `running` Tasks can be set to `error`.

        """

        q = """
        WITH $scoped_keys AS batch
        UNWIND batch AS scoped_key

        OPTIONAL MATCH (t:Task {_scoped_key: scoped_key})

        OPTIONAL MATCH (t_:Task {_scoped_key: scoped_key})
        WHERE t_.status IN ['error', 'running']
        SET t_.status = 'error'

        WITH scoped_key, t, t_

        // if we changed the status to error,
        // drop CLAIMS relationship
        OPTIONAL MATCH (t_)<-[cl:CLAIMS]-(csreg:ComputeServiceRegistration)
        DELETE cl

        RETURN scoped_key, t, t_
        """

        def err_msg(t, status):
            return f"Cannot set task {t} with current status: {status} to `error` as it is not currently `running`."

        return self._set_task_status(tasks, q, err_msg, raise_error=raise_error)

    def set_task_invalid(
        self, tasks: List[ScopedKey], raise_error: bool = False
    ) -> List[Optional[ScopedKey]]:
        """Set the status of a list of Tasks to `invalid`.

        Any Task can be set to `invalid`; an `invalid` Task cannot change to
        any other status.

        """

        # set the status and delete the ACTIONS relationship
        # make sure we follow the extends chain and set all tasks to invalid
        # and remove actions relationships
        q = """
        WITH $scoped_keys AS batch
        UNWIND batch AS scoped_key

        OPTIONAL MATCH (t:Task {_scoped_key: scoped_key})

        OPTIONAL MATCH (t_:Task {_scoped_key: scoped_key})
        WHERE NOT t_.status IN ['deleted']
        SET t_.status = 'invalid'

        WITH scoped_key, t, t_

        OPTIONAL MATCH (t_)<-[er:EXTENDS*]-(extends_task:Task)
        SET extends_task.status = 'invalid'

        WITH scoped_key, t, t_, extends_task

        OPTIONAL MATCH (t_)<-[ar:ACTIONS]-(th:TaskHub)
        OPTIONAL MATCH (extends_task)<-[are:ACTIONS]-(th:TaskHub)
        OPTIONAL MATCH (t_)<-[applies:APPLIES]-(:TaskRestartPattern)

        DELETE ar
        DELETE are
        DELETE applies

        WITH scoped_key, t, t_

        // drop CLAIMS relationship if present
        OPTIONAL MATCH (t_)<-[cl:CLAIMS]-(csreg:ComputeServiceRegistration)
        DELETE cl

        RETURN scoped_key, t, t_
        """

        def err_msg(t, status):
            return f"Cannot set task {t} with current status: {status} to `invalid` as it is `deleted`."

        return self._set_task_status(tasks, q, err_msg, raise_error=raise_error)

    def set_task_deleted(
        self, tasks: List[ScopedKey], raise_error: bool = False
    ) -> List[Optional[ScopedKey]]:
        """Set the status of a list of Tasks to `deleted`.

        Any Task can be set to `deleted`; a `deleted` Task cannot change to
        any other status.

        """

        # set the status and delete the ACTIONS relationship
        # make sure we follow the extends chain and set all tasks to deleted
        # and remove actions relationships
        q = """
        WITH $scoped_keys AS batch
        UNWIND batch AS scoped_key

        OPTIONAL MATCH (t:Task {_scoped_key: scoped_key})

        OPTIONAL MATCH (t_:Task {_scoped_key: scoped_key})
        WHERE NOT t_.status IN ['invalid']
        SET t_.status = 'deleted'

        WITH scoped_key, t, t_

        OPTIONAL MATCH (t_)<-[er:EXTENDS*]-(extends_task:Task)
        SET extends_task.status = 'deleted'

        WITH scoped_key, t, t_, extends_task

        OPTIONAL MATCH (t_)<-[ar:ACTIONS]-(th:TaskHub)
        OPTIONAL MATCH (extends_task)<-[are:ACTIONS]-(th:TaskHub)
        OPTIONAL MATCH (t_)<-[applies:APPLIES]-(:TaskRestartPattern)

        DELETE ar
        DELETE are
        DELETE applies

        WITH scoped_key, t, t_

        // drop CLAIMS relationship if present
        OPTIONAL MATCH (t_)<-[cl:CLAIMS]-(csreg:ComputeServiceRegistration)
        DELETE cl

        RETURN scoped_key, t, t_
        """

        def err_msg(t, status):
            return f"Cannot set task {t} with current status: {status} to `deleted` as it is `invalid`."

        return self._set_task_status(tasks, q, err_msg, raise_error=raise_error)

    ## task restart policy

    def add_task_restart_patterns(
        self, taskhub: ScopedKey, patterns: list[str], number_of_retries: int
    ):
        """Add a list of restart policy patterns to a `TaskHub` along with the number of retries allowed.

        Parameters
        ----------
        taskhub : ScopedKey
            TaskHub for the restart patterns to enforce.
        patterns: list[str]
            Regular expression patterns that will be compared to tracebacks returned by ProtocolUnitFailures.
        number_of_retries: int
            The number of times the given patterns will apply to a single Task, attempts to restart beyond
            this value will result in a canceled Task with an error status.

        Raises
        ------
        KeyError
            Raised when the provided TaskHub ScopedKey cannot be associated with a TaskHub in the database.
        """

        # get taskhub node
        q = """
        MATCH (th:TaskHub {`_scoped_key`: $taskhub})
        RETURN th
        """
        results = self.execute_query(q, taskhub=str(taskhub))

        # raise error if taskhub not found
        if not results.records:
            raise KeyError("No such TaskHub in the database")

        record_data = results.records[0]["th"]
        taskhub_node = record_data_to_node(record_data)
        scope = taskhub.scope

        with self.transaction() as tx:
            actioned_tasks_query = """
            MATCH (taskhub: TaskHub {`_scoped_key`: $taskhub_scoped_key})-[:ACTIONS]->(task: Task)
            RETURN task
            """

            actioned_task_records = (
                tx.run(actioned_tasks_query, taskhub_scoped_key=str(taskhub))
                .to_eager_result()
                .records
            )

            subgraph = Subgraph()

            actioned_task_nodes = []

            for actioned_tasks_record in actioned_task_records:
                actioned_task_nodes.append(
                    record_data_to_node(actioned_tasks_record["task"])
                )

            for pattern in patterns:
                task_restart_pattern = TaskRestartPattern(
                    pattern,
                    max_retries=number_of_retries,
                    taskhub_scoped_key=str(taskhub),
                )

                _, task_restart_pattern_node, scoped_key = self._gufe_to_subgraph(
                    task_restart_pattern.to_shallow_dict(),
                    labels=["GufeTokenizable", task_restart_pattern.__class__.__name__],
                    gufe_key=task_restart_pattern.key,
                    scope=scope,
                )

                subgraph |= Relationship.type("ENFORCES")(
                    task_restart_pattern_node,
                    taskhub_node,
                    _org=scope.org,
                    _campaign=scope.campaign,
                    _project=scope.project,
                )

                for actioned_task_node in actioned_task_nodes:
                    subgraph |= Relationship.type("APPLIES")(
                        task_restart_pattern_node,
                        actioned_task_node,
                        num_retries=0,
                    )
            merge_subgraph(tx, subgraph, "GufeTokenizable", "_scoped_key")

            actioned_task_scoped_keys: List[ScopedKey] = []

            for actioned_task_record in actioned_task_records:
                actioned_task_scoped_keys.append(
                    ScopedKey.from_str(actioned_task_record["task"]["_scoped_key"])
                )

            self.resolve_task_restarts(actioned_task_scoped_keys, tx=tx)

    def remove_task_restart_patterns(self, taskhub: ScopedKey, patterns: list[str]):
        """Remove a list of restart patterns enforcing a TaskHub from the database.

        Parameters
        ----------
        taskhub: ScopedKey
            The ScopedKey of the TaskHub that the patterns enforce.
        patterns: list[str]
            The patterns to remove. Patterns not enforcing the TaskHub are ignored.
        """
        q = """
        UNWIND $patterns AS pattern

        MATCH (trp: TaskRestartPattern {pattern: pattern, taskhub_scoped_key: $taskhub_scoped_key})

        DETACH DELETE trp
        """

        self.execute_query(q, patterns=patterns, taskhub_scoped_key=str(taskhub))

    def clear_task_restart_patterns(self, taskhub: ScopedKey):
        """Clear all restart patterns from a TaskHub.

        Parameters
        ----------
        taskhub: ScopedKey
            The ScopedKey of the TaskHub to clear of restart patterns.
        """
        q = """
        MATCH (trp: TaskRestartPattern {taskhub_scoped_key: $taskhub_scoped_key})
        DETACH DELETE trp
        """
        self.execute_query(q, taskhub_scoped_key=str(taskhub))

    def set_task_restart_patterns_max_retries(
        self,
        taskhub_scoped_key: ScopedKey,
        patterns: list[str],
        max_retries: int,
    ):
        """Set the maximum number of retries of a pattern enforcing a TaskHub.

        Parameters
        ----------
        taskhub_scoped_key: ScopedKey
            The ScopedKey of the TaskHub that the patterns enforce.
        patterns: list[str]
            The patterns to change the maximum retries value for.
        max_retries: int
            The new maximum retries value.
        """
        query = """
        UNWIND $patterns AS pattern
        MATCH (trp: TaskRestartPattern {pattern: pattern, taskhub_scoped_key: $taskhub_scoped_key})
        SET trp.max_retries = $max_retries
        """

        self.execute_query(
            query,
            patterns=patterns,
            taskhub_scoped_key=str(taskhub_scoped_key),
            max_retries=max_retries,
        )

    # TODO: validation of taskhubs variable, will fail in weird ways if not enforced
    def get_task_restart_patterns(
        self, taskhubs: list[ScopedKey]
    ) -> dict[ScopedKey, set[tuple[str, int]]]:
        """For a list of TaskHub ScopedKeys, get the associated restart patterns along with the maximum number of retries for each pattern.

        Parameters
        ----------
        taskhubs: list[ScopedKey]
            The ScopedKeys of the TaskHubs to get the restart patterns of.

        Returns
        -------
        dict[ScopedKey, set[tuple[str, int]]]
            A dictionary containing whose keys are the ScopedKeys of the TaskHubs provided and whose
            values are a set of tuples containing the patterns enforcing each TaskHub along with their
            associated maximum number of retries.
        """

        q = """
            UNWIND $taskhub_scoped_keys as taskhub_scoped_key
            MATCH (trp: TaskRestartPattern)-[ENFORCES]->(th: TaskHub {`_scoped_key`: taskhub_scoped_key})
            RETURN th, trp
        """

        records = self.execute_query(
            q, taskhub_scoped_keys=list(map(str, taskhubs))
        ).records

        data: dict[ScopedKey, set[tuple[str, int]]] = {
            taskhub: set() for taskhub in taskhubs
        }

        for record in records:
            pattern = record["trp"]["pattern"]
            max_retries = record["trp"]["max_retries"]
            taskhub_sk = ScopedKey.from_str(record["th"]["_scoped_key"])
            data[taskhub_sk].add((pattern, max_retries))

        return data

    @chainable
    def resolve_task_restarts(self, task_scoped_keys: Iterable[ScopedKey], *, tx=None):
        """Determine whether or not Tasks need to be restarted or canceled and perform that action.

        Parameters
        ----------
        task_scoped_keys: Iterable[ScopedKey]
            An iterable of Task ScopedKeys that need to be resolved. Tasks without the error status
            are filtered out and ignored.
        """

        # Given the scoped keys of a list of Tasks, find all tasks that have an
        # error status and have a TaskRestartPattern applied. A subquery is executed
        # to optionally get the latest traceback associated with the task
        query = """
        UNWIND $task_scoped_keys AS task_scoped_key
        MATCH (task:Task {status: $error, `_scoped_key`: task_scoped_key})<-[app:APPLIES]-(trp:TaskRestartPattern)-[:ENFORCES]->(taskhub:TaskHub)
        CALL {
            WITH task
            OPTIONAL MATCH (task:Task)-[:RESULTS_IN]->(pdrr:ProtocolDAGResultRef)<-[:DETAILS]-(tracebacks:Tracebacks)
            RETURN tracebacks
            ORDER BY pdrr.datetime_created DESCENDING
            LIMIT 1
        }
        WITH task, tracebacks, trp, app, taskhub
        RETURN task, tracebacks, trp, app, taskhub
        """

        results = tx.run(
            query,
            task_scoped_keys=list(map(str, task_scoped_keys)),
            error=TaskStatusEnum.error.value,
        ).to_eager_result()

        if not results:
            return

        # iterate over all of the results to determine if an applied pattern needs
        # to be iterated or if the task needs to be cancelled outright

        # Keep track of which task/taskhub pairs would need to be canceled
        # None => the pair never had a matching restart pattern
        # True => at least one patterns max_retries was exceeded
        # False => at least one regex matched, but no pattern max_retries were exceeded
        cancel_map: dict[Tuple[str, str], Optional[bool]] = {}
        to_increment: List[Tuple[str, str]] = []
        all_task_taskhub_pairs: set[Tuple[str, str]] = set()
        for record in results.records:
            task_restart_pattern = record["trp"]
            applies_relationship = record["app"]
            task = record["task"]
            taskhub = record["taskhub"]
            _tracebacks = record["tracebacks"]

            task_taskhub_tuple = (task["_scoped_key"], taskhub["_scoped_key"])

            all_task_taskhub_pairs.add(task_taskhub_tuple)

            # TODO: remove in v1.0.0
            # tasks that errored, prior to the indtroduction of task restart policies will have no tracebacks in the database
            if _tracebacks is None:
                cancel_map[task_taskhub_tuple] = True

            # we have already determined that the task is to be canceled.
            # this is only ever truthy when we say a task needs to be canceled.
            if cancel_map.get(task_taskhub_tuple):
                continue

            num_retries = applies_relationship["num_retries"]
            max_retries = task_restart_pattern["max_retries"]
            pattern = task_restart_pattern["pattern"]
            tracebacks: List[str] = _tracebacks["tracebacks"]

            compiled_pattern = re.compile(pattern)

            if any([compiled_pattern.search(message) for message in tracebacks]):
                if num_retries + 1 > max_retries:
                    cancel_map[task_taskhub_tuple] = True
                else:
                    to_increment.append(
                        (task["_scoped_key"], task_restart_pattern["_scoped_key"])
                    )
                    cancel_map[task_taskhub_tuple] = False

        increment_query = """
        UNWIND $trp_and_task_pairs as pairs
        WITH pairs[0] as task_scoped_key, pairs[1] as task_restart_pattern_scoped_key
        MATCH (:Task {`_scoped_key`: task_scoped_key})<-[app:APPLIES]-(:TaskRestartPattern {`_scoped_key`: task_restart_pattern_scoped_key})
        SET app.num_retries = app.num_retries + 1
        """

        tx.run(increment_query, trp_and_task_pairs=to_increment)

        # cancel all tasks that didn't trigger any restart patterns (None)
        # or exceeded a patterns max_retries value (True)
        cancel_groups: defaultdict[str, list[str]] = defaultdict(list)
        for task_taskhub_pair in all_task_taskhub_pairs:
            cancel_result = cancel_map.get(task_taskhub_pair)
            if cancel_result is True or cancel_result is None:
                cancel_groups[task_taskhub_pair[1]].append(task_taskhub_pair[0])

        for taskhub, tasks in cancel_groups.items():
            self.cancel_tasks(tasks, taskhub, tx=tx)

        # any remaining tasks must then be okay to switch to waiting
        renew_waiting_status_query = """
        UNWIND $task_scoped_keys AS task_scoped_key
        MATCH (task:Task {status: $error, `_scoped_key`: task_scoped_key})<-[app:APPLIES]-(trp:TaskRestartPattern)-[:ENFORCES]->(taskhub:TaskHub)
        SET task.status = $waiting
        """

        tx.run(
            renew_waiting_status_query,
            task_scoped_keys=list(map(str, task_scoped_keys)),
            waiting=TaskStatusEnum.waiting.value,
            error=TaskStatusEnum.error.value,
        )

    ## authentication

    def create_credentialed_entity(self, entity: CredentialedEntity):
        """Create a new credentialed entity, such as a user or compute identity.

        If an entity of this type with the same `identifier` already exists,
        then this will overwrite its properties, including credential.

        """
        node = Node("CredentialedEntity", entity.__class__.__name__, **entity.dict())

        with self.transaction() as tx:
            merge_subgraph(
                tx, Subgraph() | node, entity.__class__.__name__, "identifier"
            )

    def get_credentialed_entity(self, identifier: str, cls: type[CredentialedEntity]):
        """Get an existing credentialed entity, such as a user or compute identity."""
        q = f"""
        MATCH (n:{cls.__name__} {{identifier: '{identifier}'}})
        RETURN n
        """

        with self.transaction() as tx:
            res = tx.run(q).to_eager_result()

        nodes = set()
        for record in res.records:
            nodes.add(record_data_to_node(record["n"]))

        if len(nodes) == 0:
            raise KeyError("No such object in database")
        elif len(nodes) > 1:
            raise Neo4JStoreError(
                "More than one such object in database; this should not be possible"
            )

        return cls(**dict(list(nodes)[0]))

    def list_credentialed_entities(self, cls: type[CredentialedEntity]):
        """Get an existing credentialed entity, such as a user or compute identity."""
        q = f"""
        MATCH (n:{cls.__name__})
        RETURN n
        """
        with self.transaction() as tx:
            res = tx.run(q).to_eager_result()

        nodes = set()
        for record in res.records:
            nodes.add(record_data_to_node(record["n"]))

        return [node["identifier"] for node in nodes]

    def remove_credentialed_identity(
        self, identifier: str, cls: type[CredentialedEntity]
    ):
        """Remove a credentialed entity, such as a user or compute identity."""
        q = f"""
        MATCH (n:{cls.__name__} {{identifier: '{identifier}'}})
        DETACH DELETE n
        """

        with self.transaction() as tx:
            tx.run(q)

    def add_scope(self, identifier: str, cls: type[CredentialedEntity], scope: Scope):
        """Add a scope to the given entity."""

        # n.scopes is always initialized by the pydantic model so no need to check
        # for existence, however, we do need to check that the scope is not already
        # present
        q = f"""
        MATCH (n:{cls.__name__} {{identifier: '{identifier}'}})
        WHERE NONE(x IN n.scopes WHERE x = '{scope}')
        SET n.scopes = n.scopes + '{scope}'
        """

        with self.transaction() as tx:
            tx.run(q)

    def list_scopes(
        self, identifier: str, cls: type[CredentialedEntity]
    ) -> List[Scope]:
        """List all scopes for which the given entity has access."""

        # get the scope properties for the given entity
        q = f"""
        MATCH (n:{cls.__name__} {{identifier: '{identifier}'}})
        RETURN n.scopes as s
        """

        with self.transaction() as tx:
            res = tx.run(q).to_eager_result()

        scopes = []
        for record in res.records:
            scope_rec = record["s"]
            for scope_str in scope_rec:
                scope = Scope.from_str(scope_str)
                scopes.append(scope)
        return scopes

    def remove_scope(
        self, identifier: str, cls: type[CredentialedEntity], scope: Scope
    ):
        """Remove a scope from the given entity."""

        # use a list comprehension to remove the scope from the list
        q = f"""
        MATCH (n:{cls.__name__} {{identifier: '{identifier}'}})
        SET n.scopes = [scope IN n.scopes WHERE scope <> '{scope}']
        """

        with self.transaction() as tx:
            tx.run(q)
