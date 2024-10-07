from alchemiscale import ScopedKey
from typing import List, Optional

from py2neo.cypher.queries import (
    _create_clause,
    _merge_clause,
    _on_create_set_properties_clause,
    _relationship_data,
    _set_labels_clause,
    _set_properties_clause,
    cypher_escape,
    cypher_join,
)
from py2neo.cypher.queries import NodeKey


def cypher_list_from_scoped_keys(scoped_keys: List[Optional[ScopedKey]]) -> str:
    """Generate a Cypher list structure from a list of ScopedKeys, ignoring NoneType entries.

    Parameters
    ----------
    scoped_keys
        List of ScopedKeys to generate the Cypher list

    Returns
    -------
    str
        Cypher list
    """

    if not isinstance(scoped_keys, list):
        raise ValueError("`scoped_keys` must be a list of ScopedKeys")

    data = []
    for scoped_key in scoped_keys:
        if scoped_key:
            data.append('"' + str(scoped_key) + '"')
    return "[" + ", ".join(data) + "]"


def cypher_or(items):
    return "|".join(items)


# Original code from py2neo, licensed under the Apache License 2.0.
# Modifications by alchemiscale:
#   - switched id function to use elementId
def _match_clause(name, node_key, value, prefix="(", suffix=")"):
    if node_key is None:
        # ... add MATCH by id clause
        return "MATCH %s%s%s WHERE elementId(%s) = %s" % (
            prefix,
            name,
            suffix,
            name,
            value,
        )
    else:
        # ... add MATCH by label/property clause
        nk = NodeKey(node_key)
        n_pk = len(nk.keys())
        if n_pk == 0:
            return "MATCH %s%s%s%s" % (prefix, name, nk.label_string(), suffix)
        elif n_pk == 1:
            return "MATCH %s%s%s {%s:%s}%s" % (
                prefix,
                name,
                nk.label_string(),
                cypher_escape(nk.keys()[0]),
                value,
                suffix,
            )
        else:
            return "MATCH %s%s%s {%s}%s" % (
                prefix,
                name,
                nk.label_string(),
                nk.key_value_string(value, list(range(n_pk))),
                suffix,
            )


# Original code from py2neo, licensed under the Apache License 2.0.
def unwind_merge_nodes_query(data, merge_key, labels=None, keys=None, preserve=None):
    """Generate a parameterised ``UNWIND...MERGE`` query for bulk
    loading nodes into Neo4j.

    :param data:
    :param merge_key:
    :param labels:
    :param keys:
    :param preserve:
        Collection of key names for values that should be protected
        should the node already exist.
    :return: (query, parameters) tuple
    """
    return cypher_join(
        "UNWIND $data AS r",
        _merge_clause("_", merge_key, "r", keys),
        _on_create_set_properties_clause("r", keys, preserve),
        _set_properties_clause("r", keys, exclude_keys=preserve),
        _set_labels_clause(labels),
        data=list(data),
    )


# Original code from py2neo, licensed under the Apache License 2.0.
def unwind_merge_relationships_query(
    data, merge_key, start_node_key=None, end_node_key=None, keys=None, preserve=None
):
    """Generate a parameterised ``UNWIND...MERGE`` query for bulk
    loading relationships into Neo4j.

    :param data:
    :param merge_key: tuple of (rel_type, key1, key2...)
    :param start_node_key:
    :param end_node_key:
    :param keys:
    :param preserve:
        Collection of key names for values that should be protected
        should the relationship already exist.
    :return: (query, parameters) tuple
    """
    return cypher_join(
        "UNWIND $data AS r",
        _match_clause("a", start_node_key, "r[0]"),
        _match_clause("b", end_node_key, "r[2]"),
        _merge_clause("_", merge_key, "r[1]", keys, "(a)-[", "]->(b)"),
        _on_create_set_properties_clause("r[1]", keys, preserve),
        _set_properties_clause("r[1]", keys, exclude_keys=preserve),
        data=_relationship_data(data),
    )


# Original code from py2neo, licensed under the Apache License 2.0.
def unwind_create_nodes_query(data, labels=None, keys=None):
    """Generate a parameterised ``UNWIND...CREATE`` query for bulk
    loading nodes into Neo4j.

    :param data:
    :param labels:
    :param keys:
    :return: (query, parameters) tuple
    """
    return cypher_join(
        "UNWIND $data AS r",
        _create_clause("_", (tuple(labels or ()),)),
        _set_properties_clause("r", keys),
        data=list(data),
    )
