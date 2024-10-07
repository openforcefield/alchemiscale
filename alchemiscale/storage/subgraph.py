from py2neo import Node, Subgraph, Relationship, UniquenessError

from py2neo.cypher import cypher_join
from alchemiscale.storage.cypher import (
    unwind_create_nodes_query,
    unwind_merge_nodes_query,
    unwind_merge_relationships_query,
)

from neo4j import Transaction


# overrides for py2neo comparison and set opeartions
def custom_eq(self, other):
    return self["_scoped_key"] == other["_scoped_key"]


def custom_hash(self):
    # if a scoped key exists, we should always use this
    if self["_scoped_key"]:
        return hash(self["_scoped_key"])
    # added to handle CredentialedUserEntity
    if self["identifier"]:
        return hash(self["identifier"])
    # if all else fails, at least try and sort out the
    # objects themselves
    else:
        return hash(id(self))


Node.__eq__ = custom_eq
Node.__hash__ = custom_hash


def record_data_to_node(node):
    new_node = Node(*node.labels, **node._properties)
    return new_node


def subgraph_from_path_record(path_record):
    path_nodes = set((record_data_to_node(n) for n in path_record.nodes))
    path_rels = set(
        (
            Relationship(
                record_data_to_node(rel.start_node),
                rel.type,
                record_data_to_node(rel.end_node),
                **rel._properties,
            )
            for rel in path_record.relationships
        )
    )

    return Subgraph(path_nodes, path_rels)


# Original code from py2neo, licensed under the Apache License 2.0.
# Modifications:
#     - Removed usage of py2neo database connections to instead use
#       the official neo4j driver
#     - Switched all usage of the id function to elementId
def merge_subgraph(
    transaction: Transaction,
    subgraph: Subgraph,
    primary_label: str,
    primary_key: str,
):
    """Code adapted from the py2neo Subgraph.__db_merge__ method."""
    node_dict = {}
    for node in subgraph.nodes:
        if node.__primarylabel__ is not None:
            p_label = node.__primarylabel__
        elif node.__model__ is not None:
            p_label = node.__model__.__primarylabel__ or primary_label
        else:
            p_label = primary_label

        if node.__primarykey__ is not None:
            p_key = node.__primarykey__
        elif node.__model__ is not None:
            p_key = node.__model__.__primarykey__ or primary_key
        else:
            p_key = primary_key
        key = (p_label, p_key, frozenset(node.labels))
        node_dict.setdefault(key, []).append(node)

    rel_dict = {}
    for relationship in subgraph.relationships:
        key = type(relationship).__name__
        rel_dict.setdefault(key, []).append(relationship)

    for (pl, pk, labels), nodes in node_dict.items():
        if pl is None or pk is None:
            raise ValueError(
                "Primary label and primary key are required for MERGE operation"
            )
        pq = unwind_merge_nodes_query(map(dict, nodes), (pl, pk), labels)
        pq = cypher_join(pq, "RETURN elementId(_)")
        identities = [record[0] for record in transaction.run(*pq)]
        if len(identities) > len(nodes):
            raise UniquenessError(
                "Found %d matching nodes for primary label %r and primary "
                "key %r with labels %r but merging requires no more than "
                "one" % (len(identities), pl, pk, set(labels))
            )

        for i, identity in enumerate(identities):
            node = nodes[i]
            node.identity = identity
            node._remote_labels = labels

    for r_type, relationships in rel_dict.items():
        data = map(
            lambda r: [r.start_node.identity, dict(r), r.end_node.identity],
            relationships,
        )
        pq = unwind_merge_relationships_query(data, r_type)
        pq = cypher_join(pq, "RETURN elementId(_)")

        for i, record in enumerate(transaction.run(*pq)):
            relationship = relationships[i]
            relationship.identity = record[0]


# Original code from py2neo, licensed under the Apache License 2.0.
# Modifications:
#     - Removed usage of py2neo database connections to instead use
#       the official neo4j driver
#     - Switched all usage of the id function to elementId
def create_subgraph(transaction, subgraph):
    """Code adapted from the py2neo Subgraph.__db_create__ method."""
    node_dict = {}
    for node in subgraph.nodes:
        key = frozenset(node.labels)
        node_dict.setdefault(key, []).append(node)

    rel_dict = {}
    for relationship in subgraph.relationships:
        key = type(relationship).__name__
        rel_dict.setdefault(key, []).append(relationship)

    for labels, nodes in node_dict.items():
        pq = unwind_create_nodes_query(list(map(dict, nodes)), labels=labels)
        pq = cypher_join(pq, "RETURN elementId(_)")
        records = transaction.run(*pq)
        for i, record in enumerate(records):
            node = nodes[i]
            node.identity = record[0]
            node._remote_labels = labels
    for r_type, relationships in rel_dict.items():
        data = map(
            lambda r: [r.start_node.identity, dict(r), r.end_node.identity],
            relationships,
        )
        pq = unwind_merge_relationships_query(data, r_type)
        pq = cypher_join(pq, "RETURN elementId(_)")
        for i, record in enumerate(transaction.run(*pq)):
            relationship = relationships[i]
            relationship.identity = record[0]
