from alchemiscale import ScopedKey
from typing import List, Optional


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
