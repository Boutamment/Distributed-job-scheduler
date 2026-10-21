"""Pure DAG validation (no database access)."""
from __future__ import annotations

from collections import deque
from typing import Iterable, Mapping


class DagError(ValueError):
    """The submitted workflow is not a valid DAG."""


def validate(tasks: Iterable[Mapping]) -> list[str]:
    """Validate a workflow and return its task keys in a valid execution order.

    Raises DagError on duplicate keys, unknown or self dependencies, and cycles.
    Uses Kahn's algorithm: if we cannot order every node, the rest sit on a cycle.
    """
    tasks = list(tasks)
    keys = [t["key"] for t in tasks]
    if len(set(keys)) != len(keys):
        dupes = sorted({k for k in keys if keys.count(k) > 1})
        raise DagError(f"duplicate task keys: {', '.join(dupes)}")

    known = set(keys)
    indegree = {k: 0 for k in keys}
    children: dict[str, list[str]] = {k: [] for k in keys}
    for t in tasks:
        for dep in set(t.get("depends_on") or []):
            if dep == t["key"]:
                raise DagError(f"task '{t['key']}' depends on itself")
            if dep not in known:
                raise DagError(f"task '{t['key']}' depends on unknown task '{dep}'")
            indegree[t["key"]] += 1
            children[dep].append(t["key"])

    queue = deque(k for k in keys if indegree[k] == 0)
    order: list[str] = []
    while queue:
        k = queue.popleft()
        order.append(k)
        for child in children[k]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(order) != len(keys):
        stuck = sorted(k for k in keys if indegree[k] > 0)
        raise DagError(f"cycle detected involving: {', '.join(stuck)}")
    return order
