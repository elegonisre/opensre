"""Core graph engine for opensre.

Manages the execution graph of SRE workflow nodes, supporting
registration, traversal, and execution of node steps.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class GraphNode:
    """Represents a single step node in the SRE workflow graph."""

    def __init__(
        self,
        name: str,
        handler: Callable[..., Any],
        description: str = "",
        tags: Optional[List[str]] = None,
    ) -> None:
        self.name = name
        self.handler = handler
        self.description = description
        self.tags: List[str] = tags or []
        self._dependencies: List[str] = []

    def depends_on(self, *node_names: str) -> "GraphNode":
        """Declare dependencies on other nodes by name."""
        self._dependencies.extend(node_names)
        return self

    def run(self, context: Dict[str, Any]) -> Any:
        """Execute this node's handler with the provided context."""
        logger.debug("Running node: %s", self.name)
        return self.handler(context)

    def __repr__(self) -> str:
        return f"GraphNode(name={self.name!r}, deps={self._dependencies})"


class Graph:
    """Directed acyclic graph of SRE workflow nodes.

    Supports node registration, dependency resolution, and
    topological execution ordering.
    """

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._nodes: Dict[str, GraphNode] = {}
        self._adj: Dict[str, List[str]] = defaultdict(list)  # name -> dependents

    def register(self, node: GraphNode) -> None:
        """Register a node in the graph."""
        if node.name in self._nodes:
            raise ValueError(f"Node '{node.name}' is already registered in graph '{self.name}'.")
        self._nodes[node.name] = node
        for dep in node._dependencies:
            self._adj[dep].append(node.name)
        logger.info("Registered node '%s' in graph '%s'", node.name, self.name)

    def get_node(self, name: str) -> GraphNode:
        """Retrieve a registered node by name."""
        try:
            return self._nodes[name]
        except KeyError:
            raise KeyError(f"Node '{name}' not found in graph '{self.name}'.")

    def topological_order(self) -> List[str]:
        """Return node names in a valid topological execution order.

        Uses Kahn's algorithm for topological sorting.

        Raises:
            RuntimeError: If a cycle is detected in the graph.
            KeyError: If a declared dependency is not registered.
        """
        in_degree: Dict[str, int] = {name: 0 for name in self._nodes}
        for node in self._nodes.values():
            for dep in node._dependencies:
                if dep not in self._nodes:
                    raise KeyError(f"Dependency '{dep}' of node '{node.name}' is not registered.")
                in_degree[node.name] += 1

        # Start with all nodes that have no dependencies
        queue: List[str] = [name for name, deg in in_degree.items() if deg == 0]
        # Sort for deterministic ordering when multiple nodes have the same in-degree
        queue.sort()
        order: List[str] = []

        while queue:
            current = queue.pop(0)
            order.append(current)
            for dependent in sorted(self._adj[current]):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(order) != len(self._nodes):
            raise RuntimeError(
                f"Cycle detected in graph '{self.name}'. "
                "Topological ordering is not possible."
            )

        return order
