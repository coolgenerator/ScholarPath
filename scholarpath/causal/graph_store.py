"""Serialization and visualization export for causal DAGs.

Provides conversion between networkx DiGraph and JSON-serializable
formats, Cytoscape.js export for frontend rendering, and graph
diffing for what-if visualization.
"""

from __future__ import annotations

import logging
from typing import Any

import networkx as nx

logger = logging.getLogger(__name__)


def serialize_graph(dag: nx.DiGraph) -> dict[str, Any]:
    """Convert a networkx DiGraph to a JSON-serializable dict.

    Parameters
    ----------
    dag:
        The causal DAG to serialize.

    Returns
    -------
    dict
        ``{nodes: [...], edges: [...], metadata: {...}}`` where each
        node and edge includes all stored attributes.
    """
    nodes: list[dict[str, Any]] = []
    for node_id, attrs in dag.nodes(data=True):
        node_data: dict[str, Any] = {"id": node_id}
        for key, value in attrs.items():
            node_data[key] = _make_serializable(value)
        nodes.append(node_data)

    edges: list[dict[str, Any]] = []
    for source, target, attrs in dag.edges(data=True):
        edge_data: dict[str, Any] = {"source": source, "target": target}
        for key, value in attrs.items():
            edge_data[key] = _make_serializable(value)
        edges.append(edge_data)

    return {
        "nodes": nodes,
        "edges": edges,
        "metadata": {
            "num_nodes": dag.number_of_nodes(),
            "num_edges": dag.number_of_edges(),
            "is_dag": nx.is_directed_acyclic_graph(dag),
        },
    }


def deserialize_graph(data: dict[str, Any]) -> nx.DiGraph:
    """Reconstruct a networkx DiGraph from a serialized dict.

    Parameters
    ----------
    data:
        Dict with ``nodes`` and ``edges`` lists, as produced by
        ``serialize_graph``.

    Returns
    -------
    nx.DiGraph
        The reconstructed graph.

    Raises
    ------
    ValueError
        If the data is missing required keys.
    """
    if "nodes" not in data or "edges" not in data:
        raise ValueError("Serialized graph data must contain 'nodes' and 'edges' keys")

    dag = nx.DiGraph()

    for node_data in data["nodes"]:
        node_id = node_data["id"]
        attrs = {k: v for k, v in node_data.items() if k != "id"}
        dag.add_node(node_id, **attrs)

    for edge_data in data["edges"]:
        source = edge_data["source"]
        target = edge_data["target"]
        attrs = {k: v for k, v in edge_data.items() if k not in ("source", "target")}
        dag.add_edge(source, target, **attrs)

    return dag


def graph_to_cytoscape(dag: nx.DiGraph) -> dict[str, Any]:
    """Convert a DAG to Cytoscape.js JSON format for frontend visualization.

    Parameters
    ----------
    dag:
        The causal DAG.

    Returns
    -------
    dict
        Cytoscape.js compatible JSON with ``elements.nodes`` and
        ``elements.edges``.
    """
    # Node type to color mapping for visualization
    _NODE_COLORS: dict[str, str] = {
        "confounder": "#e74c3c",
        "mediator": "#3498db",
        "outcome": "#2ecc71",
        "treatment": "#9b59b6",
        "observed": "#95a5a6",
    }

    cy_nodes: list[dict[str, Any]] = []
    for node_id, attrs in dag.nodes(data=True):
        node_type = attrs.get("node_type", "observed")
        cy_node = {
            "data": {
                "id": node_id,
                "label": attrs.get("label", node_id),
                "node_type": node_type,
                "prior_belief": attrs.get("prior_belief", 0.5),
                "propagated_belief": attrs.get("propagated_belief"),
                "confidence": attrs.get("confidence", 0.5),
                "color": _NODE_COLORS.get(node_type, "#95a5a6"),
            }
        }
        cy_nodes.append(cy_node)

    cy_edges: list[dict[str, Any]] = []
    for source, target, attrs in dag.edges(data=True):
        strength = attrs.get("strength", 0.5)
        cy_edge = {
            "data": {
                "id": f"{source}->{target}",
                "source": source,
                "target": target,
                "strength": strength,
                "mechanism": attrs.get("mechanism", ""),
                "causal_type": attrs.get("causal_type", "direct"),
                "evidence_score": attrs.get("evidence_score", 0.5),
                "width": max(1, abs(strength) * 5),
                "line_style": "dashed" if attrs.get("causal_type") == "confounding" else "solid",
            }
        }
        cy_edges.append(cy_edge)

    return {
        "elements": {
            "nodes": cy_nodes,
            "edges": cy_edges,
        },
        "metadata": {
            "num_nodes": dag.number_of_nodes(),
            "num_edges": dag.number_of_edges(),
        },
    }


def graph_diff(
    dag_a: nx.DiGraph, dag_b: nx.DiGraph
) -> dict[str, Any]:
    """Compare two DAGs and return their differences.

    Useful for visualizing what-if scenarios by highlighting nodes and
    edges that changed between baseline and intervention.

    Parameters
    ----------
    dag_a:
        The baseline graph.
    dag_b:
        The comparison graph (e.g., after intervention).

    Returns
    -------
    dict
        ``{nodes_added, nodes_removed, edges_added, edges_removed,
          belief_changes, edge_changes}``
    """
    nodes_a = set(dag_a.nodes)
    nodes_b = set(dag_b.nodes)

    edges_a = set(dag_a.edges)
    edges_b = set(dag_b.edges)

    # Node additions and removals
    nodes_added = sorted(nodes_b - nodes_a)
    nodes_removed = sorted(nodes_a - nodes_b)

    # Edge additions and removals
    edges_added = [
        {"source": s, "target": t} for s, t in sorted(edges_b - edges_a)
    ]
    edges_removed = [
        {"source": s, "target": t} for s, t in sorted(edges_a - edges_b)
    ]

    # Belief changes for nodes present in both
    belief_changes: list[dict[str, Any]] = []
    common_nodes = nodes_a & nodes_b
    for node_id in sorted(common_nodes):
        belief_a = dag_a.nodes[node_id].get(
            "propagated_belief",
            dag_a.nodes[node_id].get("prior_belief", 0.5),
        )
        belief_b = dag_b.nodes[node_id].get(
            "propagated_belief",
            dag_b.nodes[node_id].get("prior_belief", 0.5),
        )
        delta = belief_b - belief_a
        if abs(delta) > 1e-6:
            belief_changes.append({
                "node_id": node_id,
                "label": dag_a.nodes[node_id].get("label", node_id),
                "belief_a": float(belief_a),
                "belief_b": float(belief_b),
                "delta": float(delta),
                "direction": "increased" if delta > 0 else "decreased",
            })

    # Sort belief changes by absolute delta descending
    belief_changes.sort(key=lambda x: abs(x["delta"]), reverse=True)

    # Edge strength changes
    edge_changes: list[dict[str, Any]] = []
    common_edges = edges_a & edges_b
    for source, target in sorted(common_edges):
        strength_a = dag_a.edges[source, target].get("strength", 0.5)
        strength_b = dag_b.edges[source, target].get("strength", 0.5)
        delta = strength_b - strength_a
        if abs(delta) > 1e-6:
            edge_changes.append({
                "source": source,
                "target": target,
                "strength_a": float(strength_a),
                "strength_b": float(strength_b),
                "delta": float(delta),
            })

    return {
        "nodes_added": nodes_added,
        "nodes_removed": nodes_removed,
        "edges_added": edges_added,
        "edges_removed": edges_removed,
        "belief_changes": belief_changes,
        "edge_changes": edge_changes,
    }


def _make_serializable(value: Any) -> Any:
    """Coerce numpy and other non-JSON types to native Python types."""
    import numpy as np

    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, set):
        return sorted(value)
    return value
