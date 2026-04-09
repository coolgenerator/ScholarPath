import React, { useRef, useEffect, useState } from 'react';
import * as d3 from 'd3';
import type { OrientationCausalGraph, CausalFactorNode, CausalFactorEdge } from '../../lib/types';

// ── Layer definitions ───────────────────────────────────────────────────

const LAYER_ORDER: Record<string, number> = {
  l3_environment: 0,
  l2_school: 1,
  l1_outcome: 2,
};

const LAYER_META: Record<string, { fill: string; stroke: string; label_en: string; label_zh: string }> = {
  l3_environment: { fill: '#faf5ff', stroke: '#c084fc', label_en: 'Environment (L3)', label_zh: '环境因素 (L3)' },
  l2_school:      { fill: '#eff6ff', stroke: '#60a5fa', label_en: 'School Traits (L2)', label_zh: '学校特征 (L2)' },
  l1_outcome:     { fill: '#f0fdf4', stroke: '#4ade80', label_en: 'Outcomes (L1)', label_zh: '结果指标 (L1)' },
};

// Default school color palette
const SCHOOL_PALETTE = ['#60a5fa', '#f97316', '#a78bfa', '#34d399'];

// ── Layout helpers ──────────────────────────────────────────────────────

interface PositionedNode extends CausalFactorNode {
  x: number;
  y: number;
}

interface PositionedEdge {
  source: PositionedNode;
  target: PositionedNode;
  strength: number;
  mechanism: string;
}

function layoutNodes(nodes: CausalFactorNode[], width: number, height: number): PositionedNode[] {
  const layers: Record<number, CausalFactorNode[]> = {};
  for (const n of nodes) {
    const layerIdx = LAYER_ORDER[n.layer] ?? 1;
    (layers[layerIdx] ??= []).push(n);
  }

  const layerKeys = Object.keys(layers).map(Number).sort();
  const yStep = height / (layerKeys.length + 1);
  const NODE_W = 160;

  const result: PositionedNode[] = [];
  for (const layerIdx of layerKeys) {
    const layerNodes = layers[layerIdx];
    const totalWidth = layerNodes.length * NODE_W + (layerNodes.length - 1) * 24;
    const startX = (width - totalWidth) / 2;
    layerNodes.forEach((n, i) => {
      result.push({
        ...n,
        x: startX + i * (NODE_W + 24) + NODE_W / 2,
        y: yStep * (layerKeys.indexOf(layerIdx) + 1),
      });
    });
  }
  return result;
}

function layoutEdges(
  edges: CausalFactorEdge[],
  positionedNodes: PositionedNode[],
): PositionedEdge[] {
  const nodeMap = new Map(positionedNodes.map((n) => [n.id, n]));
  return edges
    .map((e) => {
      const source = nodeMap.get(e.source);
      const target = nodeMap.get(e.target);
      if (!source || !target) return null;
      return { source, target, strength: e.strength, mechanism: e.mechanism };
    })
    .filter(Boolean) as PositionedEdge[];
}

// ── Component ───────────────────────────────────────────────────────────

interface OrientationCausalD3Props {
  graph: OrientationCausalGraph;
  schoolColors: Record<string, string>;
  schoolNames: Record<string, string>;
}

export function OrientationCausalD3({ graph, schoolColors, schoolNames }: OrientationCausalD3Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const [dimensions] = useState({ width: 960, height: 520 });

  const schoolIds = Object.keys(schoolNames);

  useEffect(() => {
    const svg = d3.select(svgRef.current);
    const tooltip = d3.select(tooltipRef.current);
    if (!svg.node() || !graph.nodes.length) return;

    svg.selectAll('*').remove();

    const { width, height } = dimensions;
    const NODE_W = 160;
    const NODE_H = 56 + schoolIds.length * 14;
    const BAR_MAX_W = 100;

    const nodes = layoutNodes(graph.nodes, width, height);
    const edges = layoutEdges(graph.edges, nodes);

    // Find max value per layer for normalization
    const maxPerLayer: Record<string, number> = {};
    for (const n of nodes) {
      const vals = Object.values(n.values).filter((v) => v > 0);
      const mx = Math.max(...vals, 1);
      maxPerLayer[n.layer] = Math.max(maxPerLayer[n.layer] ?? 0, mx);
    }

    const defs = svg.append('defs');

    // Arrow marker
    defs
      .append('marker')
      .attr('id', 'orient-arrow')
      .attr('viewBox', '0 0 10 10')
      .attr('refX', 10)
      .attr('refY', 5)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('path')
      .attr('d', 'M0,0 L10,5 L0,10 Z')
      .attr('fill', '#94a3b8');

    // ── Edges ──
    const edgeGroup = svg.append('g').attr('class', 'edges');
    edgeGroup
      .selectAll('path')
      .data(edges)
      .join('path')
      .attr('d', (d) => {
        const midY = (d.source.y + d.target.y) / 2;
        return `M${d.source.x},${d.source.y + NODE_H / 2} Q${d.source.x},${midY} ${d.target.x},${d.target.y - NODE_H / 2}`;
      })
      .attr('fill', 'none')
      .attr('stroke', '#cbd5e1')
      .attr('stroke-width', (d) => Math.max(0.5, d.strength * 2.5))
      .attr('stroke-opacity', 0.4)
      .attr('marker-end', 'url(#orient-arrow)');

    // ── Layer labels ──
    const layerKeys = [0, 1, 2];
    const layerNames = ['l3_environment', 'l2_school', 'l1_outcome'];
    const yStep = height / (layerKeys.length + 1);
    svg.append('g').attr('class', 'layer-labels')
      .selectAll('text')
      .data(layerNames)
      .join('text')
      .attr('x', 12)
      .attr('y', (_, i) => yStep * (i + 1) - NODE_H / 2 - 10)
      .attr('fill', (d) => LAYER_META[d]?.stroke ?? '#64748b')
      .attr('font-size', '11px')
      .attr('font-weight', '700')
      .attr('opacity', 0.7)
      .text((d) => LAYER_META[d]?.label_zh ?? d);

    // ── Nodes ──
    const nodeGroup = svg.append('g').attr('class', 'nodes');
    const nodeGs = nodeGroup
      .selectAll('g')
      .data(nodes)
      .join('g')
      .attr('transform', (d) => `translate(${d.x - NODE_W / 2},${d.y - NODE_H / 2})`);

    // Node background
    nodeGs
      .append('rect')
      .attr('width', NODE_W)
      .attr('height', NODE_H)
      .attr('rx', 10)
      .attr('fill', (d) => LAYER_META[d.layer]?.fill ?? '#f8fafc')
      .attr('stroke', (d) => LAYER_META[d.layer]?.stroke ?? '#94a3b8')
      .attr('stroke-width', 1.5);

    // Node label
    nodeGs
      .append('text')
      .attr('x', 8)
      .attr('y', 16)
      .attr('font-size', '10px')
      .attr('font-weight', '600')
      .attr('fill', '#1e293b')
      .text((d) => d.label.length > 22 ? d.label.slice(0, 20) + '…' : d.label);

    // Mini bar chart per school
    nodeGs.each(function (d) {
      const g = d3.select(this);
      const maxVal = maxPerLayer[d.layer] || 1;

      schoolIds.forEach((sid, i) => {
        const val = d.values[sid] ?? 0;
        const barW = Math.max(2, (val / maxVal) * BAR_MAX_W);
        const barY = 26 + i * 14;

        // Bar background
        g.append('rect')
          .attr('x', 8)
          .attr('y', barY)
          .attr('width', BAR_MAX_W)
          .attr('height', 10)
          .attr('rx', 3)
          .attr('fill', '#f1f5f9');

        // Bar fill
        g.append('rect')
          .attr('x', 8)
          .attr('y', barY)
          .attr('width', barW)
          .attr('height', 10)
          .attr('rx', 3)
          .attr('fill', schoolColors[sid] ?? SCHOOL_PALETTE[i % SCHOOL_PALETTE.length]);

        // Value label
        const displayVal = val >= 1000 ? `${(val / 1000).toFixed(0)}K` : val.toFixed(val < 10 ? 2 : 0);
        g.append('text')
          .attr('x', BAR_MAX_W + 14)
          .attr('y', barY + 9)
          .attr('font-size', '9px')
          .attr('fill', '#64748b')
          .text(displayVal);
      });
    });

    // Tooltip on hover
    nodeGs
      .on('mouseenter', function (event, d) {
        const lines = schoolIds.map((sid) => {
          const name = schoolNames[sid] ?? sid.slice(0, 8);
          const val = d.values[sid];
          return `<div style="display:flex;align-items:center;gap:6px;margin:2px 0">
            <span style="width:8px;height:8px;border-radius:50%;background:${schoolColors[sid] ?? '#94a3b8'};display:inline-block"></span>
            <span>${name}: <b>${val != null ? (typeof val === 'number' && val >= 1000 ? `${(val / 1000).toFixed(1)}K` : val) : '—'}</b></span>
          </div>`;
        });
        tooltip
          .html(`<div style="font-weight:700;margin-bottom:4px">${d.label}</div>${lines.join('')}`)
          .style('visibility', 'visible')
          .style('left', `${event.offsetX + 16}px`)
          .style('top', `${event.offsetY - 10}px`);
      })
      .on('mouseleave', () => {
        tooltip.style('visibility', 'hidden');
      });
  }, [graph, schoolColors, schoolNames, schoolIds, dimensions]);

  if (!graph.nodes.length) {
    return <div className="text-sm text-on-surface-variant/60 py-8 text-center">No causal data available</div>;
  }

  return (
    <div className="relative">
      <svg
        ref={svgRef}
        width={dimensions.width}
        height={dimensions.height}
        className="w-full"
        viewBox={`0 0 ${dimensions.width} ${dimensions.height}`}
      />
      {/* School legend */}
      <div className="flex flex-wrap gap-3 mt-3 px-2">
        {schoolIds.map((sid, i) => (
          <div key={sid} className="flex items-center gap-1.5 text-xs text-on-surface-variant">
            <span
              className="inline-block w-2.5 h-2.5 rounded-full"
              style={{ background: schoolColors[sid] ?? SCHOOL_PALETTE[i % SCHOOL_PALETTE.length] }}
            />
            {schoolNames[sid] ?? sid.slice(0, 8)}
          </div>
        ))}
      </div>
      {/* Tooltip overlay */}
      <div
        ref={tooltipRef}
        className="absolute pointer-events-none z-50 rounded-lg bg-white/95 border border-outline-variant/20 shadow-lg px-3 py-2 text-xs text-on-surface max-w-xs"
        style={{ visibility: 'hidden' }}
      />
    </div>
  );
}
