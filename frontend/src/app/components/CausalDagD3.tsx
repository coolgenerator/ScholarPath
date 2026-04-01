import React, { useRef, useEffect, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { causalApi, CausalDagResponse, CyNode, CyEdge } from '../../lib/api/causal';

// ─── Layout: assign layer-based Y positions ───

const LAYER_ORDER: Record<string, number> = {
  confounder: 0,
  treatment: 1,
  mediator: 2,
  outcome: 3,
  observed: 2,
};

const LAYER_COLORS: Record<string, { fill: string; stroke: string; label: string }> = {
  confounder: { fill: '#faf5ff', stroke: '#c084fc', label: 'Confounder' },
  treatment:  { fill: '#eff6ff', stroke: '#60a5fa', label: 'Treatment' },
  mediator:   { fill: '#fefce8', stroke: '#facc15', label: 'Mediator' },
  outcome:    { fill: '#f0fdf4', stroke: '#4ade80', label: 'Outcome' },
  observed:   { fill: '#f8fafc', stroke: '#94a3b8', label: 'Observed' },
};

interface DagNode {
  id: string;
  label: string;
  nodeType: string;
  belief: number;
  confidence: number;
  x: number;
  y: number;
}

interface DagLink {
  source: string;
  target: string;
  strength: number;
  mechanism: string;
  causalType: string;
  lineStyle: string;
}

function layoutNodes(nodes: CyNode[], width: number, height: number): DagNode[] {
  // Group by layer
  const layers: Record<number, CyNode[]> = {};
  for (const n of nodes) {
    const layer = LAYER_ORDER[n.data.node_type] ?? 2;
    (layers[layer] ??= []).push(n);
  }

  const layerKeys = Object.keys(layers).map(Number).sort();
  const yStep = height / (layerKeys.length + 1);

  const result: DagNode[] = [];
  for (const layerIdx of layerKeys) {
    const layerNodes = layers[layerIdx];
    const xStep = width / (layerNodes.length + 1);
    layerNodes.forEach((n, i) => {
      result.push({
        id: n.data.id,
        label: n.data.label,
        nodeType: n.data.node_type,
        belief: n.data.propagated_belief ?? n.data.prior_belief,
        confidence: n.data.confidence,
        x: xStep * (i + 1),
        y: yStep * (layerKeys.indexOf(layerIdx) + 1),
      });
    });
  }
  return result;
}

function layoutLinks(edges: CyEdge[]): DagLink[] {
  return edges.map((e) => ({
    source: e.data.source,
    target: e.data.target,
    strength: e.data.strength,
    mechanism: e.data.mechanism,
    causalType: e.data.causal_type,
    lineStyle: e.data.line_style,
  }));
}

// ─── D3 Renderer ───

interface CausalDagD3Props {
  studentId: string | null;
  schoolId: string;
  whatIfDeltas?: Record<string, number> | null;
  t: Record<string, any>;
}

export function CausalDagD3({ studentId, schoolId, whatIfDeltas, t }: CausalDagD3Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const [dagData, setDagData] = useState<CausalDagResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(false);

  // Fetch DAG data
  const fetchDag = useCallback(async () => {
    if (!studentId || !schoolId) return;
    setLoading(true);
    setError(false);
    try {
      const data = await causalApi.getDag(studentId, schoolId);
      setDagData(data);
    } catch {
      setError(true);
    }
    setLoading(false);
  }, [studentId, schoolId]);

  useEffect(() => {
    fetchDag();
  }, [fetchDag]);

  // Render with D3
  useEffect(() => {
    if (!dagData || !svgRef.current) return;

    const svg = d3.select(svgRef.current);
    const tooltip = tooltipRef.current ? d3.select(tooltipRef.current) : null;
    svg.selectAll('*').remove();

    const width = svgRef.current.clientWidth || 700;
    const height = 480;

    svg.attr('viewBox', `0 0 ${width} ${height}`);

    const nodes = layoutNodes(dagData.elements.nodes, width, height);
    const links = layoutLinks(dagData.elements.edges);
    const nodeMap = new Map(nodes.map((n) => [n.id, n]));

    // Arrow marker
    svg.append('defs').append('marker')
      .attr('id', 'dag-arrow')
      .attr('viewBox', '0 0 10 6')
      .attr('refX', 10)
      .attr('refY', 3)
      .attr('markerWidth', 8)
      .attr('markerHeight', 5)
      .attr('orient', 'auto')
      .append('path')
      .attr('d', 'M0,0 L10,3 L0,6')
      .attr('fill', '#94a3b8');

    // Glow filter for affected nodes
    const defs = svg.select('defs');
    const filter = defs.append('filter').attr('id', 'glow');
    filter.append('feGaussianBlur').attr('stdDeviation', '3').attr('result', 'blur');
    filter.append('feMerge').selectAll('feMergeNode')
      .data(['blur', 'SourceGraphic']).enter()
      .append('feMergeNode').attr('in', (d) => d);

    // Draw edges
    const edgeGroup = svg.append('g').attr('class', 'edges');
    edgeGroup.selectAll('path')
      .data(links)
      .enter()
      .append('path')
      .attr('d', (d) => {
        const src = nodeMap.get(d.source);
        const tgt = nodeMap.get(d.target);
        if (!src || !tgt) return '';
        const dx = tgt.x - src.x;
        const dy = tgt.y - src.y;
        const cx = src.x + dx * 0.5;
        const cy = src.y + dy * 0.5 - Math.abs(dx) * 0.1;
        return `M${src.x},${src.y} Q${cx},${cy} ${tgt.x},${tgt.y}`;
      })
      .attr('fill', 'none')
      .attr('stroke', (d) => {
        const delta = whatIfDeltas?.[d.target] ?? 0;
        if (whatIfDeltas && Math.abs(delta) > 0.01) return delta > 0 ? '#22c55e' : '#ef4444';
        return d.strength < 0 ? '#f87171' : '#cbd5e1';
      })
      .attr('stroke-width', (d) => Math.max(1.5, Math.abs(d.strength) * 4))
      .attr('stroke-opacity', (d) => {
        if (whatIfDeltas) {
          const delta = whatIfDeltas[d.target] ?? 0;
          return Math.abs(delta) > 0.01 ? 0.8 : 0.15;
        }
        return 0.4;
      })
      .attr('stroke-dasharray', (d) => d.lineStyle === 'dashed' ? '6,4' : 'none')
      .attr('marker-end', 'url(#dag-arrow)');

    // Draw nodes
    const nodeGroup = svg.append('g').attr('class', 'nodes');
    const nodeG = nodeGroup.selectAll('g')
      .data(nodes)
      .enter()
      .append('g')
      .attr('transform', (d) => `translate(${d.x},${d.y})`)
      .style('cursor', 'pointer');

    // Node background (rounded rect)
    const nodeW = 120;
    const nodeH = 52;
    nodeG.append('rect')
      .attr('x', -nodeW / 2)
      .attr('y', -nodeH / 2)
      .attr('width', nodeW)
      .attr('height', nodeH)
      .attr('rx', 14)
      .attr('fill', (d) => LAYER_COLORS[d.nodeType]?.fill ?? '#f8fafc')
      .attr('stroke', (d) => {
        const delta = whatIfDeltas?.[d.id] ?? 0;
        if (whatIfDeltas && Math.abs(delta) > 0.01) return delta > 0 ? '#22c55e' : '#ef4444';
        return LAYER_COLORS[d.nodeType]?.stroke ?? '#94a3b8';
      })
      .attr('stroke-width', (d) => {
        const delta = whatIfDeltas?.[d.id] ?? 0;
        return whatIfDeltas && Math.abs(delta) > 0.01 ? 2.5 : 1.5;
      })
      .attr('filter', (d) => {
        const delta = whatIfDeltas?.[d.id] ?? 0;
        return whatIfDeltas && Math.abs(delta) > 0.01 ? 'url(#glow)' : 'none';
      });

    // Node label
    nodeG.append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', (d) => d.belief != null ? -4 : 2)
      .attr('font-size', 10)
      .attr('font-weight', 700)
      .attr('fill', (d) => LAYER_COLORS[d.nodeType]?.stroke ?? '#475569')
      .text((d) => {
        // Shorten labels
        const lbl = d.label.replace(/\(.*\)/, '').trim();
        return lbl.length > 16 ? lbl.slice(0, 15) + '…' : lbl;
      });

    // Belief value
    nodeG.append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', 12)
      .attr('font-size', 11)
      .attr('font-weight', 800)
      .attr('fill', (d) => {
        const delta = whatIfDeltas?.[d.id] ?? 0;
        if (whatIfDeltas && delta > 0.01) return '#15803d';
        if (whatIfDeltas && delta < -0.01) return '#dc2626';
        return '#334155';
      })
      .text((d) => {
        const pct = `${Math.round(d.belief * 100)}%`;
        const delta = whatIfDeltas?.[d.id] ?? 0;
        if (whatIfDeltas && Math.abs(delta) > 0.01) {
          return `${pct} (${delta > 0 ? '+' : ''}${Math.round(delta * 100)})`;
        }
        return pct;
      });

    // Tooltip on hover
    if (tooltip) {
      nodeG
        .on('mouseenter', (event, d) => {
          const link = links.find((l) => l.target === d.id || l.source === d.id);
          tooltip
            .style('opacity', 1)
            .style('left', `${event.offsetX + 12}px`)
            .style('top', `${event.offsetY - 8}px`)
            .html(`
              <div style="font-weight:800;margin-bottom:4px">${d.label}</div>
              <div style="font-size:11px;color:#64748b">
                Type: ${d.nodeType}<br/>
                Belief: ${Math.round(d.belief * 100)}%<br/>
                Confidence: ${Math.round(d.confidence * 100)}%
                ${link?.mechanism ? `<br/>Mechanism: ${link.mechanism}` : ''}
              </div>
            `);
        })
        .on('mouseleave', () => {
          tooltip.style('opacity', 0);
        });
    }
  }, [dagData, whatIfDeltas]);

  return (
    <div className="bg-surface-container-lowest rounded-3xl border border-outline-variant/10 p-6 relative">
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <span className="material-symbols-outlined text-primary text-xl">account_tree</span>
        <h3 className="font-headline text-base font-black text-on-surface">{t.dec_dag_title ?? 'Causal DAG'}</h3>
        <div className="flex gap-3 ml-auto">
          {Object.entries(LAYER_COLORS).filter(([k]) => k !== 'observed').map(([key, col]) => (
            <div key={key} className="flex items-center gap-1">
              <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: col.stroke }} />
              <span className="text-[9px] font-bold text-on-surface-variant/60 uppercase">{col.label}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Loading */}
      {loading && (
        <div className="flex items-center justify-center py-20">
          <span className="material-symbols-outlined text-primary text-2xl animate-spin">progress_activity</span>
        </div>
      )}

      {/* Error / no school selected */}
      {!loading && error && (
        <div className="flex flex-col items-center justify-center py-16 text-center">
          <span className="material-symbols-outlined text-on-surface-variant/30 text-3xl mb-3">warning</span>
          <p className="text-xs text-on-surface-variant/50">Failed to load causal graph. Select a school first.</p>
          <button onClick={fetchDag} className="mt-3 text-xs text-primary font-bold hover:underline">Retry</button>
        </div>
      )}

      {/* Fallback: no school */}
      {!loading && !error && !dagData && (
        <div className="flex flex-col items-center justify-center py-16 text-center">
          <span className="material-symbols-outlined text-on-surface-variant/20 text-4xl mb-3">schema</span>
          <p className="text-sm text-on-surface-variant/40">Select a school to view its causal graph</p>
        </div>
      )}

      {/* SVG */}
      {dagData && !loading && (
        <svg ref={svgRef} className="w-full" style={{ height: 480 }} />
      )}

      {/* Tooltip overlay */}
      <div
        ref={tooltipRef}
        className="absolute pointer-events-none bg-white/95 backdrop-blur-sm rounded-xl shadow-lg border border-outline-variant/20 px-4 py-3 text-xs z-50 max-w-[240px]"
        style={{ opacity: 0, transition: 'opacity 0.15s' }}
      />
    </div>
  );
}
