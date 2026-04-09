import React, { useMemo, useState } from 'react';
import { useComparisonReport, type StreamingReport } from '../../hooks/useComparisonReport';
import type {
  EvaluationWithSchool,
  OrientationCausalGraph,
  OrientationComparison,
} from '../../lib/types';
import { MotionItem, MotionStagger, MotionSurface } from './WorkspaceMotion';
import { OrientationCausalD3 } from './OrientationCausalD3';

// ── Constants ───────────────────────────────────────────────────────────

const SCHOOL_PALETTE = ['#60a5fa', '#f97316', '#a78bfa', '#34d399'];

const ORIENTATION_META: Record<string, { icon: string; en: string; zh: string }> = {
  big_tech:       { icon: 'business_center', en: 'Big Tech',       zh: '大厂就业' },
  startup:        { icon: 'rocket_launch',   en: 'Startup',        zh: '创业导向' },
  roi:            { icon: 'trending_up',     en: 'ROI',            zh: '经济性价比' },
  lifestyle:      { icon: 'favorite',        en: 'Lifestyle',      zh: '生活便利性' },
  phd_research:   { icon: 'science',         en: 'PhD / Research', zh: '科研导向' },
  finance_biz:    { icon: 'account_balance', en: 'Finance / Biz',  zh: '金融商科' },
  public_service: { icon: 'gavel',           en: 'Public Service', zh: '公共服务' },
};

const LAYER_LABELS: Record<string, { en: string; zh: string }> = {
  l1: { en: 'L1 — Outcomes', zh: 'L1 — 结果指标' },
  l2: { en: 'L2 — School Traits', zh: 'L2 — 学校特征' },
  l3: { en: 'L3 — Environment', zh: 'L3 — 环境因素' },
};

// ── Ranked school type (matches DecisionsPanel) ─────────────────────────

export interface RankedSchool {
  eval: EvaluationWithSchool;
  weightedScore: number;
  rank: number;
}

// ── Sub-components ──────────────────────────────────────────────────────

function SchoolCheckCard({
  school,
  selected,
  color,
  majors,
  admitted,
  onToggle,
}: {
  school: RankedSchool;
  selected: boolean;
  color: string;
  majors: string[];
  admitted: boolean;
  onToggle: () => void;
}) {
  const name = school.eval.school?.name ?? `School #${school.rank}`;
  const matchedProgram = school.eval.school?.programs?.find((p) => {
    const m = (majors[0] ?? '').toLowerCase();
    return m && (p.name.toLowerCase().includes(m) || p.department.toLowerCase().includes(m));
  });
  return (
    <button
      type="button"
      onClick={onToggle}
      className={`relative flex items-center gap-3 rounded-xl border px-4 py-3 text-left transition-all ${
        selected
          ? 'border-primary/40 bg-primary/5 shadow-sm'
          : 'border-outline-variant/15 bg-surface-container-low hover:border-outline-variant/30'
      }`}
    >
      <span
        className="flex h-5 w-5 shrink-0 items-center justify-center rounded-md border-2 text-[10px] font-black"
        style={{
          borderColor: selected ? color : '#cbd5e1',
          background: selected ? color : 'transparent',
          color: selected ? '#fff' : '#94a3b8',
        }}
      >
        {selected ? '✓' : school.rank}
      </span>
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-1.5">
          <span className="text-sm font-bold text-on-surface truncate">{name}</span>
          {admitted && (
            <span className="shrink-0 inline-flex items-center gap-0.5 text-[8px] font-black uppercase tracking-wider text-green-700 bg-green-100 px-1.5 py-0.5 rounded">
              <span className="material-symbols-outlined text-[10px]" style={{ fontVariationSettings: "'FILL' 1" }}>check_circle</span>
              Offer
            </span>
          )}
        </div>
        <div className="flex items-center gap-1.5 mt-0.5 flex-wrap">
          {majors.length > 0 && (
            <span className="inline-flex items-center gap-0.5 text-[9px] font-bold text-primary bg-primary/8 px-1.5 py-0.5 rounded">
              {majors[0]}
              {matchedProgram?.us_news_rank && <span className="text-primary/60">#{matchedProgram.us_news_rank}</span>}
            </span>
          )}
          <span className="text-[10px] text-on-surface-variant">
            {school.eval.school?.city}, {school.eval.school?.state} · {Math.round(school.weightedScore * 100)}%
          </span>
        </div>
      </div>
    </button>
  );
}

function OrientationSection({
  comparison,
  graph,
  schoolColors,
  schoolNames,
  isCn,
  defaultExpanded,
}: {
  comparison: OrientationComparison;
  graph?: OrientationCausalGraph;
  schoolColors: Record<string, string>;
  schoolNames: Record<string, string>;
  isCn: boolean;
  defaultExpanded: boolean;
}) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const meta = ORIENTATION_META[comparison.orientation];
  const label = isCn ? meta?.zh : meta?.en;

  return (
    <MotionSurface className="overflow-hidden">
      {/* Header */}
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        className="flex w-full items-center gap-3 px-5 py-4 text-left hover:bg-surface-container-high/40 transition-colors"
      >
        <span className="material-symbols-outlined text-xl text-primary">{meta?.icon ?? 'analytics'}</span>
        <span className="flex-1 text-sm font-bold text-on-surface">{label ?? comparison.orientation}</span>
        {/* Mini score bars */}
        <div className="flex items-center gap-2">
          {comparison.schools.map((s, i) => (
            <div key={s.school_id} className="flex items-center gap-1">
              <span
                className="inline-block h-2 rounded-full"
                style={{
                  width: `${Math.max(12, s.score * 60)}px`,
                  background: schoolColors[s.school_id] ?? SCHOOL_PALETTE[i],
                }}
              />
              <span className="text-[10px] font-bold text-on-surface-variant">{Math.round(s.score * 100)}%</span>
            </div>
          ))}
        </div>
        <span className="material-symbols-outlined text-base text-on-surface-variant">
          {expanded ? 'expand_less' : 'expand_more'}
        </span>
      </button>

      {/* Expanded content */}
      {expanded && (
        <div className="border-t border-outline-variant/10 px-5 py-4 space-y-5">
          {/* Signal tables per layer */}
          {(['l1', 'l2', 'l3'] as const).map((layer) => {
            const layerKey = layer as 'l1' | 'l2' | 'l3';
            const layerLabel = isCn ? LAYER_LABELS[layer].zh : LAYER_LABELS[layer].en;
            const signals = _collectSignals(comparison, layerKey);
            if (!signals.length) return null;
            return (
              <div key={layer}>
                <div className="text-[10px] font-bold uppercase tracking-widest text-on-surface-variant/60 mb-2">
                  {layerLabel}
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-outline-variant/10">
                        <th className="text-left py-1.5 pr-3 font-bold text-on-surface-variant">Factor</th>
                        {comparison.schools.map((s, i) => (
                          <th key={s.school_id} className="text-right py-1.5 px-2 font-bold" style={{ color: schoolColors[s.school_id] ?? SCHOOL_PALETTE[i] }}>
                            {s.school_name.length > 12 ? s.school_name.slice(0, 10) + '…' : s.school_name}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {signals.map((sig) => (
                        <tr key={sig.key} className="border-b border-outline-variant/5">
                          <td className="py-1.5 pr-3 text-on-surface-variant">{sig.label}</td>
                          {comparison.schools.map((s) => {
                            const val = sig.values[s.school_id];
                            return (
                              <td key={s.school_id} className="text-right py-1.5 px-2 font-mono text-on-surface">
                                {_formatSignalValue(val)}
                              </td>
                            );
                          })}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            );
          })}

          {/* Causal graph */}
          {graph && (
            <div>
              <div className="text-[10px] font-bold uppercase tracking-widest text-on-surface-variant/60 mb-2">
                {isCn ? '因果关系图' : 'Causal Relationship Graph'}
              </div>
              <OrientationCausalD3
                graph={graph}
                schoolColors={schoolColors}
                schoolNames={schoolNames}
              />
            </div>
          )}

          {/* Narrative */}
          {comparison.narrative && (
            <div className="rounded-lg bg-surface-container-low/60 px-4 py-3 text-sm leading-relaxed text-on-surface-variant">
              {comparison.narrative}
            </div>
          )}
        </div>
      )}
    </MotionSurface>
  );
}

// ── Helpers ──────────────────────────────────────────────────────────────

interface SignalRow {
  key: string;
  label: string;
  values: Record<string, unknown>;
}

function _collectSignals(comparison: OrientationComparison, layer: 'l1' | 'l2' | 'l3'): SignalRow[] {
  const allKeys = new Map<string, string>();
  for (const school of comparison.schools) {
    const signals = school[layer].signals;
    for (const [k, v] of Object.entries(signals)) {
      if (!allKeys.has(k)) {
        allKeys.set(k, k.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()));
      }
    }
  }

  return Array.from(allKeys.entries()).map(([key, label]) => ({
    key,
    label,
    values: Object.fromEntries(
      comparison.schools.map((s) => [s.school_id, s[layer].signals[key]]),
    ),
  }));
}

function _formatSignalValue(val: unknown): string {
  if (val == null) return '—';
  if (typeof val === 'boolean') return val ? '✓' : '✗';
  if (typeof val === 'number') {
    if (val >= 1_000_000_000) return `$${(val / 1_000_000_000).toFixed(1)}B`;
    if (val >= 1_000_000) return `$${(val / 1_000_000).toFixed(0)}M`;
    if (val >= 1_000) return `${(val / 1_000).toFixed(0)}K`;
    if (val < 1 && val > 0) return `${(val * 100).toFixed(0)}%`;
    return val.toFixed(val < 10 ? 2 : 0);
  }
  return String(val);
}

// ── Main Component ──────────────────────────────────────────────────────

interface ComparisonReportPanelProps {
  studentId: string | null;
  rankedSchools: RankedSchool[];
  majors: string[];
  admittedSchoolIds: Set<string>;
  isCn: boolean;
  t: Record<string, any>;
}

export function ComparisonReportPanel({ studentId, rankedSchools, majors, admittedSchoolIds, isCn, t }: ComparisonReportPanelProps) {
  const { report, isLoading, currentStep, error, generate, clear } = useComparisonReport();
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [selectedOrientations, setSelectedOrientations] = useState<Set<string>>(new Set(['big_tech', 'roi', 'phd_research']));
  const [showReport, setShowReport] = useState(false);

  // Colors based on report school IDs (stable even when selection changes)
  const schoolColors = useMemo(() => {
    const ids = report?.schoolIds?.length ? report.schoolIds : Array.from(selectedIds);
    const colors: Record<string, string> = {};
    ids.forEach((id, i) => {
      colors[id] = SCHOOL_PALETTE[i % SCHOOL_PALETTE.length];
    });
    return colors;
  }, [selectedIds, report?.schoolIds]);

  // School names for selected + any in-report schools (for D3 graph + report card)
  const schoolNames = useMemo(() => {
    const relevantIds = new Set([...selectedIds, ...(report?.schoolIds ?? [])]);
    const names: Record<string, string> = {};
    for (const r of rankedSchools) {
      if (relevantIds.has(r.eval.school_id)) {
        names[r.eval.school_id] = r.eval.school?.name ?? `School #${r.rank}`;
      }
    }
    return names;
  }, [rankedSchools, selectedIds, report?.schoolIds]);

  const graphMap = useMemo(() => {
    if (!report) return {};
    const map: Record<string, OrientationCausalGraph> = {};
    for (const g of report.causalGraphs) {
      map[g.orientation] = g;
    }
    return map;
  }, [report]);

  function toggleSchool(id: string) {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else if (next.size < 4) {
        next.add(id);
      }
      return next;
    });
  }

  async function handleGenerate() {
    if (!studentId || selectedIds.size < 2 || selectedOrientations.size === 0) return;
    setShowReport(true);
    await generate(studentId, {
      school_ids: Array.from(selectedIds),
      orientations: Array.from(selectedOrientations),
    });
  }

  function handleBack() {
    setShowReport(false);
  }

  function handleNewReport() {
    clear();
    setShowReport(false);
  }

  // ── View routing ──
  const hasReport = report != null && report.orientations.length > 0;
  const showReportView = showReport && (hasReport || isLoading);

  // ── Phase 1: School Selection ──
  if (!showReportView) {
    return (
      <div className="space-y-6">
        {/* Previous report card */}
        {hasReport && (
          <button
            type="button"
            onClick={() => setShowReport(true)}
            className="w-full flex items-center gap-3 rounded-xl border border-primary/20 bg-primary/5 px-4 py-3 text-left hover:bg-primary/10 transition-colors"
          >
            <span className="material-symbols-outlined text-lg text-primary">description</span>
            <div className="flex-1 min-w-0">
              <div className="text-sm font-bold text-on-surface">
                {isCn ? '查看上次报告' : 'View Previous Report'}
              </div>
              <div className="text-[10px] text-on-surface-variant mt-0.5">
                {report.schoolIds.map((sid) => schoolNames[sid] ?? sid.slice(0, 8)).join(' vs ')}
                {' · '}
                {report.orientations.length} {isCn ? '个导向' : 'orientations'}
              </div>
            </div>
            <span className="material-symbols-outlined text-sm text-primary">arrow_forward</span>
          </button>
        )}

        <div>
          <h3 className="font-headline text-lg font-black text-on-surface">
            {isCn ? (hasReport ? '重新选择学校对比' : '选择学校进行深度对比') : (hasReport ? 'Compare Different Schools' : 'Select Schools for Deep Comparison')}
          </h3>
          <p className="mt-1 text-xs text-on-surface-variant">
            {isCn
              ? `选择 2-4 所学校，系统将按职业导向生成多层因果分析报告。已选 ${selectedIds.size}/4`
              : `Select 2-4 schools for a multi-orientation causal analysis report. Selected ${selectedIds.size}/4`}
          </p>
        </div>

        {/* Admitted schools first, then others up to 20 total */}
        {(() => {
          const admitted = rankedSchools.filter((r) => admittedSchoolIds.has(r.eval.school_id));
          const others = rankedSchools.filter((r) => !admittedSchoolIds.has(r.eval.school_id));
          const display = [...admitted, ...others.slice(0, Math.max(0, 20 - admitted.length))];
          return (
            <>
              {admitted.length > 0 && (
                <div className="text-[10px] font-bold uppercase tracking-widest text-green-700 flex items-center gap-1.5">
                  <span className="material-symbols-outlined text-xs" style={{ fontVariationSettings: "'FILL' 1" }}>check_circle</span>
                  {isCn ? `已拿到 Offer · ${admitted.length}` : `Admitted · ${admitted.length}`}
                </div>
              )}
              <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                {display.map((r, i) => (
                  <SchoolCheckCard
                    key={r.eval.school_id}
                    school={r}
                    selected={selectedIds.has(r.eval.school_id)}
                    color={schoolColors[r.eval.school_id] ?? SCHOOL_PALETTE[i % SCHOOL_PALETTE.length]}
                    majors={majors}
                    admitted={admittedSchoolIds.has(r.eval.school_id)}
                    onToggle={() => toggleSchool(r.eval.school_id)}
                  />
                ))}
              </div>
            </>
          );
        })()}


        {/* Orientation selection */}
        <div>
          <div className="text-[10px] font-bold uppercase tracking-widest text-on-surface-variant mb-2">
            {isCn ? '选择分析维度' : 'Analysis Dimensions'}
          </div>
          <div className="flex flex-wrap gap-2">
            {Object.entries(ORIENTATION_META).map(([key, meta]) => {
              const active = selectedOrientations.has(key);
              return (
                <button
                  key={key}
                  type="button"
                  onClick={() => setSelectedOrientations((prev) => {
                    const next = new Set(prev);
                    if (next.has(key)) next.delete(key);
                    else next.add(key);
                    return next;
                  })}
                  className={`inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-bold transition-all ${
                    active
                      ? 'bg-primary text-on-primary shadow-sm'
                      : 'bg-surface-container-low text-on-surface-variant border border-outline-variant/15 hover:bg-surface-container-high'
                  }`}
                >
                  <span className="material-symbols-outlined text-sm">{meta.icon}</span>
                  {isCn ? meta.zh : meta.en}
                </button>
              );
            })}
          </div>
        </div>

        {error && (
          <div className="rounded-lg bg-red-50 border border-red-200 px-4 py-3 text-sm text-red-700">
            {error.message}
          </div>
        )}

        <button
          type="button"
          onClick={handleGenerate}
          disabled={selectedIds.size < 2 || selectedOrientations.size === 0 || isLoading}
          className="flex items-center gap-2 rounded-xl bg-primary px-6 py-3 text-sm font-bold text-on-primary shadow-md hover:brightness-110 transition-all disabled:opacity-50"
        >
          <span className={`material-symbols-outlined text-base ${isLoading ? 'animate-spin' : ''}`}>
            {isLoading ? 'progress_activity' : 'compare'}
          </span>
          {isLoading
            ? (isCn ? '正在生成报告…' : 'Generating report…')
            : (isCn ? '生成对比报告' : 'Generate Comparison Report')}
        </button>
      </div>
    );
  }

  // ── Phase 2: Report Display (progressive) ──
  const stepLabel = currentStep
    ? (ORIENTATION_META[currentStep]
        ? (isCn ? ORIENTATION_META[currentStep].zh : ORIENTATION_META[currentStep].en)
        : (currentStep === 'recommendation' ? (isCn ? '综合建议' : 'Final recommendation') : currentStep))
    : null;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="font-headline text-lg font-black text-on-surface">
            {isCn ? '深度对比报告' : 'Deep Comparison Report'}
          </h3>
          <p className="mt-1 text-xs text-on-surface-variant">
            {isCn ? `${report.orientations.length} 个职业导向 × ${report.schoolIds.length} 所学校` : `${report.orientations.length} orientations × ${report.schoolIds.length} schools`}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={handleNewReport}
            disabled={isLoading}
            className="flex items-center gap-1.5 rounded-xl border border-outline-variant/15 px-4 py-2 text-xs font-bold text-on-surface-variant hover:bg-surface-container-high transition-colors disabled:opacity-50"
          >
            <span className="material-symbols-outlined text-sm">compare</span>
            {isCn ? '重新对比' : 'New Report'}
          </button>
          <button
            type="button"
            onClick={handleBack}
            disabled={isLoading}
            className="flex items-center gap-1.5 rounded-xl border border-outline-variant/15 px-4 py-2 text-xs font-bold text-on-surface-variant hover:bg-surface-container-high transition-colors disabled:opacity-50"
          >
            <span className="material-symbols-outlined text-sm">arrow_back</span>
            {isCn ? '返回选择' : 'Back'}
          </button>
        </div>
      </div>

      {/* Streaming progress indicator */}
      {isLoading && (
        <div className="flex items-center gap-3 rounded-xl bg-primary/5 border border-primary/15 px-4 py-3">
          <span className="material-symbols-outlined text-base text-primary animate-spin">progress_activity</span>
          <div className="flex-1 min-w-0">
            <div className="text-xs font-bold text-primary">
              {isCn ? '正在分析…' : 'Analyzing…'}
            </div>
            {stepLabel && (
              <div className="text-[10px] text-on-surface-variant mt-0.5">
                {isCn ? `当前: ${stepLabel}` : `Current: ${stepLabel}`}
                {report.orientations.length > 0 && ` (${report.orientations.length}/${selectedOrientations.size})`}
              </div>
            )}
          </div>
          <div className="h-1 w-24 bg-primary/10 rounded-full overflow-hidden">
            <div
              className="h-full bg-primary rounded-full transition-all duration-500"
              style={{ width: `${Math.round((report.orientations.length / Math.max(1, selectedOrientations.size)) * 100)}%` }}
            />
          </div>
        </div>
      )}

      {/* School legend */}
      <div className="flex flex-wrap gap-3">
        {report.schoolIds.map((sid, i) => (
          <div key={sid} className="flex items-center gap-1.5 text-xs font-bold text-on-surface">
            <span
              className="inline-block w-3 h-3 rounded-full"
              style={{ background: schoolColors[sid] ?? SCHOOL_PALETTE[i] }}
            />
            {schoolNames[sid] ?? sid.slice(0, 8)}
          </div>
        ))}
      </div>

      {/* Orientation sections (rendered progressively as they stream in) */}
      <MotionStagger className="space-y-3" delay={0.02} stagger={0.06}>
        {report.orientations.map((comp, idx) => (
          <MotionItem key={comp.orientation}>
            <OrientationSection
              comparison={comp}
              graph={graphMap[comp.orientation]}
              schoolColors={schoolColors}
              schoolNames={schoolNames}
              isCn={isCn}
              defaultExpanded={idx === 0}
            />
          </MotionItem>
        ))}
      </MotionStagger>

      {/* Final recommendation (appears last when streaming completes) */}
      {report.recommendation && (
        <MotionSurface className="border-l-4 border-primary px-5 py-4">
          <div className="flex items-center gap-2 mb-2">
            <span className="material-symbols-outlined text-lg text-primary">recommend</span>
            <span className="text-sm font-bold text-on-surface">{isCn ? '综合建议' : 'Final Recommendation'}</span>
            {report.confidence != null && (
              <span className="ml-auto text-xs font-mono text-on-surface-variant">
                {isCn ? '置信度' : 'Confidence'}: {Math.round(report.confidence * 100)}%
              </span>
            )}
          </div>
          <p className="text-sm leading-relaxed text-on-surface-variant">{report.recommendation}</p>
        </MotionSurface>
      )}
    </div>
  );
}
