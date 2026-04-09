import React, { Suspense, lazy, useState, useMemo, useEffect } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { ClaimsGraphCard } from './ClaimsGraphCard';
import { CommunityReportCard } from './CommunityReportCard';
import { useApp } from '../../context/AppContext';
import { useEvaluations } from '../../hooks/useEvaluations';
import { useOffers } from '../../hooks/useOffers';
import { useSimulations } from '../../hooks/useSimulations';
import { portfolioApi } from '../../lib/api/portfolio';
import { WhatIfDeltaCard } from './StructuredMessageCards';
import type {
  EvaluationWithSchool,
  ScenarioCompareRequestItem,
  WhatIfResponse,
  WhatIfViewModel,
} from '../../lib/types';
import {
  DASHBOARD_SELECT_EMPTY_VALUE,
  DashboardFieldLabel,
  DashboardSelect,
  DashboardSelectContent,
  DashboardSelectItem,
  DashboardSelectTrigger,
  DashboardSelectValue,
} from './ui/dashboard-select';
import { DashboardSegmentedGroup, DashboardSegmentedItem } from './ui/dashboard-segmented';
import { AnimatedWorkspacePage, MotionItem, MotionSection, MotionStagger, MotionSurface } from './WorkspaceMotion';

const LazyCausalDagD3 = lazy(() => import('./CausalDagD3').then((module) => ({ default: module.CausalDagD3 })));
const LazyComparisonReportPanel = lazy(() => import('./ComparisonReportPanel').then((module) => ({ default: module.ComparisonReportPanel })));

// ─── Priority Dimensions ───

interface Priority {
  id: string;
  icon: string;
  label: string;
  labelCn: string;
  desc: string;
  descCn: string;
  scoreKey: keyof Pick<EvaluationWithSchool, 'academic_fit' | 'financial_fit' | 'career_fit' | 'life_fit'>;
}

const PRIORITIES: Priority[] = [
  {
    id: 'academic',
    icon: 'school',
    label: 'Academic Excellence',
    labelCn: '学术实力',
    desc: 'Research opportunities, faculty quality, program ranking',
    descCn: '科研机会、师资力量、专业排名',
    scoreKey: 'academic_fit',
  },
  {
    id: 'financial',
    icon: 'savings',
    label: 'Financial Value',
    labelCn: '经济性价比',
    desc: 'Tuition, financial aid, cost of living, ROI',
    descCn: '学费、奖学金、生活成本、投资回报',
    scoreKey: 'financial_fit',
  },
  {
    id: 'career',
    icon: 'trending_up',
    label: 'Career Outcomes',
    labelCn: '就业前景',
    desc: 'Job placement, alumni network, industry connections',
    descCn: '就业率、校友网络、行业联系',
    scoreKey: 'career_fit',
  },
  {
    id: 'life',
    icon: 'favorite',
    label: 'Campus & Life',
    labelCn: '校园生活',
    desc: 'Location, campus culture, student life, safety',
    descCn: '地理位置、校园文化、学生生活、安全性',
    scoreKey: 'life_fit',
  },
];

// ─── Helpers ───

function formatPercent(v: number): string {
  return `${Math.round(v * 100)}%`;
}

export interface RankedSchool {
  eval: EvaluationWithSchool;
  weightedScore: number;
  rank: number;
  topFactor: { label: string; score: number; weight: number; contribution: number };
  factors: { label: string; score: number; weight: number; contribution: number }[];
}

function computeRanking(
  evals: EvaluationWithSchool[],
  weights: Record<string, number>,
  isCn: boolean,
): RankedSchool[] {
  const totalWeight = Object.values(weights).reduce((s, w) => s + w, 0) || 1;
  const normWeights = Object.fromEntries(
    Object.entries(weights).map(([k, w]) => [k, w / totalWeight]),
  );

  const ranked = evals.map((ev) => {
    const factors = PRIORITIES.map((p) => {
      const score = ev[p.scoreKey] ?? 0;
      const weight = normWeights[p.id] ?? 0.25;
      return {
        label: isCn ? p.labelCn : p.label,
        score,
        weight,
        contribution: score * weight,
      };
    });
    const weightedScore = factors.reduce((s, f) => s + f.contribution, 0);
    const topFactor = [...factors].sort((a, b) => b.contribution - a.contribution)[0];
    return { eval: ev, weightedScore, rank: 0, topFactor, factors };
  });

  ranked.sort((a, b) => b.weightedScore - a.weightedScore);
  ranked.forEach((r, i) => (r.rank = i + 1));
  return ranked;
}

// ─── Priority Slider ───

function PrioritySlider({ priority, value, onChange, isCn }: {
  priority: Priority;
  value: number;
  onChange: (v: number) => void;
  isCn: boolean;
}) {
  return (
    <div className="flex items-center gap-3">
      <span className="material-symbols-outlined text-primary text-base shrink-0">{priority.icon}</span>
      <div className="flex-1 min-w-0">
        <div className="flex justify-between items-center">
          <span className="text-[11px] font-bold text-on-surface">{isCn ? priority.labelCn : priority.label}</span>
          <span className="text-[11px] font-black text-primary tabular-nums">{value}%</span>
        </div>
        <input
          type="range"
          min={0} max={100} step={5}
          value={value}
          onChange={(e) => onChange(Number(e.target.value))}
          className="w-full h-1 bg-surface-container-high/40 rounded-full appearance-none cursor-pointer accent-primary"
        />
      </div>
    </div>
  );
}

// ─── Ranked School Card ───

const RANK_STYLES = [
  'bg-primary text-on-primary',
  'bg-primary/80 text-white',
  'bg-primary/60 text-white',
];

// Map priority dimension IDs to claim topics
const WEIGHT_TO_TOPICS: Record<string, string[]> = {
  academic: ['academic'],
  financial: ['financial'],
  career: ['career'],
  life: ['campus_life', 'vibe'],
};

function RankedSchoolCard({ item, isCn, majors, onAgentAnalyze, weights }: { item: RankedSchool; isCn: boolean; majors: string[]; onAgentAnalyze?: (schoolName: string) => void; weights?: Record<string, number> }) {
  const [expanded, setExpanded] = useState(false);
  const [showCommunity, setShowCommunity] = useState(false);

  // Derive relevant topics from user's priority weights (show topics where weight > 40%)
  const relevantTopics = useMemo(() => {
    if (!weights) return undefined;
    const topics: string[] = [];
    for (const [dim, w] of Object.entries(weights)) {
      if (w >= 40 && WEIGHT_TO_TOPICS[dim]) {
        topics.push(...WEIGHT_TO_TOPICS[dim]);
      }
    }
    return topics.length > 0 ? topics : undefined;
  }, [weights]);
  const ev = item.eval;

  // Find matching programs for the student's intended majors
  const matchedPrograms = useMemo(() => {
    if (!ev.school?.programs?.length || !majors.length) return [];
    const matches = [];
    for (const major of majors) {
      const ml = major.toLowerCase();
      const found = ev.school.programs.find(
        (p) => p.name.toLowerCase().includes(ml) || p.department.toLowerCase().includes(ml),
      );
      if (found && !matches.some((m) => m.id === found.id)) matches.push(found);
    }
    return matches;
  }, [ev.school?.programs, majors]);

  return (
    <motion.div layout className="dashboard-surface dashboard-hover-lift">
      <div
        className="flex items-center gap-4 p-5 cursor-pointer"
        onClick={() => setExpanded(!expanded)}
      >
        {/* Rank badge */}
        <div className={`w-10 h-10 rounded-xl flex items-center justify-center shrink-0 font-headline text-base font-black ${
          item.rank <= 3 ? RANK_STYLES[item.rank - 1] : 'bg-surface-container-high/40 text-on-surface-variant'
        }`}>
          {item.rank}
        </div>

        {/* School info */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="font-headline text-sm font-bold text-on-surface truncate">{ev.school?.name ?? t.common_school}</h3>
            {ev.school?.name_cn && (
              <span className="text-xs text-on-surface-variant/50 truncate">{ev.school.name_cn}</span>
            )}
          </div>
          <div className="flex items-center gap-1.5 mt-1 flex-wrap">
            {matchedPrograms.length > 0 ? matchedPrograms.map((prog) => (
              <span key={prog.id} className="inline-flex items-center gap-1 text-[10px] font-bold text-primary bg-primary/8 px-1.5 py-0.5 rounded">
                <span className="material-symbols-outlined text-[10px]">school</span>
                {prog.name}
                {prog.us_news_rank && (
                  <span className="text-[9px] text-primary/70">#{prog.us_news_rank}</span>
                )}
              </span>
            )) : majors.length > 0 ? (
              <span className="inline-flex items-center gap-1 text-[10px] font-bold text-on-surface-variant/60 bg-surface-container-high/40 px-1.5 py-0.5 rounded">
                <span className="material-symbols-outlined text-[10px]">school</span>
                {majors[0]}
              </span>
            ) : null}
            {matchedPrograms.some((p) => p.has_research_opps) && (
              <span className="text-[9px] text-tertiary bg-tertiary/8 px-1.5 py-0.5 rounded font-bold">
                {isCn ? '科研' : 'Research'}
              </span>
            )}
            {matchedPrograms.some((p) => p.has_coop) && (
              <span className="text-[9px] text-secondary bg-secondary/8 px-1.5 py-0.5 rounded font-bold">
                Co-op
              </span>
            )}
            <span className="text-[10px] text-on-surface-variant/50">
              {ev.school?.city}, {ev.school?.state}
            </span>
          </div>
        </div>

        {/* Score */}
        <div className="text-right shrink-0">
          <div className="text-xl font-black text-primary">{formatPercent(item.weightedScore)}</div>
          <div className="text-[8px] font-bold text-on-surface-variant/50 uppercase tracking-widest">
            {isCn ? '综合匹配' : 'Match'}
          </div>
        </div>

        {/* Tier badge */}
        <span className={`px-2 py-1 text-[8px] font-black uppercase tracking-widest rounded-md shrink-0 ${
          ev.tier === 'reach' ? 'bg-secondary-fixed/50 text-on-secondary-fixed-variant'
            : ev.tier === 'target' ? 'bg-tertiary-fixed/50 text-on-tertiary-fixed-variant'
            : ev.tier === 'safety' ? 'bg-primary/10 text-primary'
            : 'bg-tertiary/10 text-tertiary'
        }`}>
          {ev.tier}
        </span>

        {/* Expand */}
        <span className={`material-symbols-outlined text-on-surface-variant/30 text-lg transition-transform ${expanded ? 'rotate-180' : ''}`}>
          expand_more
        </span>
      </div>

      {/* Expanded: Causal reasoning */}
      <AnimatePresence initial={false}>
      {expanded && (
        <motion.div
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: 'auto' }}
          exit={{ opacity: 0, height: 0 }}
          transition={{ duration: 0.34, ease: [0.22, 1, 0.36, 1] }}
          className="overflow-hidden"
        >
          <div className="px-5 pb-5 pt-1 border-t border-outline-variant/10 space-y-4">
          {/* Factor breakdown */}
          <div>
            <div className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-3">
              {isCn ? '因果分析 — 为什么排在这里' : 'Causal Analysis — Why this ranking'}
            </div>
            <div className="space-y-2.5">
              {item.factors
                .sort((a, b) => b.contribution - a.contribution)
                .map((f) => (
                <div key={f.label}>
                  <div className="flex justify-between items-center mb-1">
                    <span className="text-xs text-on-surface">{f.label}</span>
                    <div className="flex items-center gap-3">
                      <span className="text-[10px] text-on-surface-variant/50">
                        {isCn ? '得分' : 'Score'} {formatPercent(f.score)} × {isCn ? '权重' : 'Weight'} {Math.round(f.weight * 100)}%
                      </span>
                      <span className="text-xs font-black text-primary w-10 text-right">{formatPercent(f.contribution)}</span>
                    </div>
                  </div>
                  <div className="h-1.5 bg-surface-container-high/40 rounded-full overflow-hidden">
                    <div className="h-full rounded-full bg-primary transition-all duration-500" style={{ width: `${Math.round(f.contribution * 100)}%` }} />
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Key metrics */}
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
              <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">{isCn ? '录取概率' : 'Admit Prob'}</div>
              <div className="text-sm font-black text-primary">{formatPercent(ev.admission_probability ?? 0)}</div>
            </div>
            <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
              <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">{isCn ? '净价' : 'Net Price'}</div>
              <div className="text-sm font-black text-on-surface">{ev.school?.avg_net_price ? `$${(ev.school.avg_net_price / 1000).toFixed(0)}K` : '—'}</div>
            </div>
            <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
              <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">SAT</div>
              <div className="text-sm font-black text-on-surface">{ev.school?.sat_25 && ev.school?.sat_75 ? `${ev.school.sat_25}–${ev.school.sat_75}` : '—'}</div>
            </div>
            <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
              <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">ED/EA</div>
              <div className="text-sm font-black text-on-surface uppercase">{ev.ed_ea_recommendation ?? '—'}</div>
            </div>
          </div>

          {/* AI Reasoning */}
          {ev.reasoning && (
            <div className="rounded-2xl bg-surface-container-low/50 p-4 shadow-sm ring-1 ring-outline-variant/8">
              <div className="flex items-center gap-2 mb-2">
                <span className="material-symbols-outlined text-tertiary text-base" style={{ fontVariationSettings: "'FILL' 1" }}>psychology</span>
                <span className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">
                  {isCn ? 'AI 推理' : 'AI Reasoning'}
                </span>
              </div>
              <p className="text-sm text-on-surface/80 leading-relaxed">{ev.reasoning}</p>
            </div>
          )}

          {/* Community Insights (expandable) */}
          <div>
            <button
              onClick={() => setShowCommunity(!showCommunity)}
              className="flex w-full items-center justify-center gap-2 rounded-xl border border-outline-variant/12 bg-surface-container-lowest py-2 text-[11px] font-bold text-on-surface-variant/60 transition hover:bg-surface-container-low hover:text-on-surface-variant"
            >
              <span className="material-symbols-outlined text-sm">{showCommunity ? 'expand_less' : 'forum'}</span>
              {showCommunity ? (isCn ? '收起社媒评价' : 'Collapse') : (isCn ? '查看社媒真实评价' : 'Community Reviews')}
            </button>
            {showCommunity && (
              <div className="mt-3 space-y-3">
                <CommunityReportCard schoolId={ev.school_id} />
                <ClaimsGraphCard schoolId={ev.school_id} relevantTopics={relevantTopics} />
              </div>
            )}
          </div>

          {/* Agent deep analysis button */}
          {onAgentAnalyze && (
            <button
              onClick={() => onAgentAnalyze(ev.school?.name ?? '')}
              className="flex w-full items-center justify-center gap-2 rounded-xl bg-primary/5 border border-primary/15 py-2.5 text-xs font-bold text-primary transition hover:bg-primary/10"
            >
              <span className="material-symbols-outlined text-sm">auto_awesome</span>
              {isCn ? 'AI 深度分析这所学校' : 'AI Deep Analysis'}
            </button>
          )}
          </div>
        </motion.div>
      )}
      </AnimatePresence>
    </motion.div>
  );
}

// ─── Preset Quick Picks ───

interface Preset {
  id: string;
  icon: string;
  label: string;
  labelCn: string;
  weights: Record<string, number>;
}

const PRESETS: Preset[] = [
  { id: 'balanced', icon: 'balance', label: 'Balanced', labelCn: '均衡', weights: { academic: 50, financial: 50, career: 50, life: 50 } },
  { id: 'scholar', icon: 'science', label: 'Research-Focused', labelCn: '科研导向', weights: { academic: 90, financial: 30, career: 50, life: 20 } },
  { id: 'practical', icon: 'work', label: 'Career-First', labelCn: '就业优先', weights: { academic: 40, financial: 50, career: 95, life: 30 } },
  { id: 'value', icon: 'savings', label: 'Best Value', labelCn: '最高性价比', weights: { academic: 40, financial: 95, career: 40, life: 40 } },
  { id: 'experience', icon: 'emoji_people', label: 'Best Experience', labelCn: '最佳体验', weights: { academic: 30, financial: 20, career: 30, life: 95 } },
];

type ScenarioNodeKey =
  | 'student_ability'
  | 'financial_aid'
  | 'research_opportunities'
  | 'brand_signal'
  | 'career_services'
  | 'peer_network'
  | 'location_effect'
  | 'family_ses';

const SCENARIO_NODE_KEYS: ScenarioNodeKey[] = [
  'student_ability',
  'financial_aid',
  'research_opportunities',
  'brand_signal',
  'career_services',
  'peer_network',
  'location_effect',
  'family_ses',
];

interface ScenarioInterventionDraft {
  id: string;
  key: ScenarioNodeKey;
  value: number;
}

interface ScenarioDraft {
  id: string;
  name: string;
  schoolId: string | null;
  interventions: ScenarioInterventionDraft[];
}

const SCENARIO_DEFAULTS: Array<{ key: ScenarioNodeKey; value: number }> = [
  { key: 'financial_aid', value: 1.0 },
  { key: 'research_opportunities', value: 0.8 },
  { key: 'career_services', value: 0.85 },
];

function createLocalId(prefix: string): string {
  return `${prefix}-${crypto.randomUUID?.() ?? Math.random().toString(36).slice(2)}`;
}

function createInterventionDraft(key: ScenarioNodeKey, value: number): ScenarioInterventionDraft {
  return {
    id: createLocalId('scenario-intervention'),
    key,
    value,
  };
}

function createScenarioDraft(index: number, isCn = false): ScenarioDraft {
  const fallback = SCENARIO_DEFAULTS[index] ?? SCENARIO_DEFAULTS[SCENARIO_DEFAULTS.length - 1];
  return {
    id: createLocalId('scenario'),
    name: isCn ? `场景 ${String.fromCharCode(65 + index)}` : `Scenario ${String.fromCharCode(65 + index)}`,
    schoolId: null,
    interventions: [createInterventionDraft(fallback.key, fallback.value)],
  };
}

function buildScenarioComparePayload(
  scenarios: ScenarioDraft[],
): ScenarioCompareRequestItem[] {
  return scenarios.map((scenario) => ({
    school_id: scenario.schoolId!,
    label: scenario.name,
    interventions: scenario.interventions.reduce<Record<string, number>>((acc, intervention) => {
      acc[intervention.key] = Number(intervention.value.toFixed(2));
      return acc;
    }, {}),
  }));
}

interface ScenarioSchoolOption {
  id: string;
  label: string;
  rank: number;
}

function mapSimulationResultToViewModel(result: WhatIfResponse): WhatIfViewModel {
  return {
    deltas: Object.entries(result.deltas ?? {}).map(([key, value]) => ({
      key,
      value,
    })),
    explanation: result.explanation,
    suggestions: [],
  };
}

function localizeScenarioNodeLabel(key: ScenarioNodeKey, t: Record<string, any>): string {
  const labels: Record<ScenarioNodeKey, string> = {
    student_ability: t.dec_student_ability,
    financial_aid: t.dec_financial_aid,
    research_opportunities: t.dec_research,
    brand_signal: t.dec_brand_signal,
    career_services: t.dec_career_services,
    peer_network: t.dec_peer_network,
    location_effect: t.dec_location,
    family_ses: t.dec_family_ses,
  };
  return labels[key];
}

function ScenarioDraftCard({
  scenario,
  schoolOptions,
  isCn,
  t,
  canRemoveScenario,
  onRemoveScenario,
  onChangeSchool,
  onAddIntervention,
  onRemoveIntervention,
  onChangeIntervention,
}: {
  scenario: ScenarioDraft;
  schoolOptions: ScenarioSchoolOption[];
  isCn: boolean;
  t: Record<string, any>;
  canRemoveScenario: boolean;
  onRemoveScenario: () => void;
  onChangeSchool: (schoolId: string | null) => void;
  onAddIntervention: () => void;
  onRemoveIntervention: (interventionId: string) => void;
  onChangeIntervention: (interventionId: string, patch: Partial<Omit<ScenarioInterventionDraft, 'id'>>) => void;
}) {
  return (
    <div className="rounded-2xl bg-surface-container-lowest p-4 shadow-sm ring-1 ring-outline-variant/8">
      <div className="mb-4 flex items-center justify-between gap-3">
        <div>
          <div className="text-sm font-black text-on-surface">{scenario.name}</div>
          <div className="mt-1 text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/55">
            {scenario.interventions.length} {isCn ? '个干预项' : 'interventions'}
          </div>
        </div>
        {canRemoveScenario && (
          <button
            onClick={onRemoveScenario}
            className="inline-flex items-center gap-1 rounded-lg border border-outline-variant/10 px-3 py-1.5 text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant transition-colors hover:bg-surface-container-high/50"
          >
            <span className="material-symbols-outlined text-sm">delete</span>
            {t.dec_compare_remove}
          </button>
        )}
      </div>

      <div className="space-y-3">
        <div className="rounded-2xl bg-white px-4 py-3 shadow-sm ring-1 ring-outline-variant/8">
          <DashboardFieldLabel>{t.dec_compare_school}</DashboardFieldLabel>
          <DashboardSelect
            value={scenario.schoolId ?? undefined}
            onValueChange={(value) => {
              onChangeSchool(value === DASHBOARD_SELECT_EMPTY_VALUE ? null : value);
            }}
          >
            <DashboardSelectTrigger>
              <DashboardSelectValue placeholder={t.dec_compare_pick_school} />
            </DashboardSelectTrigger>
            <DashboardSelectContent>
              <DashboardSelectItem value={DASHBOARD_SELECT_EMPTY_VALUE}>
                {t.dec_compare_pick_school}
              </DashboardSelectItem>
              {schoolOptions.map((schoolOption) => (
                <DashboardSelectItem key={schoolOption.id} value={schoolOption.id}>
                  #{schoolOption.rank} {schoolOption.label}
                </DashboardSelectItem>
              ))}
            </DashboardSelectContent>
          </DashboardSelect>
        </div>

        {scenario.interventions.map((intervention) => (
          <div key={intervention.id} className="rounded-2xl bg-white px-4 py-3 shadow-sm ring-1 ring-outline-variant/8">
            <div className="flex items-start gap-3">
              <div className="min-w-0 flex-1 space-y-3">
                <div>
                  <DashboardFieldLabel>{t.dec_compare_variable}</DashboardFieldLabel>
                  <DashboardSelect
                    value={intervention.key}
                    onValueChange={(value) =>
                      onChangeIntervention(intervention.id, { key: value as ScenarioNodeKey })
                    }
                  >
                    <DashboardSelectTrigger>
                      <DashboardSelectValue placeholder={t.dec_compare_variable} />
                    </DashboardSelectTrigger>
                    <DashboardSelectContent>
                      {SCENARIO_NODE_KEYS.map((key) => (
                        <DashboardSelectItem key={key} value={key}>
                          {localizeScenarioNodeLabel(key, t)}
                        </DashboardSelectItem>
                      ))}
                    </DashboardSelectContent>
                  </DashboardSelect>
                </div>

                <div>
                  <div className="mb-1 flex items-center justify-between gap-3 text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/55">
                    <span>{t.dec_compare_strength}</span>
                    <span className="text-primary">{Math.round(intervention.value * 100)}%</span>
                  </div>
                  <input
                    type="range"
                    min={0}
                    max={1}
                    step={0.05}
                    value={intervention.value}
                    onChange={(event) => onChangeIntervention(intervention.id, { value: Number(event.target.value) })}
                    className="w-full cursor-pointer appearance-none rounded-full accent-primary"
                  />
                </div>
              </div>

              {scenario.interventions.length > 1 && (
                <button
                  onClick={() => onRemoveIntervention(intervention.id)}
                  className="mt-6 inline-flex h-9 w-9 items-center justify-center rounded-full border border-outline-variant/10 text-on-surface-variant transition-colors hover:bg-surface-container-high/50"
                >
                  <span className="material-symbols-outlined text-sm">remove</span>
                </button>
              )}
            </div>
          </div>
        ))}
      </div>

      {scenario.interventions.length < 3 && (
        <button
          onClick={onAddIntervention}
          className="mt-4 inline-flex items-center gap-2 rounded-xl border border-primary/15 bg-primary/5 px-3 py-2 text-xs font-bold text-primary transition-colors hover:bg-primary/10"
        >
          <span className="material-symbols-outlined text-sm">add</span>
          {t.dec_compare_add_intervention}
        </button>
      )}
    </div>
  );
}

// ─── Community Insight Row (expandable per school) ───

function CommunityInsightRow({ schoolId, schoolName, schoolNameCn, rank }: {
  schoolId: string;
  schoolName: string;
  schoolNameCn?: string;
  rank: number;
}) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="rounded-xl border border-outline-variant/10 bg-white overflow-hidden">
      <button
        className="flex w-full items-center gap-3 px-4 py-3 text-left transition hover:bg-surface-container-high/20"
        onClick={() => setExpanded(!expanded)}
      >
        <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-primary/10 text-xs font-black text-primary">
          {rank}
        </span>
        <div className="flex-1 min-w-0">
          <span className="text-sm font-bold text-on-surface">{schoolName}</span>
          {schoolNameCn && <span className="ml-2 text-xs text-on-surface-variant/50">{schoolNameCn}</span>}
        </div>
        <span className="material-symbols-outlined text-sm text-on-surface-variant/40 transition-transform" style={{ transform: expanded ? 'rotate(180deg)' : '' }}>
          expand_more
        </span>
      </button>
      {expanded && (
        <div className="px-4 pb-4 space-y-3 border-t border-outline-variant/8">
          <div className="pt-3">
            <CommunityReportCard schoolId={schoolId} />
          </div>
          <ClaimsGraphCard schoolId={schoolId} />
        </div>
      )}
    </div>
  );
}

// ─── Causal DAG Section ───

function CausalDagSection({ ranked, studentId, isCn, t }: {
  ranked: RankedSchool[];
  studentId: string | null;
  isCn: boolean;
  t: Record<string, any>;
}) {
  const [dagSchoolId, setDagSchoolId] = useState<string>('');
  const [showDag, setShowDag] = useState(false);

  // Deduplicated school list
  const schools = useMemo(() => {
    const seen = new Set<string>();
    return ranked.filter((r) => {
      if (seen.has(r.eval.school_id)) return false;
      seen.add(r.eval.school_id);
      return true;
    });
  }, [ranked]);

  return (
    <div className="mt-8 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="material-symbols-outlined text-primary text-xl">account_tree</span>
          <div>
            <h3 className="font-headline text-sm font-black text-on-surface">
              {isCn ? '因果推理可视化' : 'Causal Inference Visualization'}
            </h3>
            <p className="text-[10px] text-on-surface-variant/50">
              {isCn ? '查看AI如何评估每所学校的因果关系' : 'See how the AI evaluates causal pathways for each school'}
            </p>
          </div>
        </div>
        <button
          onClick={() => setShowDag(!showDag)}
          className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-colors flex items-center gap-1.5 ${
            showDag ? 'bg-primary text-on-primary' : 'bg-primary/5 text-primary border border-primary/15 hover:bg-primary/10'
          }`}
        >
          <span className="material-symbols-outlined text-sm">{showDag ? 'visibility_off' : 'visibility'}</span>
          {showDag ? (isCn ? '隐藏' : 'Hide') : (isCn ? '显示因果图' : 'Show DAG')}
        </button>
      </div>

      {showDag && (
        <div className="space-y-3">
          {/* School selector */}
          <div className="max-w-md">
            <DashboardFieldLabel>{t.common_school}</DashboardFieldLabel>
            <DashboardSelect
              value={dagSchoolId || undefined}
              onValueChange={(value) => {
                setDagSchoolId(value === DASHBOARD_SELECT_EMPTY_VALUE ? '' : value);
              }}
            >
              <DashboardSelectTrigger>
                <DashboardSelectValue placeholder={t.dec_dag_empty} />
              </DashboardSelectTrigger>
              <DashboardSelectContent>
                <DashboardSelectItem value={DASHBOARD_SELECT_EMPTY_VALUE}>
                  {t.dec_dag_empty}
                </DashboardSelectItem>
                {schools.map((r) => (
                  <DashboardSelectItem key={r.eval.school_id} value={r.eval.school_id}>
                    #{r.rank} {r.eval.school?.name ?? t.common_school}
                  </DashboardSelectItem>
                ))}
              </DashboardSelectContent>
            </DashboardSelect>
          </div>

          {dagSchoolId && studentId && (
            <Suspense
              fallback={(
                <div className="rounded-3xl border border-outline-variant/10 bg-surface-container-lowest p-6 shadow-sm">
                  <div className="mb-3 h-3 w-32 animate-pulse rounded-full bg-surface-container-high/60" />
                  <div className="h-[360px] animate-pulse rounded-[1.5rem] bg-surface-container-high/45" />
                </div>
              )}
            >
              <LazyCausalDagD3
                studentId={studentId}
                schoolId={dagSchoolId}
                t={t}
              />
            </Suspense>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Main Panel ───

interface DecisionsPanelProps {
  studentId: string | null;
}

export function DecisionsPanel({ studentId }: DecisionsPanelProps) {
  const { t, locale, setActiveNav } = useApp();
  const isCn = locale === 'zh';
  const { tieredList, isLoading } = useEvaluations(studentId);
  const { offers } = useOffers(studentId);
  const {
    comparisonResult,
    isLoading: isComparingScenarios,
    error: simulationError,
    compareScenarios,
    clearComparison,
  } = useSimulations();

  const [majors, setMajors] = useState<string[]>([]);
  useEffect(() => {
    if (!studentId) return;
    portfolioApi.get(studentId).then((p) => {
      setMajors(p.academics.intended_majors ?? []);
    }).catch(() => {});
  }, [studentId]);

  const [viewMode, setViewMode] = useState<'ranking' | 'comparison'>('ranking');
  const [weights, setWeights] = useState<Record<string, number>>({
    academic: 50,
    financial: 50,
    career: 50,
    life: 50,
  });
  const [activePreset, setActivePreset] = useState<string>('balanced');
  const [scenarios, setScenarios] = useState<ScenarioDraft[]>([
    createScenarioDraft(0, isCn),
    createScenarioDraft(1, isCn),
  ]);

  const allEvals = useMemo<EvaluationWithSchool[]>(() => {
    if (!tieredList) return [];
    const all = [...tieredList.reach, ...tieredList.target, ...tieredList.safety, ...tieredList.likely];
    // Deduplicate by school_id — keep highest overall_score per school
    const best = new Map<string, EvaluationWithSchool>();
    for (const ev of all) {
      const prev = best.get(ev.school_id);
      if (!prev || ev.overall_score > prev.overall_score) {
        best.set(ev.school_id, ev);
      }
    }
    return Array.from(best.values());
  }, [tieredList]);

  const ranked = useMemo(
    () => computeRanking(allEvals, weights, isCn),
    [allEvals, weights, isCn],
  );

  const handleAgentAnalyze = (schoolName: string) => {
    const msg = isCn
      ? `请帮我深度分析 ${schoolName}，结合社区评价、因果数据、我的档案，给出综合建议。`
      : `Please do a deep analysis of ${schoolName}, combining community reviews, causal data, and my profile.`;
    localStorage.setItem('sp_pending_advisor_message', msg);
    setActiveNav('advisor');
  };

  // Split into admitted (have offers with status=admitted) vs prospect
  const admittedSchoolIds = useMemo(() => {
    return new Set(offers.filter((o) => o.status === 'admitted').map((o) => o.school_id));
  }, [offers]);

  const rankedAdmitted = useMemo(
    () => ranked.filter((item) => admittedSchoolIds.has(item.eval.school_id))
      .map((item, idx) => ({ ...item, rank: idx + 1 })),
    [ranked, admittedSchoolIds],
  );

  const rankedProspect = useMemo(
    () => ranked.filter((item) => !admittedSchoolIds.has(item.eval.school_id))
      .map((item, idx) => ({ ...item, rank: idx + 1 })),
    [ranked, admittedSchoolIds],
  );
  const scenarioSchoolOptions = useMemo<ScenarioSchoolOption[]>(() => {
    const seen = new Set<string>();
    return ranked
      .filter((item) => {
        if (seen.has(item.eval.school_id)) return false;
        seen.add(item.eval.school_id);
        return true;
      })
      .map((item) => ({
        id: item.eval.school_id,
        rank: item.rank,
        label: item.eval.school?.name ?? `${t.common_school} #${item.rank}`,
      }));
  }, [ranked, t.common_school]);
  const missingScenarioSchool = scenarios.some((scenario) => !scenario.schoolId);

  const handlePreset = (preset: Preset) => {
    setWeights({ ...preset.weights });
    setActivePreset(preset.id);
  };

  const handleWeightChange = (id: string, value: number) => {
    setWeights((prev) => ({ ...prev, [id]: value }));
    setActivePreset('');
  };

  const handleAddScenario = () => {
    setScenarios((prev) => {
      if (prev.length >= 3) return prev;
      return [...prev, createScenarioDraft(prev.length, isCn)];
    });
  };

  const handleRemoveScenario = (scenarioId: string) => {
    setScenarios((prev) => prev
      .filter((scenario) => scenario.id !== scenarioId)
      .map((scenario, index) => ({
        ...scenario,
        name: isCn ? `场景 ${String.fromCharCode(65 + index)}` : `Scenario ${String.fromCharCode(65 + index)}`,
      })));
  };

  const handleAddIntervention = (scenarioId: string) => {
    setScenarios((prev) => prev.map((scenario) => {
      if (scenario.id !== scenarioId || scenario.interventions.length >= 3) return scenario;
      return {
        ...scenario,
        interventions: [...scenario.interventions, createInterventionDraft('student_ability', 0.5)],
      };
    }));
  };

  const handleRemoveIntervention = (scenarioId: string, interventionId: string) => {
    setScenarios((prev) => prev.map((scenario) => {
      if (scenario.id !== scenarioId || scenario.interventions.length <= 1) return scenario;
      return {
        ...scenario,
        interventions: scenario.interventions.filter((intervention) => intervention.id !== interventionId),
      };
    }));
  };

  const handleInterventionChange = (
    scenarioId: string,
    interventionId: string,
    patch: Partial<Omit<ScenarioInterventionDraft, 'id'>>,
  ) => {
    setScenarios((prev) => prev.map((scenario) => {
      if (scenario.id !== scenarioId) return scenario;
      return {
        ...scenario,
        interventions: scenario.interventions.map((intervention) => (
          intervention.id === interventionId ? { ...intervention, ...patch } : intervention
        )),
      };
    }));
  };

  const handleScenarioSchoolChange = (scenarioId: string, schoolId: string | null) => {
    setScenarios((prev) => prev.map((scenario) => (
      scenario.id === scenarioId ? { ...scenario, schoolId } : scenario
    )));
  };

  const handleCompareScenarios = async () => {
    if (!studentId) return;
    if (missingScenarioSchool) return;
    await compareScenarios(studentId, buildScenarioComparePayload(scenarios));
  };

  return (
    <AnimatedWorkspacePage className="w-full bg-background font-body">
      <section className="w-full bg-background flex flex-col h-full overflow-hidden font-body">
      <header className="sticky top-0 z-20 flex min-h-16 items-center justify-between border-b border-outline-variant/10 bg-background/90 px-4 py-3 backdrop-blur-md sm:px-6 lg:px-8">
        <MotionSection role="toolbar">
          <div>
          <h1 className="font-headline text-lg font-black text-on-surface tracking-tight">
            {isCn ? '智能择校' : 'Smart Ranking'}
          </h1>
          <p className="text-[9px] text-on-surface-variant font-bold tracking-[0.1em] uppercase">
            {isCn ? '基于因果推理的个性化排名' : 'Personalized ranking powered by causal inference'}
            {ranked.length > 0 && ` • ${ranked.length} ${isCn ? '所学校' : 'Schools'}`}
          </p>
          </div>
        </MotionSection>
      </header>

      {/* View mode tabs */}
      <div className="border-b border-outline-variant/10 px-4 py-2 sm:px-6 lg:px-8">
        <DashboardSegmentedGroup
          type="single"
          value={viewMode}
          onValueChange={(v) => v && setViewMode(v as 'ranking' | 'comparison')}
          className="inline-flex"
          size="compact"
        >
          <DashboardSegmentedItem value="ranking" className="gap-1.5">
            <span className="material-symbols-outlined text-sm">leaderboard</span>
            {isCn ? '快速排名' : 'Quick Ranking'}
          </DashboardSegmentedItem>
          <DashboardSegmentedItem value="comparison" className="gap-1.5">
            <span className="material-symbols-outlined text-sm">compare</span>
            {isCn ? '深度对比' : 'Deep Comparison'}
          </DashboardSegmentedItem>
        </DashboardSegmentedGroup>
      </div>

      {viewMode === 'comparison' ? (
        <div className="flex-1 overflow-y-auto px-4 py-5 sm:px-6 lg:px-8">
          <Suspense fallback={<div className="animate-pulse bg-surface-container-high/60 rounded-2xl h-64" />}>
            <LazyComparisonReportPanel
              studentId={studentId}
              rankedSchools={ranked}
              majors={majors}
              admittedSchoolIds={admittedSchoolIds}
              isCn={isCn}
              t={t}
            />
          </Suspense>
        </div>
      ) : (
      <div className="flex-1 overflow-y-auto">
        <div className="flex h-full flex-col lg:flex-row">
          {/* Left: Priority Controls */}
          <MotionStagger className="w-full shrink-0 border-b border-outline-variant/10 p-4 space-y-4 overflow-y-auto sm:p-5 lg:w-72 lg:border-b-0 lg:border-r" delay={0.03} stagger={0.06}>
            {/* Quick presets */}
            <MotionItem role="toolbar">
            <div>
              <div className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-3">
                {isCn ? '快速选择' : 'Quick Presets'}
              </div>
              <DashboardSegmentedGroup
                type="single"
                value={activePreset ?? ''}
                onValueChange={(value) => {
                  if (!value) return;
                  const preset = PRESETS.find((item) => item.id === value);
                  if (preset) handlePreset(preset);
                }}
              >
                {PRESETS.map((preset) => (
                  <DashboardSegmentedItem
                    key={preset.id}
                    value={preset.id}
                    accent="primary"
                    size="compact"
                    className="min-h-9"
                  >
                    <span className="material-symbols-outlined text-sm">{preset.icon}</span>
                    {isCn ? preset.labelCn : preset.label}
                  </DashboardSegmentedItem>
                ))}
              </DashboardSegmentedGroup>
            </div>
            </MotionItem>

            {/* Priority sliders */}
            <MotionItem role="section">
            <MotionSurface className="p-4">
              <div className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-2">
                {isCn ? '调整你的优先级' : 'Your Priorities'}
              </div>
              <div className="space-y-3">
                {PRIORITIES.map((p) => (
                  <PrioritySlider
                    key={p.id}
                    priority={p}
                    value={weights[p.id]}
                    onChange={(v) => handleWeightChange(p.id, v)}
                    isCn={isCn}
                  />
                ))}
              </div>
            </MotionSurface>
            </MotionItem>

            <MotionItem role="section">
            <MotionSurface className="p-4 sm:p-5">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div>
                  <div className="text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/55">
                    {t.dec_compare_kicker}
                  </div>
                  <h3 className="mt-1 font-headline text-sm font-black text-on-surface">
                    {t.dec_compare_title}
                  </h3>
                  <p className="mt-1 text-xs leading-relaxed text-on-surface-variant/70">
                    {t.dec_compare_desc}
                  </p>
                </div>
                <button
                  onClick={() => {
                    setScenarios([createScenarioDraft(0, isCn), createScenarioDraft(1, isCn)]);
                    clearComparison();
                  }}
                  className="inline-flex items-center gap-1 rounded-lg border border-outline-variant/10 px-3 py-1.5 text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant transition-colors hover:bg-surface-container-high/50"
                >
                  <span className="material-symbols-outlined text-sm">restart_alt</span>
                  {t.dec_compare_reset}
                </button>
              </div>

              <div className="mt-4 space-y-4">
                {scenarios.map((scenario) => (
                  <ScenarioDraftCard
                    key={scenario.id}
                    scenario={scenario}
                    schoolOptions={scenarioSchoolOptions}
                    isCn={isCn}
                    t={t}
                    canRemoveScenario={scenarios.length > 2}
                    onRemoveScenario={() => handleRemoveScenario(scenario.id)}
                    onChangeSchool={(schoolId) => handleScenarioSchoolChange(scenario.id, schoolId)}
                    onAddIntervention={() => handleAddIntervention(scenario.id)}
                    onRemoveIntervention={(interventionId) => handleRemoveIntervention(scenario.id, interventionId)}
                    onChangeIntervention={(interventionId, patch) => handleInterventionChange(scenario.id, interventionId, patch)}
                  />
                ))}
              </div>

              {scenarioSchoolOptions.length === 0 && (
                <div className="mt-4 rounded-2xl border border-outline-variant/10 bg-surface-container-high/30 px-4 py-3 text-xs text-on-surface-variant/70">
                  {t.dec_compare_school_empty}
                </div>
              )}

              {scenarioSchoolOptions.length > 0 && missingScenarioSchool && (
                <div className="mt-4 rounded-2xl border border-outline-variant/10 bg-surface-container-high/30 px-4 py-3 text-xs text-on-surface-variant/70">
                  {t.dec_compare_school_required}
                </div>
              )}

              {scenarios.length < 3 && (
                <button
                  onClick={handleAddScenario}
                  className="mt-4 inline-flex items-center gap-2 rounded-xl border border-primary/15 bg-primary/5 px-3 py-2 text-xs font-bold text-primary transition-colors hover:bg-primary/10"
                >
                  <span className="material-symbols-outlined text-sm">add</span>
                  {t.dec_compare_add_scenario}
                </button>
              )}

              {simulationError && (
                <div className="mt-4 rounded-2xl border border-error/15 bg-error/5 px-4 py-3 text-sm text-error">
                  {simulationError.message}
                </div>
              )}

              <button
                onClick={() => { void handleCompareScenarios(); }}
                disabled={!studentId || isComparingScenarios || missingScenarioSchool}
                className="mt-4 inline-flex w-full items-center justify-center gap-2 rounded-xl bg-primary px-4 py-3 text-sm font-bold text-on-primary shadow-md transition-all hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <span className="material-symbols-outlined text-sm">compare_arrows</span>
                {isComparingScenarios ? t.dec_compare_running : t.dec_compare_run}
              </button>
            </MotionSurface>
            </MotionItem>
          </MotionStagger>

          {/* Right: Ranked Results */}
          <div className="flex-1 overflow-y-auto px-4 py-5 sm:px-6 sm:py-6 lg:px-8">
            {isLoading && (
              <div className="space-y-3">
                {[...Array(4)].map((_, i) => (
                  <div key={i} className="animate-pulse bg-surface-container-high/60 rounded-2xl h-20" />
                ))}
              </div>
            )}

            {!isLoading && ranked.length === 0 && (
              <MotionSurface className="flex flex-col items-center justify-center py-24 text-center">
                <div className="w-20 h-20 rounded-3xl bg-surface-container-high/40 flex items-center justify-center mb-6">
                  <span className="material-symbols-outlined text-4xl text-on-surface-variant/50">leaderboard</span>
                </div>
                <h3 className="font-headline text-xl font-black text-on-surface mb-2">
                  {isCn ? '暂无学校数据' : 'No Schools Yet'}
                </h3>
                <p className="text-sm text-on-surface-variant/70 max-w-sm leading-relaxed">
                  {isCn ? '先在选校列表中添加学校，然后回来查看个性化排名。' : 'Add schools to your School List first, then come back to see your personalized ranking.'}
                </p>
              </MotionSurface>
            )}

            {!isLoading && comparisonResult && (
              <MotionSection delay={0.08}>
              <div className="mb-6 rounded-3xl bg-surface-container-lowest p-6 shadow-sm ring-1 ring-outline-variant/8">
                <div className="mb-4">
                  <div className="text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/55">
                    {t.dec_compare_results_kicker}
                  </div>
                  <h2 className="mt-1 font-headline text-lg font-black text-on-surface">
                    {t.dec_compare_results_title}
                  </h2>
                  <p className="mt-2 text-sm leading-relaxed text-on-surface-variant/70">
                    {comparisonResult.summary}
                  </p>
                </div>

                <div className="grid grid-cols-1 gap-4 2xl:grid-cols-2">
                  {comparisonResult.results.map((result, index) => (
                    <WhatIfDeltaCard
                      key={`scenario-result-${index}`}
                      data={mapSimulationResultToViewModel(result as WhatIfResponse)}
                      kicker={{ en: `Scenario ${String.fromCharCode(65 + index)}`, zh: `场景 ${String.fromCharCode(65 + index)}` }}
                      title={{ en: `${scenarios[index]?.name ?? `Scenario ${String.fromCharCode(65 + index)}`} Deltas`, zh: `${scenarios[index]?.name ?? `场景 ${String.fromCharCode(65 + index)}`} 变化` }}
                      description={{ en: 'Side-by-side outcome deltas for this configured scenario.', zh: '展示当前配置场景下的关键结果变化。' }}
                      showSuggestions={false}
                    />
                  ))}
                </div>
              </div>
              </MotionSection>
            )}

            {/* Admitted section */}
            {!isLoading && rankedAdmitted.length > 0 && (
              <div className="space-y-3">
                <div className="flex items-center gap-2 px-1">
                  <span className="material-symbols-outlined text-tertiary text-lg">verified</span>
                  <h2 className="font-headline text-sm font-black text-on-surface">
                    {isCn ? '已拿到 Offer' : 'Admitted'}
                  </h2>
                  <span className="text-[10px] font-bold text-on-surface-variant/50">{rankedAdmitted.length}</span>
                </div>
                <MotionStagger className="space-y-2" delay={0.1} stagger={0.06}>
                  {rankedAdmitted.map((item) => (
                    <MotionItem key={item.eval.id} role="surface">
                      <RankedSchoolCard item={item} isCn={isCn} majors={majors} onAgentAnalyze={handleAgentAnalyze} weights={weights} />
                    </MotionItem>
                  ))}
                </MotionStagger>
              </div>
            )}

            {/* Prospect section */}
            {!isLoading && rankedProspect.length > 0 && (
              <div className="space-y-3">
                <div className="flex items-center gap-2 px-1">
                  <span className="material-symbols-outlined text-on-surface-variant/60 text-lg">explore</span>
                  <h2 className="font-headline text-sm font-black text-on-surface">
                    {isCn ? '待申请 / 评估中' : 'Prospects'}
                  </h2>
                  <span className="text-[10px] font-bold text-on-surface-variant/50">{rankedProspect.length}</span>
                </div>
                <MotionStagger className="space-y-2" delay={0.1} stagger={0.06}>
                  {rankedProspect.map((item) => (
                    <MotionItem key={item.eval.id} role="surface">
                      <RankedSchoolCard item={item} isCn={isCn} majors={majors} onAgentAnalyze={handleAgentAnalyze} weights={weights} />
                    </MotionItem>
                  ))}
                </MotionStagger>
              </div>
            )}

            {/* Causal DAG Section */}
            {!isLoading && ranked.length > 0 && (
              <MotionSection delay={0.12}>
                <CausalDagSection ranked={ranked} studentId={studentId} isCn={isCn} t={t} />
              </MotionSection>
            )}

            {/* Community Insights for top schools */}
            {!isLoading && ranked.length > 0 && (
              <MotionSection delay={0.14}>
                <div className="space-y-3">
                  <div className="flex items-center gap-2 px-1">
                    <span className="material-symbols-outlined text-primary text-lg">forum</span>
                    <h2 className="font-headline text-sm font-black text-on-surface">
                      {isCn ? '社区深度洞察' : 'Community Insights'}
                    </h2>
                  </div>
                  <p className="text-xs text-on-surface-variant/60 px-1">
                    {isCn
                      ? '来自 Reddit、小红书、知乎等社区的真实评价和争议分析。点击展开查看详情。'
                      : 'Real reviews and controversy analysis from Reddit, Xiaohongshu, Zhihu, and more.'}
                  </p>
                  <div className="space-y-2">
                    {ranked.slice(0, 10).map((item) => (
                      <CommunityInsightRow
                        key={item.eval.school_id}
                        schoolId={item.eval.school_id}
                        schoolName={item.eval.school?.name ?? ''}
                        schoolNameCn={item.eval.school?.name_cn ?? undefined}
                        rank={item.rank}
                      />
                    ))}
                  </div>
                </div>
              </MotionSection>
            )}

            <div className="h-12" />
          </div>
        </div>
      </div>
      )}
    </section>
    </AnimatedWorkspacePage>
  );
}
