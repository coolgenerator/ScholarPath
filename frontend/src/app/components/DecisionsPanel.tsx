import React, { useState, useMemo } from 'react';
import { useApp } from '../../context/AppContext';
import { useEvaluations } from '../../hooks/useEvaluations';
import { CausalDagD3 } from './CausalDagD3';
import type { EvaluationWithSchool } from '../../lib/types';

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

interface RankedSchool {
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
    <div className="flex items-center gap-4">
      <div className="w-8 h-8 rounded-lg bg-primary/5 flex items-center justify-center shrink-0">
        <span className="material-symbols-outlined text-primary text-base">{priority.icon}</span>
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex justify-between items-center mb-1">
          <span className="text-xs font-bold text-on-surface">{isCn ? priority.labelCn : priority.label}</span>
          <span className="text-xs font-black text-primary">{value}%</span>
        </div>
        <input
          type="range"
          min={0} max={100} step={5}
          value={value}
          onChange={(e) => onChange(Number(e.target.value))}
          className="w-full h-1.5 bg-surface-container-high/40 rounded-full appearance-none cursor-pointer accent-primary"
        />
        <p className="text-[10px] text-on-surface-variant/50 mt-0.5">{isCn ? priority.descCn : priority.desc}</p>
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

function RankedSchoolCard({ item, isCn }: { item: RankedSchool; isCn: boolean }) {
  const [expanded, setExpanded] = useState(false);
  const ev = item.eval;

  return (
    <div className="bg-surface-container-lowest rounded-2xl border border-outline-variant/10 hover:shadow-sm transition-all">
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
            <h3 className="font-headline text-sm font-bold text-on-surface truncate">{ev.school?.name ?? 'School'}</h3>
            {ev.school?.name_cn && (
              <span className="text-xs text-on-surface-variant/50 truncate">{ev.school.name_cn}</span>
            )}
          </div>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-[10px] text-on-surface-variant/60">
              {ev.school?.city}, {ev.school?.state}
            </span>
            <span className="text-[10px] text-on-surface-variant/40">•</span>
            <span className="text-[10px] text-on-surface-variant/60">
              {isCn ? '关键因素' : 'Top factor'}: {item.topFactor.label}
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
      {expanded && (
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
          <div className="grid grid-cols-4 gap-3">
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
            <div className="bg-surface-container-high/20 rounded-xl p-4 border border-outline-variant/10">
              <div className="flex items-center gap-2 mb-2">
                <span className="material-symbols-outlined text-tertiary text-base" style={{ fontVariationSettings: "'FILL' 1" }}>psychology</span>
                <span className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">
                  {isCn ? 'AI 推理' : 'AI Reasoning'}
                </span>
              </div>
              <p className="text-sm text-on-surface/80 leading-relaxed">{ev.reasoning}</p>
            </div>
          )}
        </div>
      )}
    </div>
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
          <select
            className="w-full bg-surface-container-highest/60 border border-outline-variant/20 rounded-xl px-4 py-2.5 text-sm text-on-surface outline-none focus:border-primary max-w-md"
            value={dagSchoolId}
            onChange={(e) => setDagSchoolId(e.target.value)}
          >
            <option value="">{isCn ? '选择学校查看因果图...' : 'Select school to view DAG...'}</option>
            {schools.map((r) => (
              <option key={r.eval.school_id} value={r.eval.school_id}>
                #{r.rank} {r.eval.school?.name ?? 'School'}
              </option>
            ))}
          </select>

          {dagSchoolId && studentId && (
            <CausalDagD3
              studentId={studentId}
              schoolId={dagSchoolId}
              t={t}
            />
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
  const { t, locale } = useApp();
  const isCn = locale === 'zh';
  const { tieredList, isLoading } = useEvaluations(studentId);

  const [weights, setWeights] = useState<Record<string, number>>({
    academic: 50,
    financial: 50,
    career: 50,
    life: 50,
  });
  const [activePreset, setActivePreset] = useState<string>('balanced');

  const allEvals = useMemo<EvaluationWithSchool[]>(() => {
    if (!tieredList) return [];
    return [...tieredList.reach, ...tieredList.target, ...tieredList.safety, ...tieredList.likely];
  }, [tieredList]);

  const ranked = useMemo(
    () => computeRanking(allEvals, weights, isCn),
    [allEvals, weights, isCn],
  );

  const handlePreset = (preset: Preset) => {
    setWeights({ ...preset.weights });
    setActivePreset(preset.id);
  };

  const handleWeightChange = (id: string, value: number) => {
    setWeights((prev) => ({ ...prev, [id]: value }));
    setActivePreset('');
  };

  return (
    <section className="w-full bg-background flex flex-col h-full overflow-hidden font-body" data-testid="decisions-panel">
      <header className="h-16 px-10 flex items-center justify-between sticky top-0 bg-background/90 backdrop-blur-md z-20 border-b border-outline-variant/10">
        <div>
          <h1 className="font-headline text-lg font-black text-on-surface tracking-tight">
            {isCn ? '智能择校' : 'Smart Ranking'}
          </h1>
          <p className="text-[9px] text-on-surface-variant font-bold tracking-[0.1em] uppercase">
            {isCn ? '基于因果推理的个性化排名' : 'Personalized ranking powered by causal inference'}
            {ranked.length > 0 && ` • ${ranked.length} ${isCn ? '所学校' : 'Schools'}`}
          </p>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto">
        <div className="flex h-full">
          {/* Left: Priority Controls */}
          <div className="w-80 shrink-0 border-r border-outline-variant/10 p-6 space-y-6 overflow-y-auto">
            {/* Quick presets */}
            <div>
              <div className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-3">
                {isCn ? '快速选择' : 'Quick Presets'}
              </div>
              <div className="flex flex-wrap gap-2">
                {PRESETS.map((preset) => (
                  <button
                    key={preset.id}
                    onClick={() => handlePreset(preset)}
                    className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-colors flex items-center gap-1.5 ${
                      activePreset === preset.id
                        ? 'bg-primary text-on-primary shadow-sm'
                        : 'bg-surface-container-high/30 text-on-surface-variant hover:bg-surface-container-high/50'
                    }`}
                  >
                    <span className="material-symbols-outlined text-sm">{preset.icon}</span>
                    {isCn ? preset.labelCn : preset.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Priority sliders */}
            <div>
              <div className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-3">
                {isCn ? '调整你的优先级' : 'Your Priorities'}
              </div>
              <div className="space-y-5">
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
            </div>

            {/* How it works */}
            <div className="bg-surface-container-high/20 rounded-xl p-4 border border-outline-variant/5">
              <div className="flex items-center gap-2 mb-2">
                <span className="material-symbols-outlined text-primary text-sm">info</span>
                <span className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">
                  {isCn ? '工作原理' : 'How it works'}
                </span>
              </div>
              <p className="text-[11px] text-on-surface-variant/60 leading-relaxed">
                {isCn
                  ? '根据你设定的优先级权重，因果推理引擎会重新计算每所学校的综合匹配度。展开任一学校卡片，可以看到每个维度的得分×权重的详细拆解。'
                  : 'The causal inference engine re-weights each school\'s fit scores based on your priorities. Expand any school card to see the detailed score × weight breakdown for each dimension.'}
              </p>
            </div>
          </div>

          {/* Right: Ranked Results */}
          <div className="flex-1 overflow-y-auto px-8 py-6">
            {isLoading && (
              <div className="space-y-3">
                {[...Array(4)].map((_, i) => (
                  <div key={i} className="animate-pulse bg-surface-container-high/60 rounded-2xl h-20" />
                ))}
              </div>
            )}

            {!isLoading && ranked.length === 0 && (
              <div className="flex flex-col items-center justify-center py-24 text-center">
                <div className="w-20 h-20 rounded-3xl bg-surface-container-high/40 flex items-center justify-center mb-6">
                  <span className="material-symbols-outlined text-4xl text-on-surface-variant/50">leaderboard</span>
                </div>
                <h3 className="font-headline text-xl font-black text-on-surface mb-2">
                  {isCn ? '暂无学校数据' : 'No Schools Yet'}
                </h3>
                <p className="text-sm text-on-surface-variant/70 max-w-sm leading-relaxed">
                  {isCn ? '先在 School List 中添加学校，然后回来查看个性化排名。' : 'Add schools to your School List first, then come back to see your personalized ranking.'}
                </p>
              </div>
            )}

            {!isLoading && ranked.length > 0 && (
              <div className="space-y-2">
                {ranked.map((item) => (
                  <RankedSchoolCard key={item.eval.id} item={item} isCn={isCn} />
                ))}
              </div>
            )}

            {/* Causal DAG Section */}
            {!isLoading && ranked.length > 0 && (
              <CausalDagSection ranked={ranked} studentId={studentId} isCn={isCn} t={t} />
            )}

            <div className="h-12" />
          </div>
        </div>
      </div>
    </section>
  );
}
