import React, { useState, useMemo, useEffect, useRef, useCallback } from 'react';
import { useEvaluations } from '../../hooks/useEvaluations';
import { useSchools } from '../../hooks/useSchools';
import { useApp } from '../../context/AppContext';
import { evaluationsApi } from '../../lib/api/evaluations';
import { schoolsApi } from '../../lib/api/schools';
import { reportsApi } from '../../lib/api/reports';
import type { EvaluationWithSchool, SchoolResponse } from '../../lib/types';

// ─── Helpers ───

function formatPercent(value: number | null | undefined): string {
  if (value == null) return 'N/A';
  const pct = value <= 1 ? Math.round(value * 100) : Math.round(value);
  return `${pct}%`;
}

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return 'N/A';
  return `$${value.toLocaleString()}`;
}

const TIER_CFG: Record<string, { bg: string; text: string; label: string; bar: string }> = {
  reach:  { bg: 'bg-secondary-fixed/50', text: 'text-on-secondary-fixed-variant', label: 'Reach',  bar: 'bg-secondary' },
  target: { bg: 'bg-tertiary-fixed/50',  text: 'text-on-tertiary-fixed-variant',  label: 'Target', bar: 'bg-tertiary' },
  safety: { bg: 'bg-primary/10',         text: 'text-primary',                     label: 'Safety', bar: 'bg-primary' },
  likely: { bg: 'bg-tertiary/10',        text: 'text-tertiary',                    label: 'Likely', bar: 'bg-tertiary/60' },
};

function MiniBar({ value, color }: { value: number; color: string }) {
  return (
    <div className="w-12 h-1.5 bg-surface-container-high/40 rounded-full overflow-hidden">
      <div className={`h-full rounded-full ${color}`} style={{ width: `${Math.round(value * 100)}%` }} />
    </div>
  );
}

function ScoreBar({ label, score, color }: { label: string; score: number; color: string }) {
  const pct = Math.round(score * 100);
  return (
    <div className="space-y-1.5">
      <div className="flex justify-between items-center">
        <span className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">{label}</span>
        <span className="text-xs font-black text-on-surface">{pct}%</span>
      </div>
      <div className="h-2 bg-surface-container-high/40 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color} transition-all duration-700`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

// ─── Tier Summary Strip ───

function TierSummary({ tieredCounts, total, avgScore, t }: { tieredCounts: Record<string, number>; total: number; avgScore: number; t: Record<string, any> }) {
  const tiers = ['reach', 'target', 'safety', 'likely'] as const;
  return (
    <div className="bg-surface-container-lowest rounded-2xl p-5 border border-outline-variant/10 flex items-center gap-6">
      {/* Tier distribution bar */}
      <div className="flex-1">
        <div className="flex h-3 rounded-full overflow-hidden bg-surface-container-high/30">
          {tiers.map((tier) => {
            const pct = total > 0 ? (tieredCounts[tier] / total) * 100 : 0;
            if (pct === 0) return null;
            return (
              <div key={tier} className={`${TIER_CFG[tier].bar} transition-all duration-500`} style={{ width: `${pct}%` }} />
            );
          })}
        </div>
        <div className="flex gap-4 mt-2">
          {tiers.map((tier) => (
            <div key={tier} className="flex items-center gap-1.5">
              <div className={`w-2 h-2 rounded-full ${TIER_CFG[tier].bar}`} />
              <span className="text-[9px] font-bold text-on-surface-variant/60 uppercase tracking-widest">
                {TIER_CFG[tier].label} {tieredCounts[tier]}
              </span>
            </div>
          ))}
        </div>
      </div>
      {/* Stats */}
      <div className="text-center px-4 border-l border-outline-variant/10">
        <div className="text-2xl font-black text-on-surface">{total}</div>
        <div className="text-[8px] font-bold text-on-surface-variant/50 uppercase tracking-widest">{t.sl_schools}</div>
      </div>
      <div className="text-center px-4 border-l border-outline-variant/10">
        <div className="text-2xl font-black text-primary">{Math.round(avgScore * 100)}%</div>
        <div className="text-[8px] font-bold text-on-surface-variant/50 uppercase tracking-widest">{t.sl_avg_match}</div>
      </div>
    </div>
  );
}

// ─── Expandable School Row ───

function SchoolRow({ ev, isFavorite, isBlacklisted, onToggleFavorite, onToggleBlacklist, t }: {
  ev: EvaluationWithSchool;
  isFavorite: boolean;
  isBlacklisted: boolean;
  onToggleFavorite: () => void;
  onToggleBlacklist: () => void;
  t: Record<string, any>;
}) {
  const [expanded, setExpanded] = useState(false);
  const tier = TIER_CFG[ev.tier] ?? TIER_CFG.target;

  if (isBlacklisted) return null;

  return (
    <div className={`bg-surface-container-lowest rounded-2xl border transition-all ${
      expanded ? 'border-primary/20 shadow-md' : 'border-outline-variant/10 hover:shadow-sm'
    }`}>
      {/* Compact row */}
      <div
        className="flex items-center gap-4 p-4 cursor-pointer"
        onClick={() => setExpanded(!expanded)}
      >
        {/* Rank / favorite */}
        <button
          onClick={(e) => { e.stopPropagation(); onToggleFavorite(); }}
          className="w-8 h-8 rounded-lg flex items-center justify-center hover:bg-primary/5 transition-colors shrink-0"
        >
          <span
            className={`material-symbols-outlined text-lg ${isFavorite ? 'text-yellow-500' : 'text-on-surface-variant/30'}`}
            style={isFavorite ? { fontVariationSettings: "'FILL' 1" } : undefined}
          >
            star
          </span>
        </button>

        {/* School info */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-headline text-sm font-bold text-on-surface truncate">{ev.school?.name ?? 'School'}</span>
            {ev.school?.name_cn && (
              <span className="text-xs text-on-surface-variant/50 truncate">{ev.school.name_cn}</span>
            )}
          </div>
          <div className="text-[10px] text-on-surface-variant/60 mt-0.5">
            {ev.school ? `${ev.school.city}, ${ev.school.state}` : ''}
          </div>
        </div>

        {/* Tier badge */}
        <span className={`px-2 py-1 ${tier.bg} ${tier.text} text-[8px] font-black uppercase tracking-widest rounded-md shrink-0`}>
          {tier.label}
        </span>

        {/* Mini score bars */}
        <div className="flex flex-col gap-1 shrink-0">
          <MiniBar value={ev.academic_fit} color="bg-primary" />
          <MiniBar value={ev.financial_fit} color="bg-tertiary" />
          <MiniBar value={ev.career_fit} color="bg-secondary" />
          <MiniBar value={ev.life_fit} color="bg-primary-fixed-dim" />
        </div>

        {/* Overall score */}
        <div className="text-right shrink-0 w-14">
          <div className="text-base font-black text-on-surface">{Math.round(ev.overall_score * 100)}%</div>
          <div className="text-[8px] text-on-surface-variant/50 font-bold">{t.sl_match}</div>
        </div>

        {/* Admission probability */}
        <div className="text-right shrink-0 w-14">
          <div className="text-base font-black text-primary">{formatPercent(ev.admission_probability)}</div>
          <div className="text-[8px] text-on-surface-variant/50 font-bold">{t.sl_admit}</div>
        </div>

        {/* Remove button */}
        <button
          onClick={(e) => { e.stopPropagation(); onToggleBlacklist(); }}
          className="w-7 h-7 rounded-lg flex items-center justify-center hover:bg-error/5 text-on-surface-variant/20 hover:text-error transition-colors shrink-0"
        >
          <span className="material-symbols-outlined text-base">close</span>
        </button>

        {/* Expand chevron */}
        <span className={`material-symbols-outlined text-on-surface-variant/30 text-lg transition-transform ${expanded ? 'rotate-180' : ''}`}>
          expand_more
        </span>
      </div>

      {/* Expanded: Level 1 Causal Visualization */}
      {expanded && (
        <div className="px-6 pb-6 pt-2 border-t border-outline-variant/10 space-y-6">
          {/* Score breakdown */}
          <div className="grid grid-cols-2 gap-x-8 gap-y-4">
            <ScoreBar label="Academic Fit" score={ev.academic_fit} color="bg-primary" />
            <ScoreBar label="Financial Fit" score={ev.financial_fit} color="bg-tertiary" />
            <ScoreBar label="Career Fit" score={ev.career_fit} color="bg-secondary" />
            <ScoreBar label="Life Fit" score={ev.life_fit} color="bg-primary-fixed-dim" />
          </div>

          {/* Key metrics */}
          <div className="grid grid-cols-4 gap-3">
            <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
              <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">{t.sl_net_price}</div>
              <div className="text-sm font-black">{formatCurrency(ev.school?.avg_net_price)}</div>
            </div>
            <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
              <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">{t.sl_acceptance}</div>
              <div className="text-sm font-black">{formatPercent(ev.school?.acceptance_rate)}</div>
            </div>
            <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
              <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">{t.sl_graduation}</div>
              <div className="text-sm font-black">{formatPercent(ev.school?.graduation_rate_4yr)}</div>
            </div>
            <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
              <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">{t.sl_ed_ea_rec}</div>
              <div className="text-sm font-black uppercase">{ev.ed_ea_recommendation ?? '—'}</div>
            </div>
          </div>

          {/* Reasoning */}
          {ev.reasoning && (
            <div className="bg-surface-container-high/20 rounded-xl p-4 border border-outline-variant/10">
              <div className="flex items-center gap-2 mb-2">
                <span className="material-symbols-outlined text-tertiary text-base" style={{ fontVariationSettings: "'FILL' 1" }}>psychology</span>
                <span className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">{t.sl_ai_reasoning}</span>
              </div>
              <p className="text-sm text-on-surface/80 leading-relaxed">{ev.reasoning}</p>
            </div>
          )}

          {/* Fit details (causal factor contributions) */}
          {ev.fit_details && Object.keys(ev.fit_details).length > 0 && (
            <div>
              <div className="flex items-center gap-2 mb-3">
                <span className="material-symbols-outlined text-primary text-base">account_tree</span>
                <span className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">{t.sl_causal_factors}</span>
              </div>
              <div className="grid grid-cols-2 gap-2">
                {Object.entries(ev.fit_details).map(([key, val]) => {
                  const numVal = typeof val === 'number' ? val : null;
                  if (numVal == null) return null;
                  const isPositive = numVal >= 0.5;
                  return (
                    <div key={key} className="flex items-center gap-2">
                      <div className={`w-1.5 h-1.5 rounded-full ${isPositive ? 'bg-tertiary' : 'bg-error/60'}`} />
                      <span className="text-xs text-on-surface-variant flex-1 truncate">
                        {key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}
                      </span>
                      <span className={`text-xs font-bold ${isPositive ? 'text-tertiary' : 'text-error/70'}`}>
                        {typeof val === 'number' ? `${Math.round(val * 100)}%` : String(val)}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Add School Modal ───

function AddSchoolModal({ studentId, onClose, onAdded, t }: { studentId: string; onClose: () => void; onAdded: () => void; t: Record<string, any> }) {
  const { schools, isLoading, search } = useSchools();
  const [query, setQuery] = useState('');
  const [adding, setAdding] = useState<string | null>(null);

  const handleSearch = () => search({ query: query || undefined, per_page: '10' });
  const handleKeyDown = (e: React.KeyboardEvent) => { if (e.key === 'Enter') handleSearch(); };

  const handleAdd = async (schoolId: string) => {
    setAdding(schoolId);
    try {
      await evaluationsApi.evaluate(studentId, schoolId);
      onAdded();
    } catch { /* ignore */ }
    setAdding(null);
  };

  return (
    <div className="fixed inset-0 bg-black/30 backdrop-blur-sm z-[100] flex items-center justify-center" onClick={onClose}>
      <div className="bg-white rounded-3xl shadow-2xl w-[560px] max-h-[70vh] flex flex-col" onClick={(e) => e.stopPropagation()}>
        <div className="p-6 border-b border-outline-variant/10">
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-headline text-lg font-black text-on-surface">{t.sl_add_modal_title}</h3>
            <button onClick={onClose} className="w-8 h-8 rounded-full flex items-center justify-center hover:bg-surface-container-high transition-colors">
              <span className="material-symbols-outlined text-on-surface-variant">close</span>
            </button>
          </div>
          <div className="flex gap-2">
            <input
              className="flex-1 bg-surface-container-highest rounded-xl px-4 py-2.5 text-sm outline-none border border-outline-variant/20 focus:border-primary transition-colors"
              placeholder={t.sl_search_placeholder}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={handleKeyDown}
              autoFocus
            />
            <button onClick={handleSearch} className="px-4 py-2.5 bg-primary text-on-primary rounded-xl text-xs font-bold">
              {t.sl_search}
            </button>
          </div>
        </div>
        <div className="flex-1 overflow-y-auto p-4 space-y-2">
          {isLoading && <div className="text-center py-8 text-sm text-on-surface-variant/60">{t.sl_searching}</div>}
          {!isLoading && schools.length === 0 && (
            <div className="text-center py-8 text-sm text-on-surface-variant/60">{t.sl_search_empty}</div>
          )}
          {schools.map((s) => (
            <div key={s.id} className="flex items-center gap-3 p-3 rounded-xl hover:bg-surface-container-high/30 transition-colors">
              <div className="flex-1 min-w-0">
                <div className="text-sm font-bold text-on-surface truncate">{s.name}</div>
                <div className="text-[10px] text-on-surface-variant/60">{s.city}, {s.state}</div>
              </div>
              {s.us_news_rank && <span className="text-xs font-bold text-primary">#{s.us_news_rank}</span>}
              <button
                onClick={() => handleAdd(s.id)}
                disabled={adding === s.id}
                className="px-3 py-1.5 bg-primary/5 text-primary text-xs font-bold rounded-lg border border-primary/15 hover:bg-primary/10 transition-colors disabled:opacity-50"
              >
                {adding === s.id ? t.sl_adding : t.sl_add}
              </button>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─── Main Panel ───

interface SchoolListPanelProps {
  studentId: string | null;
}

// ─── Interest Tags for AI Preferences ───

const INTEREST_TAGS = [
  { id: 'cs', label: 'Computer Science', icon: 'code' },
  { id: 'engineering', label: 'Engineering', icon: 'engineering' },
  { id: 'business', label: 'Business', icon: 'business_center' },
  { id: 'biology', label: 'Biology / Pre-Med', icon: 'biotech' },
  { id: 'arts', label: 'Arts & Humanities', icon: 'palette' },
  { id: 'social', label: 'Social Sciences', icon: 'groups' },
  { id: 'math', label: 'Math & Statistics', icon: 'calculate' },
  { id: 'research', label: 'Strong Research', icon: 'science' },
];

const PREFERENCE_TAGS = [
  { id: 'more_reach', label_key: 'sl_more_reach' as const, icon: 'trending_up' },
  { id: 'more_safety', label_key: 'sl_more_safety' as const, icon: 'shield' },
  { id: 'low_cost', label: 'Low Cost', icon: 'savings' },
  { id: 'urban', label: 'Urban Campus', icon: 'location_city' },
  { id: 'small', label: 'Small Class Size', icon: 'group' },
  { id: 'international', label: 'International Friendly', icon: 'public' },
];

function AIPreferencesPanel({ studentId, onGenerated, t }: {
  studentId: string;
  onGenerated: () => void;
  t: Record<string, any>;
}) {
  const [selectedInterests, setSelectedInterests] = useState<Set<string>>(new Set());
  const [selectedPrefs, setSelectedPrefs] = useState<Set<string>>(new Set());
  const [generating, setGenerating] = useState(false);
  const [status, setStatus] = useState<'idle' | 'polling' | 'done' | 'error'>('idle');
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const toggleTag = (set: Set<string>, id: string, setter: React.Dispatch<React.SetStateAction<Set<string>>>) => {
    setter((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  // Cleanup polling on unmount
  useEffect(() => {
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, []);

  const handleGenerate = async () => {
    if (!studentId) return;
    setGenerating(true);
    setStatus('polling');

    const hints = {
      interests: [...selectedInterests],
      preferences: [...selectedPrefs],
    };

    try {
      const result = await schoolsApi.generateList(studentId, hints);

      // If backend ran synchronously (no Celery), it returns status: "completed" directly
      if (result.status === 'completed') {
        setStatus('done');
        setGenerating(false);
        onGenerated();
        return;
      }

      // Celery path: poll the task
      if (result.task_id) {
        pollRef.current = setInterval(async () => {
          try {
            const taskStatus = await reportsApi.getTask(result.task_id!);
            if (taskStatus.status === 'completed' || taskStatus.status === 'SUCCESS') {
              if (pollRef.current) clearInterval(pollRef.current);
              setStatus('done');
              setGenerating(false);
              onGenerated();
            } else if (taskStatus.status === 'failed' || taskStatus.status === 'FAILURE') {
              if (pollRef.current) clearInterval(pollRef.current);
              setStatus('error');
              setGenerating(false);
            }
          } catch {
            if (pollRef.current) clearInterval(pollRef.current);
            setTimeout(() => {
              setStatus('done');
              setGenerating(false);
              onGenerated();
            }, 5000);
          }
        }, 2000);
        // Safety timeout
        setTimeout(() => {
          if (pollRef.current) clearInterval(pollRef.current);
          setGenerating(false);
          setStatus('done');
          onGenerated();
        }, 30000);
      }
    } catch {
      setStatus('error');
      setGenerating(false);
    }
  };

  return (
    <div className="bg-surface-container-lowest rounded-2xl border border-outline-variant/10 p-6 space-y-5">
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center">
          <span className="material-symbols-outlined text-primary text-xl">auto_awesome</span>
        </div>
        <div>
          <h3 className="font-headline text-sm font-black text-on-surface">{t.sl_preferences_title}</h3>
          <p className="text-[10px] text-on-surface-variant/60">{t.sl_preferences_desc}</p>
        </div>
      </div>

      {/* Interest tags */}
      <div>
        <div className="text-[9px] font-bold text-on-surface-variant/50 uppercase tracking-widest mb-2">Interests</div>
        <div className="flex flex-wrap gap-2">
          {INTEREST_TAGS.map((tag) => {
            const selected = selectedInterests.has(tag.id);
            return (
              <button
                key={tag.id}
                onClick={() => toggleTag(selectedInterests, tag.id, setSelectedInterests)}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-bold transition-all ${
                  selected
                    ? 'bg-primary/10 text-primary border border-primary/20'
                    : 'bg-surface-container-high/30 text-on-surface-variant border border-transparent hover:border-outline-variant/20'
                }`}
              >
                <span className="material-symbols-outlined text-sm">{tag.icon}</span>
                {tag.label}
              </button>
            );
          })}
        </div>
      </div>

      {/* Preference tags */}
      <div>
        <div className="text-[9px] font-bold text-on-surface-variant/50 uppercase tracking-widest mb-2">Preferences</div>
        <div className="flex flex-wrap gap-2">
          {PREFERENCE_TAGS.map((tag) => {
            const selected = selectedPrefs.has(tag.id);
            const label = tag.label_key ? (t as Record<string, any>)[tag.label_key] : tag.label;
            return (
              <button
                key={tag.id}
                onClick={() => toggleTag(selectedPrefs, tag.id, setSelectedPrefs)}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-bold transition-all ${
                  selected
                    ? 'bg-tertiary/10 text-tertiary border border-tertiary/20'
                    : 'bg-surface-container-high/30 text-on-surface-variant border border-transparent hover:border-outline-variant/20'
                }`}
              >
                <span className="material-symbols-outlined text-sm">{tag.icon}</span>
                {label}
              </button>
            );
          })}
        </div>
      </div>

      {/* Generate button + status */}
      <div className="flex items-center gap-3">
        <button
          onClick={handleGenerate}
          disabled={generating}
          className="flex-1 py-3 bg-primary text-on-primary rounded-xl font-bold text-sm hover:brightness-110 transition-all shadow-md disabled:opacity-50 flex items-center justify-center gap-2"
        >
          <span className={`material-symbols-outlined text-sm ${generating ? 'animate-spin' : ''}`}>
            {generating ? 'progress_activity' : 'auto_awesome'}
          </span>
          {generating ? t.sl_generating_status : t.sl_generate_with_prefs}
        </button>
        {status === 'done' && (
          <span className="text-xs font-bold text-tertiary flex items-center gap-1">
            <span className="material-symbols-outlined text-sm" style={{ fontVariationSettings: "'FILL' 1" }}>check_circle</span>
            {t.sl_poll_complete}
          </span>
        )}
        {status === 'error' && (
          <span className="text-xs font-bold text-error flex items-center gap-1">
            <span className="material-symbols-outlined text-sm">error</span>
            {t.sl_poll_failed}
          </span>
        )}
      </div>
    </div>
  );
}

// ─── Main Panel ───

export function SchoolListPanel({ studentId }: SchoolListPanelProps) {
  const { tieredList, evaluations, isLoading, refetch } = useEvaluations(studentId);
  const { favoriteSchoolIds, toggleFavorite, blacklistedSchoolIds, toggleBlacklist, t } = useApp();
  const [showAddModal, setShowAddModal] = useState(false);
  const [showPrefs, setShowPrefs] = useState(false);
  const [sortBy, setSortBy] = useState<'score' | 'tier' | 'favorite'>('score');
  const [tierFilter, setTierFilter] = useState<string | null>(null);
  const [listVersion, setListVersion] = useState(0);
  const prevCountRef = useRef(0);

  // Auto-detect when chat adds new schools (evaluation count increases)
  const currentCount = evaluations.length;
  const chatUpdated = currentCount > prevCountRef.current && prevCountRef.current > 0;
  useEffect(() => {
    prevCountRef.current = currentCount;
  }, [currentCount]);

  // Periodic background refresh to pick up chat-added schools
  useEffect(() => {
    const interval = setInterval(() => { refetch(); }, 15000);
    return () => clearInterval(interval);
  }, [refetch]);

  // Flatten and deduplicate by school_id (keep highest-scoring evaluation per school)
  const allEvals: EvaluationWithSchool[] = useMemo(() => {
    if (!tieredList) return [];
    const raw = [...tieredList.reach, ...tieredList.target, ...tieredList.safety, ...tieredList.likely];
    const best = new Map<string, EvaluationWithSchool>();
    for (const ev of raw) {
      const existing = best.get(ev.school_id);
      if (!existing || ev.overall_score > existing.overall_score) {
        best.set(ev.school_id, ev);
      }
    }
    return [...best.values()];
  }, [tieredList]);

  const visibleEvals = useMemo(() => {
    let list = tierFilter ? allEvals.filter((ev) => ev.tier === tierFilter) : allEvals;
    if (sortBy === 'favorite') {
      list = [...list].sort((a, b) => {
        const af = favoriteSchoolIds.has(a.school_id) ? 1 : 0;
        const bf = favoriteSchoolIds.has(b.school_id) ? 1 : 0;
        return bf - af || b.overall_score - a.overall_score;
      });
    } else if (sortBy === 'score') {
      list = [...list].sort((a, b) => b.overall_score - a.overall_score);
    }
    return list;
  }, [allEvals, tierFilter, sortBy, favoriteSchoolIds]);

  const tieredCounts = useMemo(() => ({
    reach: tieredList?.reach.length ?? 0,
    target: tieredList?.target.length ?? 0,
    safety: tieredList?.safety.length ?? 0,
    likely: tieredList?.likely.length ?? 0,
  }), [tieredList]);

  const total = allEvals.length;
  const avgScore = total > 0 ? allEvals.reduce((s, e) => s + e.overall_score, 0) / total : 0;

  const handleRefresh = useCallback(() => {
    refetch();
    setListVersion((v) => v + 1);
  }, [refetch]);

  return (
    <section className="w-full bg-background flex flex-col h-full overflow-hidden font-body" data-testid="school-list-panel">
      {/* Header */}
      <header className="h-16 px-10 flex items-center justify-between sticky top-0 bg-background/90 backdrop-blur-md z-20 border-b border-outline-variant/10">
        <div>
          <h1 className="font-headline text-lg font-black text-on-surface tracking-tight">{t.sl_title}</h1>
          <p className="text-[9px] text-on-surface-variant font-bold tracking-[0.1em] uppercase">{t.sl_subtitle}</p>
        </div>
        <div className="flex items-center gap-2">
          <select
            className="bg-surface-container-highest/60 border border-outline-variant/20 rounded-xl px-3 py-2 text-xs font-bold text-on-surface outline-none"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as 'score' | 'tier' | 'favorite')}
          >
            <option value="score">{t.sl_sort_score}</option>
            <option value="tier">{t.sl_sort_tier}</option>
            <option value="favorite">{t.sl_sort_fav}</option>
          </select>

          <select
            className="bg-surface-container-highest/60 border border-outline-variant/20 rounded-xl px-3 py-2 text-xs font-bold text-on-surface outline-none"
            value={tierFilter ?? ''}
            onChange={(e) => setTierFilter(e.target.value || null)}
          >
            <option value="">{t.sl_all_tiers}</option>
            <option value="reach">Reach</option>
            <option value="target">Target</option>
            <option value="safety">Safety</option>
            <option value="likely">Likely</option>
          </select>

          {/* Refresh */}
          <button
            onClick={handleRefresh}
            className="w-9 h-9 rounded-full flex items-center justify-center text-on-surface-variant hover:bg-surface-container-high transition-colors"
            title={t.sl_refresh_list}
          >
            <span className="material-symbols-outlined text-[20px]">refresh</span>
          </button>

          <button
            onClick={() => setShowAddModal(true)}
            className="px-4 py-2 bg-primary/5 text-primary text-xs font-bold uppercase tracking-widest rounded-xl border border-primary/15 hover:bg-primary/10 transition-colors flex items-center gap-1.5"
          >
            <span className="material-symbols-outlined text-sm">add</span>
            {t.sl_add_school}
          </button>

          <button
            onClick={() => setShowPrefs(!showPrefs)}
            className={`px-4 py-2 text-xs font-bold rounded-xl transition-all flex items-center gap-1.5 ${
              showPrefs
                ? 'bg-primary text-on-primary shadow-md'
                : 'bg-primary text-on-primary hover:brightness-110 shadow-md'
            }`}
          >
            <span className="material-symbols-outlined text-sm">auto_awesome</span>
            {t.sl_ai_recommend}
          </button>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto px-10 py-8 space-y-6">
        {/* Chat update notification */}
        {chatUpdated && (
          <div className="flex items-center gap-3 p-3 bg-tertiary/5 border border-tertiary/15 rounded-xl">
            <span className="material-symbols-outlined text-tertiary text-lg">notifications_active</span>
            <span className="text-xs font-bold text-tertiary flex-1">{t.sl_chat_updated}</span>
            <button
              onClick={handleRefresh}
              className="px-3 py-1 bg-tertiary/10 text-tertiary text-xs font-bold rounded-lg hover:bg-tertiary/15 transition-colors"
            >
              {t.sl_refresh_list}
            </button>
          </div>
        )}

        {/* AI Preferences Panel */}
        {showPrefs && studentId && (
          <AIPreferencesPanel
            studentId={studentId}
            onGenerated={() => { refetch(); }}
            t={t}
          />
        )}

        {/* Loading */}
        {isLoading && (
          <div className="space-y-4">
            <div className="animate-pulse bg-surface-container-high/60 rounded-2xl h-20" />
            {[...Array(3)].map((_, i) => (
              <div key={i} className="animate-pulse bg-surface-container-high/60 rounded-2xl h-16" />
            ))}
          </div>
        )}

        {/* Empty state */}
        {!isLoading && total === 0 && !showPrefs && (
          <div className="flex flex-col items-center justify-center py-24 text-center">
            <div className="w-20 h-20 rounded-3xl bg-surface-container-high/40 flex items-center justify-center mb-6">
              <span className="material-symbols-outlined text-4xl text-on-surface-variant/50">school</span>
            </div>
            <h3 className="font-headline text-xl font-black text-on-surface mb-2">{t.sl_empty_title}</h3>
            <p className="text-sm text-on-surface-variant/70 max-w-sm leading-relaxed mb-6">{t.sl_empty_desc}</p>
            <div className="flex gap-3">
              <button
                onClick={() => setShowAddModal(true)}
                className="px-5 py-2.5 bg-primary/5 text-primary text-sm font-bold rounded-xl border border-primary/15 hover:bg-primary/10 transition-colors"
              >
                {t.sl_manual_add}
              </button>
              <button
                onClick={() => setShowPrefs(true)}
                className="px-5 py-2.5 bg-primary text-on-primary text-sm font-bold rounded-xl hover:brightness-110 transition-all shadow-md"
              >
                {t.sl_ai_smart}
              </button>
            </div>
          </div>
        )}

        {/* Personalization banner + Tier Summary */}
        {!isLoading && total > 0 && (
          <>
            <div className="flex items-start gap-3 p-4 bg-primary/3 border border-primary/10 rounded-xl">
              <span className="material-symbols-outlined text-primary text-lg mt-0.5" style={{ fontVariationSettings: "'FILL' 1" }}>person_pin</span>
              <div className="flex-1">
                <p className="text-xs text-on-surface/70 leading-relaxed">{t.sl_personalized_banner}</p>
                <p className="text-[10px] text-primary/60 mt-1 font-bold">{t.sl_personalized_chat_tip}</p>
              </div>
            </div>
            <TierSummary tieredCounts={tieredCounts} total={total} avgScore={avgScore} t={t} />
          </>
        )}

        {/* School Rows */}
        {!isLoading && visibleEvals.length > 0 && (
          <div className="space-y-2">
            {visibleEvals.map((ev) => (
              <SchoolRow
                key={ev.id}
                ev={ev}
                isFavorite={favoriteSchoolIds.has(ev.school_id)}
                isBlacklisted={blacklistedSchoolIds.has(ev.school_id)}
                onToggleFavorite={() => toggleFavorite(ev.school_id)}
                onToggleBlacklist={() => toggleBlacklist(ev.school_id)}
                t={t}
              />
            ))}
          </div>
        )}

        {!isLoading && blacklistedSchoolIds.size > 0 && (
          <div className="text-center text-xs text-on-surface-variant/40 py-4">
            {t.sl_hidden_schools(blacklistedSchoolIds.size)}
          </div>
        )}

        <div className="h-12" />
      </div>

      {showAddModal && studentId && (
        <AddSchoolModal
          studentId={studentId}
          onClose={() => setShowAddModal(false)}
          onAdded={() => { refetch(); }}
          t={t}
        />
      )}
    </section>
  );
}
