import React, { useState, useMemo, useEffect, useRef, useCallback } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { useEvaluations } from '../../hooks/useEvaluations';
import { useSchools } from '../../hooks/useSchools';
import { useOffers } from '../../hooks/useOffers';
import { useApp } from '../../context/AppContext';
import { evaluationsApi } from '../../lib/api/evaluations';
import { schoolsApi } from '../../lib/api/schools';
import { portfolioApi } from '../../lib/api/portfolio';
import type {
  EvaluationWithSchool,
  GenerateSchoolListResponse,
  RecommendationScenarioPack,
} from '../../lib/types';
import { DashboardInput } from './ui/dashboard-input';
import { DashboardSegmentedGroup, DashboardSegmentedItem } from './ui/dashboard-segmented';
import { AnimatedWorkspacePage, MotionItem, MotionSection, MotionStagger, MotionSurface } from './WorkspaceMotion';

// ─── Helpers ───

function formatPercent(value: number | null | undefined): string {
  if (value == null) return '—';
  const pct = value <= 1 ? Math.round(value * 100) : Math.round(value);
  return `${pct}%`;
}

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return '—';
  return `$${value.toLocaleString()}`;
}

const TIER_CFG: Record<string, { bg: string; text: string; labelKey: 'sl_tier_reach' | 'sl_tier_target' | 'sl_tier_safety' | 'sl_tier_likely'; bar: string }> = {
  reach:  { bg: 'bg-secondary-fixed/50', text: 'text-on-secondary-fixed-variant', labelKey: 'sl_tier_reach', bar: 'bg-secondary' },
  target: { bg: 'bg-tertiary-fixed/50',  text: 'text-on-tertiary-fixed-variant',  labelKey: 'sl_tier_target', bar: 'bg-tertiary' },
  safety: { bg: 'bg-primary/10',         text: 'text-primary',                     labelKey: 'sl_tier_safety', bar: 'bg-primary' },
  likely: { bg: 'bg-tertiary/10',        text: 'text-tertiary',                    labelKey: 'sl_tier_likely', bar: 'bg-tertiary/60' },
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
                {t[TIER_CFG[tier].labelKey]} {tieredCounts[tier]}
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

function SchoolRow({ ev, isFavorite, isBlacklisted, onToggleFavorite, onToggleBlacklist, showRestore, t }: {
  ev: EvaluationWithSchool;
  isFavorite: boolean;
  isBlacklisted: boolean;
  onToggleFavorite: () => void;
  onToggleBlacklist: () => void;
  showRestore?: boolean;
  t: Record<string, any>;
}) {
  const [expanded, setExpanded] = useState(false);
  const [confirmRemove, setConfirmRemove] = useState(false);
  const confirmTimerRef = useRef<number | null>(null);
  const tier = TIER_CFG[ev.tier] ?? TIER_CFG.target;

  // In removed tab, show all blacklisted; in other tabs, hide blacklisted
  if (isBlacklisted && !showRestore) return null;

  return (
    <motion.div layout className={`dashboard-hover-lift bg-surface-container-lowest rounded-2xl border transition-all ${
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
            <span className="font-headline text-sm font-bold text-on-surface truncate">{ev.school?.name ?? t.common_school}</span>
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
          {t[tier.labelKey]}
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

        {/* Remove / Restore button */}
        {showRestore ? (
          <button
            onClick={(e) => { e.stopPropagation(); onToggleBlacklist(); }}
            className="inline-flex items-center gap-1 px-3 py-1.5 rounded-lg bg-tertiary/10 text-tertiary text-xs font-bold hover:bg-tertiary/20 transition-colors shrink-0"
          >
            <span className="material-symbols-outlined text-sm">undo</span>
            {t.sl_restore}
          </button>
        ) : confirmRemove ? (
          <button
            onClick={(e) => {
              e.stopPropagation();
              setConfirmRemove(false);
              if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current);
              onToggleBlacklist();
            }}
            className="inline-flex items-center gap-1 px-2.5 py-1 rounded-lg bg-error/10 text-error text-[11px] font-bold hover:bg-error/20 transition-colors shrink-0 animate-pulse"
          >
            <span className="material-symbols-outlined text-sm">delete</span>
            {t.sl_remove_confirm}
          </button>
        ) : (
          <button
            onClick={(e) => {
              e.stopPropagation();
              setConfirmRemove(true);
              // Auto-dismiss after 3 seconds
              if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current);
              confirmTimerRef.current = window.setTimeout(() => setConfirmRemove(false), 3000);
            }}
            className="w-7 h-7 rounded-lg flex items-center justify-center hover:bg-error/5 text-on-surface-variant/20 hover:text-error transition-colors shrink-0"
          >
            <span className="material-symbols-outlined text-base">close</span>
          </button>
        )}

        {/* Expand chevron */}
        <span className={`material-symbols-outlined text-on-surface-variant/30 text-lg transition-transform ${expanded ? 'rotate-180' : ''}`}>
          expand_more
        </span>
      </div>

      {/* Expanded: Level 1 Causal Visualization */}
      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.34, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="px-6 pb-6 pt-2 border-t border-outline-variant/10 space-y-6">
          {/* Score breakdown */}
          <div className="grid grid-cols-2 gap-x-8 gap-y-4">
            <ScoreBar label={t.common_academic} score={ev.academic_fit} color="bg-primary" />
            <ScoreBar label={t.common_financial} score={ev.financial_fit} color="bg-tertiary" />
            <ScoreBar label={t.common_career} score={ev.career_fit} color="bg-secondary" />
            <ScoreBar label={t.common_life} score={ev.life_fit} color="bg-primary-fixed-dim" />
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
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
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
            <DashboardInput
              className="flex-1"
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
  { id: 'cs', labelKey: 'sl_interest_cs' as const, icon: 'code' },
  { id: 'engineering', labelKey: 'sl_interest_engineering' as const, icon: 'engineering' },
  { id: 'business', labelKey: 'sl_interest_business' as const, icon: 'business_center' },
  { id: 'biology', labelKey: 'sl_interest_biology' as const, icon: 'biotech' },
  { id: 'arts', labelKey: 'sl_interest_arts' as const, icon: 'palette' },
  { id: 'social', labelKey: 'sl_interest_social' as const, icon: 'groups' },
  { id: 'math', labelKey: 'sl_interest_math' as const, icon: 'calculate' },
  { id: 'research', labelKey: 'sl_interest_research' as const, icon: 'science' },
];

const PREFERENCE_TAGS = [
  { id: 'more_reach', labelKey: 'sl_more_reach' as const, icon: 'trending_up' },
  { id: 'more_safety', labelKey: 'sl_more_safety' as const, icon: 'shield' },
  { id: 'low_cost', labelKey: 'sl_preference_low_cost' as const, icon: 'savings' },
  { id: 'urban', labelKey: 'sl_preference_urban' as const, icon: 'location_city' },
  { id: 'small', labelKey: 'sl_preference_small_class' as const, icon: 'group' },
  { id: 'international', labelKey: 'sl_preference_international' as const, icon: 'public' },
];

function toStringArray(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw.filter((item): item is string => typeof item === 'string');
  }
  if (typeof raw === 'string' && raw.trim()) return [raw.trim()];
  return [];
}

function inferSelectedPrefsFromPreferences(preferences: Record<string, unknown>): Set<string> {
  const tags = new Set<string>(toStringArray(preferences.ui_preference_tags));

  const risk = typeof preferences.risk_preference === 'string' ? preferences.risk_preference : '';
  if (risk === 'reach') tags.add('more_reach');
  if (risk === 'safety') tags.add('more_safety');

  const cost = typeof preferences.cost_priority === 'string' ? preferences.cost_priority : '';
  if (cost === 'low_cost') tags.add('low_cost');

  const location = toStringArray(preferences.location ?? preferences.location_preference);
  if (location.includes('urban')) tags.add('urban');

  const size = toStringArray(preferences.size ?? preferences.school_size_preference);
  if (size.includes('small')) tags.add('small');

  const culture = toStringArray(preferences.culture ?? preferences.campus_culture);
  if (culture.includes('international_friendly')) tags.add('international');

  return tags;
}

function mergePreferenceTags(
  base: Record<string, unknown>,
  selectedInterests: Set<string>,
  selectedPrefs: Set<string>,
): Record<string, unknown> {
  const next: Record<string, unknown> = { ...base };
  next.interests = [...selectedInterests];
  next.ui_preference_tags = [...selectedPrefs];

  if (selectedPrefs.has('more_reach') && !selectedPrefs.has('more_safety')) {
    next.risk_preference = 'reach';
  } else if (selectedPrefs.has('more_safety') && !selectedPrefs.has('more_reach')) {
    next.risk_preference = 'safety';
  } else {
    delete next.risk_preference;
  }

  if (selectedPrefs.has('low_cost')) next.cost_priority = 'low_cost';
  else if (next.cost_priority === 'low_cost') delete next.cost_priority;

  const location = new Set<string>(toStringArray(base.location ?? base.location_preference));
  if (selectedPrefs.has('urban')) location.add('urban');
  else location.delete('urban');
  if (location.size > 0) next.location = [...location];
  else delete next.location;

  const schoolSize = new Set<string>(toStringArray(base.size ?? base.school_size_preference));
  if (selectedPrefs.has('small')) schoolSize.add('small');
  else schoolSize.delete('small');
  if (schoolSize.size > 0) next.size = [...schoolSize];
  else delete next.size;

  const campusCulture = new Set<string>(toStringArray(base.culture ?? base.campus_culture));
  if (selectedPrefs.has('international')) campusCulture.add('international_friendly');
  else campusCulture.delete('international_friendly');
  if (campusCulture.size > 0) next.culture = [...campusCulture];
  else delete next.culture;

  delete next.location_preference;
  delete next.school_size_preference;
  delete next.campus_culture;

  return next;
}

function buildCanonicalPreferenceHints(preferences: Record<string, unknown>): string[] {
  const hints: string[] = [];

  const risk = typeof preferences.risk_preference === 'string' ? preferences.risk_preference : '';
  if (risk) hints.push(`risk:${risk}`);

  const cost = typeof preferences.cost_priority === 'string' ? preferences.cost_priority : '';
  if (cost) hints.push(`cost:${cost}`);

  for (const location of toStringArray(preferences.location ?? preferences.location_preference)) {
    hints.push(`location:${location}`);
  }
  for (const culture of toStringArray(preferences.culture ?? preferences.campus_culture)) {
    hints.push(`culture:${culture}`);
  }
  for (const size of toStringArray(preferences.size ?? preferences.school_size_preference)) {
    hints.push(`size:${size}`);
  }

  return hints;
}

function AIPreferencesPanel({ studentId, onGenerated, t }: {
  studentId: string;
  onGenerated: (payload: GenerateSchoolListResponse) => void;
  t: Record<string, any>;
}) {
  const [selectedInterests, setSelectedInterests] = useState<Set<string>>(new Set());
  const [selectedPrefs, setSelectedPrefs] = useState<Set<string>>(new Set());
  const [basePreferences, setBasePreferences] = useState<Record<string, unknown>>({});
  const [generating, setGenerating] = useState(false);
  const [status, setStatus] = useState<'idle' | 'done' | 'error'>('idle');

  useEffect(() => {
    let cancelled = false;

    const loadPreferences = async () => {
      try {
        const portfolio = await portfolioApi.get(studentId);
        if (cancelled) return;
        const preferences = (portfolio.preferences ?? {}) as Record<string, unknown>;
        setBasePreferences(preferences);
        setSelectedInterests(new Set(toStringArray(preferences.interests)));
        setSelectedPrefs(inferSelectedPrefsFromPreferences(preferences));
      } catch {
        if (!cancelled) {
          setBasePreferences({});
          setSelectedInterests(new Set());
          setSelectedPrefs(new Set());
        }
      }
    };

    loadPreferences();

    return () => { cancelled = true; };
  }, [studentId]);

  const handleGenerate = async () => {
    if (!studentId) return;
    setGenerating(true);
    setStatus('idle');

    const mergedPreferences = mergePreferenceTags(basePreferences, selectedInterests, selectedPrefs);

    try {
      await portfolioApi.patch(studentId, {
        preferences: mergedPreferences as any,
      });
      setBasePreferences(mergedPreferences);
    } catch {
      // Keep generation non-blocking even if preference persistence fails.
    }

    const hints = {
      interests: [...selectedInterests],
      preferences: buildCanonicalPreferenceHints(mergedPreferences),
    };

    try {
      const result = await schoolsApi.generateList(studentId, hints);
      if (result.status === 'completed') {
        setStatus('done');
        onGenerated(result);
      } else {
        setStatus('error');
      }
    } catch {
      setStatus('error');
    } finally {
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
        <div className="text-[9px] font-bold text-on-surface-variant/50 uppercase tracking-widest mb-2">{t.sl_interests_label}</div>
        <DashboardSegmentedGroup
          type="multiple"
          value={[...selectedInterests]}
          onValueChange={(values) => setSelectedInterests(new Set(values))}
          className="items-start"
        >
          {INTEREST_TAGS.map((tag) => {
            const label = t[tag.labelKey];
            return (
              <DashboardSegmentedItem
                key={tag.id}
                value={tag.id}
                accent="primary"
                size="compact"
                className="min-h-9"
              >
                <span className="material-symbols-outlined text-sm">{tag.icon}</span>
                {label}
              </DashboardSegmentedItem>
            );
          })}
        </DashboardSegmentedGroup>
      </div>

      {/* Preference tags */}
      <div>
        <div className="text-[9px] font-bold text-on-surface-variant/50 uppercase tracking-widest mb-2">{t.sl_preferences_label}</div>
        <DashboardSegmentedGroup
          type="multiple"
          value={[...selectedPrefs]}
          onValueChange={(values) => setSelectedPrefs(new Set(values))}
          className="items-start"
        >
          {PREFERENCE_TAGS.map((tag) => {
            const label = t[tag.labelKey];
            return (
              <DashboardSegmentedItem
                key={tag.id}
                value={tag.id}
                accent="tertiary"
                size="compact"
                className="min-h-9"
              >
                <span className="material-symbols-outlined text-sm">{tag.icon}</span>
                {label}
              </DashboardSegmentedItem>
            );
          })}
        </DashboardSegmentedGroup>
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

// ─── Region / Rank filter constants ───

const REGION_STATES: Record<string, string[]> = {
  east: ['CT','DC','DE','FL','GA','MA','MD','ME','NC','NH','NJ','NY','PA','RI','SC','VA','VT','WV'],
  west: ['AK','AZ','CA','CO','HI','ID','MT','NM','NV','OR','UT','WA','WY'],
  central: ['IA','IL','IN','KS','MI','MN','MO','ND','NE','OH','OK','SD','WI'],
  south: ['AL','AR','KY','LA','MS','OK','TN','TX'],
};

const REGION_CHIPS = [
  { id: 'east', labelKey: 'sl_region_east' as const },
  { id: 'west', labelKey: 'sl_region_west' as const },
  { id: 'central', labelKey: 'sl_region_central' as const },
  { id: 'south', labelKey: 'sl_region_south' as const },
] as const;

const RANK_CHIPS = [
  { id: 'top20', max: 20, labelKey: 'sl_rank_top20' as const },
  { id: 'top50', max: 50, labelKey: 'sl_rank_top50' as const },
  { id: 'top100', max: 100, labelKey: 'sl_rank_top100' as const },
] as const;

const TIER_ORDER = ['reach', 'target', 'safety', 'likely'] as const;
const TIER_ICONS: Record<string, string> = { reach: '\u{1F3AF}', target: '\u2705', safety: '\u{1F6E1}\uFE0F', likely: '\u{1F4CB}' };
const DEFAULT_VISIBLE_PER_TIER = 5;

// ─── Collapsible Tier Section ───

function TierSection({
  tier,
  evals,
  favoriteSchoolIds,
  blacklistedSchoolIds,
  toggleFavorite,
  toggleBlacklist,
  listVersion,
  total,
  t,
}: {
  tier: string;
  evals: EvaluationWithSchool[];
  favoriteSchoolIds: Set<string>;
  blacklistedSchoolIds: Set<string>;
  toggleFavorite: (id: string) => void;
  toggleBlacklist: (id: string) => void;
  listVersion: number;
  total: number;
  t: Record<string, any>;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const [showAll, setShowAll] = useState(false);
  const cfg = TIER_CFG[tier] ?? TIER_CFG.target;
  const icon = TIER_ICONS[tier] ?? '';
  const visible = showAll ? evals : evals.slice(0, DEFAULT_VISIBLE_PER_TIER);
  const hasMore = evals.length > DEFAULT_VISIBLE_PER_TIER;

  if (evals.length === 0) return null;

  return (
    <div className="space-y-2">
      {/* Section header */}
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="w-full flex items-center gap-3 py-2 px-1 group"
      >
        <span className="text-base">{icon}</span>
        <span className={`text-xs font-black uppercase tracking-widest ${cfg.text}`}>
          {t[cfg.labelKey]}
        </span>
        <span className="text-[10px] font-bold text-on-surface-variant/50 bg-surface-container-high/40 px-2 py-0.5 rounded-full">
          {evals.length}
        </span>
        <div className="flex-1" />
        <span className={`material-symbols-outlined text-on-surface-variant/30 text-lg transition-transform ${collapsed ? '-rotate-90' : ''}`}>
          expand_more
        </span>
      </button>

      {/* Section content */}
      <AnimatePresence initial={false}>
        {!collapsed && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <MotionStagger className="space-y-2" delay={0.04} stagger={0.03} role="surface" key={`tier-${tier}-${listVersion}-${total}`}>
              {visible.map((ev) => (
                <MotionItem key={ev.id} role="surface">
                  <SchoolRow
                    ev={ev}
                    isFavorite={favoriteSchoolIds.has(ev.school_id)}
                    isBlacklisted={blacklistedSchoolIds.has(ev.school_id)}
                    onToggleFavorite={() => toggleFavorite(ev.school_id)}
                    onToggleBlacklist={() => toggleBlacklist(ev.school_id)}
                    t={t}
                  />
                </MotionItem>
              ))}
            </MotionStagger>
            {hasMore && (
              <div className="flex justify-center pt-2">
                <button
                  onClick={() => setShowAll(!showAll)}
                  className="text-xs font-bold text-primary hover:text-primary/80 transition-colors flex items-center gap-1"
                >
                  <span className="material-symbols-outlined text-sm">{showAll ? 'expand_less' : 'expand_more'}</span>
                  {showAll ? t.sl_show_less : `${t.sl_show_more} (${evals.length - DEFAULT_VISIBLE_PER_TIER})`}
                </button>
              </div>
            )}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ─── Main Panel ───

export function SchoolListPanel({ studentId }: SchoolListPanelProps) {
  const { tieredList, evaluations, isLoading, refetch } = useEvaluations(studentId);
  const { favoriteSchoolIds, toggleFavorite, blacklistedSchoolIds, toggleBlacklist, setActiveNav, t } = useApp();
  const { offers } = useOffers(studentId ?? null);
  const hasOffers = offers.some((o) => o.status === 'admitted');
  const [showAddModal, setShowAddModal] = useState(false);
  const [showPrefs, setShowPrefs] = useState(false);
  const [sortBy, setSortBy] = useState<'score' | 'tier' | 'favorite'>('score');
  const [activeTab, setActiveTab] = useState<'favorites' | 'all' | 'removed'>('all');
  const [regionFilter, setRegionFilter] = useState<string | null>(null);
  const [rankFilter, setRankFilter] = useState<number | null>(null);
  const [listVersion, setListVersion] = useState(0);
  const [scenarioPack, setScenarioPack] = useState<RecommendationScenarioPack | null>(null);
  const [activeScenarioId, setActiveScenarioId] = useState<string>('baseline');
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

  // Apply quick filters (region + rank) to a list of evals
  const applyQuickFilters = useCallback((list: EvaluationWithSchool[]) => {
    let filtered = list;
    if (regionFilter) {
      const states = new Set(REGION_STATES[regionFilter] ?? []);
      filtered = filtered.filter((ev) => ev.school?.state && states.has(ev.school.state));
    }
    if (rankFilter) {
      filtered = filtered.filter((ev) => ev.school?.us_news_rank != null && ev.school.us_news_rank <= rankFilter);
    }
    return filtered;
  }, [regionFilter, rankFilter]);

  // Sort helper
  const applySorting = useCallback((list: EvaluationWithSchool[]) => {
    if (sortBy === 'favorite') {
      return [...list].sort((a, b) => {
        const af = favoriteSchoolIds.has(a.school_id) ? 1 : 0;
        const bf = favoriteSchoolIds.has(b.school_id) ? 1 : 0;
        return bf - af || b.overall_score - a.overall_score;
      });
    } else if (sortBy === 'score') {
      return [...list].sort((a, b) => b.overall_score - a.overall_score);
    }
    return list;
  }, [sortBy, favoriteSchoolIds]);

  // Non-blacklisted evals (used for "all" and "favorites" tabs)
  const activeEvals = useMemo(() => {
    return allEvals.filter((ev) => !blacklistedSchoolIds.has(ev.school_id));
  }, [allEvals, blacklistedSchoolIds]);

  // Favorites tab data
  const favoriteEvals = useMemo(() => {
    const favs = activeEvals.filter((ev) => favoriteSchoolIds.has(ev.school_id));
    return applySorting(applyQuickFilters(favs));
  }, [activeEvals, favoriteSchoolIds, applyQuickFilters, applySorting]);

  // Removed tab data
  const removedEvals = useMemo(() => {
    return allEvals.filter((ev) => blacklistedSchoolIds.has(ev.school_id));
  }, [allEvals, blacklistedSchoolIds]);

  // "All" tab: group by tier, applying filters + sorting within each tier
  const tieredGroups = useMemo(() => {
    const groups: Record<string, EvaluationWithSchool[]> = { reach: [], target: [], safety: [], likely: [] };
    for (const ev of activeEvals) {
      const tier = ev.tier && groups[ev.tier] ? ev.tier : 'target';
      groups[tier].push(ev);
    }
    // Apply quick filters and sorting within each tier
    for (const tier of TIER_ORDER) {
      groups[tier] = applySorting(applyQuickFilters(groups[tier]));
    }
    return groups;
  }, [activeEvals, applyQuickFilters, applySorting]);

  const tieredCounts = useMemo(() => ({
    reach: tieredGroups.reach.length,
    target: tieredGroups.target.length,
    safety: tieredGroups.safety.length,
    likely: tieredGroups.likely.length,
  }), [tieredGroups]);

  const total = activeEvals.length;
  const avgScore = total > 0 ? activeEvals.reduce((s, e) => s + e.overall_score, 0) / total : 0;

  const scenarioTabs = useMemo(() => {
    if (!scenarioPack) return [];
    const tabs: Array<{ id: string; label: string; schools: any[] }> = [
      { id: 'baseline', label: t.sl_scenario_baseline ?? 'Baseline', schools: scenarioPack.baseline ?? [] },
      ...((scenarioPack.scenarios ?? []).map((item) => ({ id: item.id, label: item.label || item.id, schools: item.schools || [] }))),
    ];
    return tabs;
  }, [scenarioPack, t]);
  const activeScenario = useMemo(() => {
    if (scenarioTabs.length === 0) return null;
    return scenarioTabs.find((item) => item.id === activeScenarioId) ?? scenarioTabs[0];
  }, [scenarioTabs, activeScenarioId]);

  const handleRefresh = useCallback(() => {
    refetch();
    setListVersion((v) => v + 1);
  }, [refetch]);

  const toggleRegion = useCallback((id: string) => {
    setRegionFilter((prev) => prev === id ? null : id);
  }, []);

  const toggleRank = useCallback((max: number) => {
    setRankFilter((prev) => prev === max ? null : max);
  }, []);

  return (
    <AnimatedWorkspacePage className="w-full bg-background font-body">
      <section className="flex h-full w-full flex-col overflow-hidden">
        <header className="sticky top-0 z-20 border-b border-outline-variant/10 bg-background/90 px-4 py-3 backdrop-blur-md sm:px-6 lg:px-8">
          <MotionSection role="toolbar" className="space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex flex-col gap-1">
                <h1 className="font-headline text-lg font-black tracking-tight text-on-surface">{t.sl_title}</h1>
                <p className="text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/60">{t.sl_subtitle}</p>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={handleRefresh}
                  className="dashboard-hover-lift flex h-10 w-10 shrink-0 items-center justify-center rounded-full text-on-surface-variant transition-colors hover:bg-surface-container-high"
                  title={t.sl_refresh_list}
                >
                  <span className="material-symbols-outlined text-[20px]">refresh</span>
                </button>
                <button
                  onClick={() => setShowAddModal(true)}
                  className="dashboard-hover-lift inline-flex shrink-0 items-center gap-1.5 rounded-xl border border-primary/15 bg-primary/5 px-4 py-2 text-xs font-bold uppercase tracking-widest text-primary transition-colors hover:bg-primary/10"
                >
                  <span className="material-symbols-outlined text-sm">add</span>
                  {t.sl_add_school}
                </button>
                <button
                  onClick={() => setShowPrefs(!showPrefs)}
                  className="dashboard-hover-lift inline-flex shrink-0 items-center gap-1.5 rounded-xl bg-primary px-4 py-2 text-xs font-bold text-on-primary shadow-md transition-all hover:brightness-110"
                >
                  <span className="material-symbols-outlined text-sm">auto_awesome</span>
                  {t.sl_ai_recommend}
                </button>
              </div>
            </div>

            {/* Tab navigation */}
            <DashboardSegmentedGroup
              type="single"
              value={activeTab}
              onValueChange={(value) => { if (value) setActiveTab(value as 'favorites' | 'all' | 'removed'); }}
            >
              <DashboardSegmentedItem value="favorites" accent="primary" size="compact">
                <span className="material-symbols-outlined text-sm" style={{ fontVariationSettings: "'FILL' 1" }}>star</span>
                {t.sl_tab_favorites}
                {favoriteSchoolIds.size > 0 && (
                  <span className="ml-1 text-[9px] bg-primary/10 text-primary px-1.5 py-0.5 rounded-full font-black">{favoriteSchoolIds.size}</span>
                )}
              </DashboardSegmentedItem>
              <DashboardSegmentedItem value="all" accent="primary" size="compact">
                <span className="material-symbols-outlined text-sm">list</span>
                {t.sl_tab_all}
                {total > 0 && (
                  <span className="ml-1 text-[9px] bg-surface-container-high/60 text-on-surface-variant px-1.5 py-0.5 rounded-full font-black">{total}</span>
                )}
              </DashboardSegmentedItem>
              <DashboardSegmentedItem value="removed" accent="primary" size="compact">
                <span className="material-symbols-outlined text-sm">delete_outline</span>
                {t.sl_tab_removed}
                {blacklistedSchoolIds.size > 0 && (
                  <span className="ml-1 text-[9px] bg-error/10 text-error px-1.5 py-0.5 rounded-full font-black">{blacklistedSchoolIds.size}</span>
                )}
              </DashboardSegmentedItem>
            </DashboardSegmentedGroup>

            {/* Quick filter bar (shown for favorites + all tabs) */}
            {activeTab !== 'removed' && (
              <div className="flex flex-wrap items-center gap-1.5">
                {REGION_CHIPS.map((chip) => (
                  <button
                    key={chip.id}
                    onClick={() => toggleRegion(chip.id)}
                    className={`px-3 py-1.5 rounded-full text-[10px] font-bold uppercase tracking-wider transition-colors border ${
                      regionFilter === chip.id
                        ? 'bg-primary text-on-primary border-primary'
                        : 'bg-surface-container-lowest text-on-surface-variant/70 border-outline-variant/15 hover:bg-surface-container-high/50'
                    }`}
                  >
                    {t[chip.labelKey]}
                  </button>
                ))}
                <div className="w-px h-5 bg-outline-variant/15 mx-1" />
                {RANK_CHIPS.map((chip) => (
                  <button
                    key={chip.id}
                    onClick={() => toggleRank(chip.max)}
                    className={`px-3 py-1.5 rounded-full text-[10px] font-bold uppercase tracking-wider transition-colors border ${
                      rankFilter === chip.max
                        ? 'bg-secondary text-on-secondary border-secondary'
                        : 'bg-surface-container-lowest text-on-surface-variant/70 border-outline-variant/15 hover:bg-surface-container-high/50'
                    }`}
                  >
                    {t[chip.labelKey]}
                  </button>
                ))}
              </div>
            )}
          </MotionSection>
        </header>

        <div className="flex-1 overflow-y-auto px-4 py-5 space-y-6 sm:px-6 sm:py-6 lg:px-8">
        {/* Chat update notification */}
        {chatUpdated && (
          <MotionSection>
            <div className="dashboard-surface-soft flex items-center gap-3 border-tertiary/15 bg-tertiary/5 p-3">
              <span className="material-symbols-outlined text-lg text-tertiary">notifications_active</span>
              <span className="flex-1 text-xs font-bold text-tertiary">{t.sl_chat_updated}</span>
              <button
                onClick={handleRefresh}
                className="rounded-lg bg-tertiary/10 px-3 py-1 text-xs font-bold text-tertiary transition-colors hover:bg-tertiary/15"
              >
                {t.sl_refresh_list}
              </button>
            </div>
          </MotionSection>
        )}

        {/* AI Preferences Panel */}
        {showPrefs && studentId && (
          <MotionSection delay={0.04}>
            <AIPreferencesPanel
              studentId={studentId}
              onGenerated={(payload) => {
                if (payload?.scenario_pack) {
                  setScenarioPack(payload.scenario_pack);
                  setActiveScenarioId('baseline');
                }
                refetch();
              }}
              t={t}
            />
          </MotionSection>
        )}

        {activeScenario && (
          <MotionSection delay={0.05}>
            <div className="dashboard-surface-soft border-primary/10 bg-primary/3 p-4 space-y-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <p className="text-[11px] font-bold uppercase tracking-wider text-primary/70">
                    {t.sl_scenario_title ?? 'Scenario Pack'}
                  </p>
                  <p className="text-xs text-on-surface-variant/70">
                    {t.sl_scenario_desc ?? 'Use scenario tabs to see how ranking changes under different constraints.'}
                  </p>
                </div>
                <div className="text-[11px] font-semibold text-on-surface-variant/70">
                  {activeScenario.schools.length} {t.sl_schools ?? 'schools'}
                </div>
              </div>
              <div className="flex flex-wrap gap-2">
                {scenarioTabs.map((tab) => (
                  <button
                    key={tab.id}
                    onClick={() => setActiveScenarioId(tab.id)}
                    className={`rounded-full px-3 py-1 text-xs font-bold transition-colors ${
                      activeScenario.id === tab.id
                        ? 'bg-primary text-on-primary'
                        : 'bg-surface-container-high text-on-surface-variant hover:bg-surface-container-high/80'
                    }`}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
              <div className="space-y-2">
                {(activeScenario.schools ?? []).slice(0, 8).map((school: any) => (
                  <div key={`${activeScenario.id}-${school.school_id ?? school.school_name}`} className="flex items-center gap-3 rounded-xl border border-outline-variant/10 bg-surface-container-lowest px-3 py-2">
                    <div className="w-8 text-center text-xs font-black text-on-surface-variant/60">#{school.rank ?? '—'}</div>
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-sm font-bold text-on-surface">{school.school_name}</div>
                      <div className="flex items-center gap-2 text-[10px] text-on-surface-variant/60">
                        {school.prefilter_tag === 'eligible' ? <span>{t.sl_in_budget ?? 'In budget'}</span> : null}
                        {school.prefilter_tag === 'stretch' ? <span>{t.sl_stretch ?? 'Stretch'}</span> : null}
                        {typeof school.rank_delta === 'number' && school.rank_delta !== 0 ? (
                          <span className={school.rank_delta > 0 ? 'text-tertiary font-bold' : 'text-error font-bold'}>
                            {school.rank_delta > 0 ? `↑${school.rank_delta}` : `${school.rank_delta}`}
                          </span>
                        ) : null}
                      </div>
                    </div>
                    <div className="text-xs font-black text-primary">{Math.round((school.scenario_score ?? school.overall_score ?? 0) * 100)}%</div>
                  </div>
                ))}
              </div>
            </div>
          </MotionSection>
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
        {!isLoading && total === 0 && activeTab !== 'removed' && !showPrefs && (
          <MotionSurface className="flex flex-col items-center justify-center py-24 text-center">
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
          </MotionSurface>
        )}

        {/* ═══════ FAVORITES TAB ═══════ */}
        {!isLoading && activeTab === 'favorites' && (
          <>
            {favoriteEvals.length === 0 && total > 0 && (
              <MotionSurface className="flex flex-col items-center justify-center py-16 text-center">
                <span className="material-symbols-outlined text-4xl text-on-surface-variant/30 mb-4" style={{ fontVariationSettings: "'FILL' 1" }}>star</span>
                <p className="text-sm text-on-surface-variant/60">{t.sl_no_favorites}</p>
              </MotionSurface>
            )}
            {favoriteEvals.length > 0 && (
              <MotionStagger className="space-y-2" delay={0.08} stagger={0.05} role="surface" key={`fav-list-${listVersion}-${favoriteEvals.length}`}>
                {favoriteEvals.map((ev) => (
                  <MotionItem key={ev.id} role="surface">
                    <SchoolRow
                      ev={ev}
                      isFavorite={true}
                      isBlacklisted={false}
                      onToggleFavorite={() => toggleFavorite(ev.school_id)}
                      onToggleBlacklist={() => toggleBlacklist(ev.school_id)}
                      t={t}
                    />
                  </MotionItem>
                ))}
              </MotionStagger>
            )}
          </>
        )}

        {/* ═══════ ALL TAB (tier-grouped) ═══════ */}
        {!isLoading && activeTab === 'all' && total > 0 && (
          <>
            {/* Offers nudge banner */}
            {hasOffers && (
              <MotionSection delay={0.02}>
                <div className="dashboard-surface-soft flex items-center gap-3 border-tertiary/15 bg-tertiary/5 p-4">
                  <span className="material-symbols-outlined text-lg text-tertiary" style={{ fontVariationSettings: "'FILL' 1" }}>verified</span>
                  <span className="flex-1 text-xs leading-relaxed text-on-surface/70">{t.sl_offers_banner}</span>
                  <button
                    onClick={() => setActiveNav('decisions')}
                    className="shrink-0 rounded-xl bg-tertiary/10 px-4 py-2 text-xs font-bold text-tertiary transition-colors hover:bg-tertiary/20"
                  >
                    {t.sl_offers_banner_cta}
                  </button>
                </div>
              </MotionSection>
            )}

            {/* Personalization banner + Tier Summary */}
            <MotionStagger className="space-y-4" delay={0.04} role="metric">
              <MotionItem role="surface">
                <div className="dashboard-surface-soft flex items-start gap-3 border-primary/10 bg-primary/3 p-4">
                  <span className="material-symbols-outlined mt-0.5 text-lg text-primary" style={{ fontVariationSettings: "'FILL' 1" }}>person_pin</span>
                  <div className="flex-1">
                    <p className="text-xs leading-relaxed text-on-surface/70">{t.sl_personalized_banner}</p>
                    <p className="mt-1 text-[10px] font-bold text-primary/60">{t.sl_personalized_chat_tip}</p>
                  </div>
                </div>
              </MotionItem>
              <MotionItem role="metric">
                <MotionSurface className="p-0">
                  <TierSummary tieredCounts={tieredCounts} total={total} avgScore={avgScore} t={t} />
                </MotionSurface>
              </MotionItem>
            </MotionStagger>

            {/* Tier sections */}
            {TIER_ORDER.map((tier) => (
              <TierSection
                key={tier}
                tier={tier}
                evals={tieredGroups[tier]}
                favoriteSchoolIds={favoriteSchoolIds}
                blacklistedSchoolIds={blacklistedSchoolIds}
                toggleFavorite={toggleFavorite}
                toggleBlacklist={toggleBlacklist}
                listVersion={listVersion}
                total={total}
                t={t}
              />
            ))}
          </>
        )}

        {/* ═══════ REMOVED TAB ═══════ */}
        {!isLoading && activeTab === 'removed' && (
          <>
            {removedEvals.length === 0 && (
              <MotionSurface className="flex flex-col items-center justify-center py-16 text-center">
                <span className="material-symbols-outlined text-4xl text-on-surface-variant/30 mb-4">delete_outline</span>
                <p className="text-sm text-on-surface-variant/60">{t.sl_no_removed}</p>
              </MotionSurface>
            )}
            {removedEvals.length > 0 && (
              <MotionStagger className="space-y-2" delay={0.08} stagger={0.05} role="surface" key={`removed-list-${listVersion}-${removedEvals.length}`}>
                {removedEvals.map((ev) => (
                  <MotionItem key={ev.id} role="surface">
                    <SchoolRow
                      ev={ev}
                      isFavorite={favoriteSchoolIds.has(ev.school_id)}
                      isBlacklisted={true}
                      showRestore
                      onToggleFavorite={() => toggleFavorite(ev.school_id)}
                      onToggleBlacklist={() => toggleBlacklist(ev.school_id)}
                      t={t}
                    />
                  </MotionItem>
                ))}
              </MotionStagger>
            )}
          </>
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
    </AnimatedWorkspacePage>
  );
}
