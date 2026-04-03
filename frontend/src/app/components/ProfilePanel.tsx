import React, { useState, useEffect, useCallback } from 'react';
import { useApp } from '../../context/AppContext';
import { portfolioApi } from '../../lib/api/portfolio';
import type { StudentPortfolioResponse } from '../../lib/types';
import { DashboardFieldLabel } from './ui/dashboard-select';
import { DashboardInput } from './ui/dashboard-input';
import { DashboardSegmentedGroup, DashboardSegmentedItem } from './ui/dashboard-segmented';
import { AnimatedWorkspacePage, MotionItem, MotionSection, MotionStagger, MotionSurface } from './WorkspaceMotion';

const ED_NONE_VALUE = '__profile_ed_none__';

function Field({ label, value, editMode, type = 'text', onChange }: {
  label: string;
  value: string;
  editMode: boolean;
  type?: string;
  onChange?: (v: string) => void;
}) {
  return (
    <div>
      <DashboardFieldLabel className="text-[9px]">{label}</DashboardFieldLabel>
      {editMode ? (
        <DashboardInput
          type={type}
          value={value}
          onChange={(e) => onChange?.(e.target.value)}
        />
      ) : (
        <div className="text-sm font-bold text-on-surface py-2">{value || '\u2014'}</div>
      )}
    </div>
  );
}

function TagList({ label, items, editMode, placeholder, onChange }: {
  label: string;
  items: string[];
  editMode: boolean;
  placeholder: string;
  onChange?: (items: string[]) => void;
}) {
  const [input, setInput] = useState('');

  return (
    <div>
      <DashboardFieldLabel className="text-[9px]">{label}</DashboardFieldLabel>
      <div className="flex flex-wrap gap-1.5">
        {items.map((item, i) => (
          <span key={i} className="inline-flex items-center gap-1 px-3 py-1 bg-primary/5 text-primary text-xs font-bold rounded-lg border border-primary/10">
            {item}
            {editMode && (
              <button
                onClick={() => onChange?.(items.filter((_, j) => j !== i))}
                className="ml-0.5 text-primary/40 hover:text-primary"
              >
                <span className="material-symbols-outlined text-xs">close</span>
              </button>
            )}
          </span>
        ))}
        {editMode && (
          <div className="flex items-center gap-1">
            <DashboardInput
              variant="compact"
              className="w-32"
              placeholder={placeholder}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && input.trim()) {
                  onChange?.([...items, input.trim()]);
                  setInput('');
                }
              }}
            />
          </div>
        )}
      </div>
      {items.length === 0 && !editMode && <div className="text-sm text-on-surface-variant/40 py-2">{'\u2014'}</div>}
    </div>
  );
}

/** Normalize extracurriculars/awards data into a flat string array for display. */
function normalizeList(data: unknown): string[] {
  if (!data) return [];
  if (Array.isArray(data)) {
    return data.map((item) => {
      if (typeof item === 'string') return item;
      if (typeof item === 'object' && item !== null) {
        // Extract meaningful string from dict: try common keys
        const obj = item as Record<string, unknown>;
        return obj.name ?? obj.title ?? obj.activity ?? JSON.stringify(obj);
      }
      return String(item);
    }) as string[];
  }
  if (typeof data === 'object' && data !== null) {
    // Dict format like { activities: [...], list: [...] }
    const obj = data as Record<string, unknown>;
    const inner = obj.activities ?? obj.list ?? obj.items ?? Object.values(obj).flat();
    return normalizeList(inner);
  }
  return [String(data)];
}

function ProfileMetric({
  label,
  value,
  accent = 'text-primary',
}: {
  label: string;
  value: string;
  accent?: string;
}) {
  return (
    <div className="dashboard-surface-muted px-4 py-4">
      <div className="dashboard-summary-label">{label}</div>
      <div className={`mt-2 text-2xl font-black tracking-tight ${accent}`}>{value}</div>
    </div>
  );
}

function buildDraftFromPortfolio(portfolio: StudentPortfolioResponse): Record<string, any> {
  return {
    name: portfolio.identity.name,
    gpa: String(portfolio.academics.gpa),
    gpa_scale: portfolio.academics.gpa_scale,
    sat_total: String(portfolio.academics.sat_total ?? ''),
    act_composite: String(portfolio.academics.act_composite ?? ''),
    toefl_total: String(portfolio.academics.toefl_total ?? ''),
    curriculum_type: portfolio.academics.curriculum_type,
    intended_majors: portfolio.academics.intended_majors ?? [],
    ap_courses: portfolio.academics.ap_courses ?? [],
    extracurriculars: normalizeList(portfolio.activities.extracurriculars),
    awards: normalizeList(portfolio.activities.awards),
    budget_usd: String(portfolio.finance.budget_usd ?? ''),
    target_year: String(portfolio.identity.target_year),
    ed_preference: portfolio.strategy.ed_preference ?? '',
    need_financial_aid: portfolio.finance.need_financial_aid,
  };
}

interface ProfilePanelProps {
  studentId: string | null;
}

export function ProfilePanel({ studentId }: ProfilePanelProps) {
  const { t, setStudentName } = useApp();
  const [portfolio, setPortfolio] = useState<StudentPortfolioResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [editMode, setEditMode] = useState(false);
  const [saving, setSaving] = useState(false);
  const [draft, setDraft] = useState<Record<string, any>>({});

  const loadPortfolio = useCallback(async (id: string) => {
    setIsLoading(true);
    setError(null);
    try {
      const next = await portfolioApi.get(id);
      setPortfolio(next);
      return next;
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
      return null;
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    if (studentId) {
      loadPortfolio(studentId);
    }
  }, [studentId, loadPortfolio]);

  useEffect(() => {
    if (portfolio) {
      setDraft(buildDraftFromPortfolio(portfolio));
    }
  }, [portfolio]);

  const handleSave = useCallback(async () => {
    if (!studentId) return;
    setSaving(true);
    try {
      await portfolioApi.patch(studentId, {
        identity: {
          name: draft.name,
          target_year: Number(draft.target_year) || 2027,
        },
        academics: {
          gpa: Number(draft.gpa) || 0,
          gpa_scale: draft.gpa_scale,
          sat_total: Number(draft.sat_total) || null,
          act_composite: Number(draft.act_composite) || null,
          toefl_total: Number(draft.toefl_total) || null,
          curriculum_type: draft.curriculum_type,
          intended_majors: draft.intended_majors,
          ap_courses: draft.ap_courses,
        },
        activities: {
          extracurriculars: draft.extracurriculars,
          awards: draft.awards,
        },
        finance: {
          budget_usd: Number(draft.budget_usd) || 0,
          need_financial_aid: draft.need_financial_aid,
        },
        strategy: {
          ed_preference: draft.ed_preference || null,
        },
      });
      setStudentName(draft.name);
      setEditMode(false);
      await loadPortfolio(studentId);
    } catch { /* ignore */ }
    setSaving(false);
  }, [studentId, draft, loadPortfolio, setStudentName]);

  const p = (key: string): string => String(draft[key] ?? '');
  const completionPct = portfolio?.completion.completion_pct != null ? Math.round(portfolio.completion.completion_pct * 100) : 0;
  const summaryBudget = portfolio?.finance.budget_usd ? `$${Math.round(portfolio.finance.budget_usd / 1000)}K` : '\u2014';
  const summaryTests = portfolio?.academics.sat_total ? `${portfolio.academics.sat_total}` : portfolio?.academics.act_composite ? `ACT ${portfolio.academics.act_composite}` : '\u2014';

  return (
    <AnimatedWorkspacePage className="w-full bg-background font-body">
      <section className="flex h-full w-full flex-col overflow-hidden">
      <header className="sticky top-0 z-20 flex min-h-16 items-center justify-between border-b border-outline-variant/10 bg-background/90 px-4 py-3 backdrop-blur-md sm:px-6 lg:px-8">
        <div>
          <h1 className="font-headline text-lg font-black text-on-surface tracking-tight">
            {t.prof_title}
          </h1>
          <p className="text-[9px] text-on-surface-variant font-bold tracking-[0.1em] uppercase">
            {t.prof_subtitle}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {editMode ? (
            <>
              <button
                onClick={() => {
                  setEditMode(false);
                  if (portfolio) setDraft(buildDraftFromPortfolio(portfolio));
                }}
                className="px-4 py-2 text-on-surface-variant text-xs font-bold rounded-xl hover:bg-surface-container-high transition-colors"
              >
                {t.prof_cancel}
              </button>
              <button
                onClick={handleSave}
                disabled={saving}
                className="px-4 py-2 bg-primary text-on-primary text-xs font-bold rounded-xl hover:brightness-110 transition-all shadow-md disabled:opacity-50 flex items-center gap-1.5"
              >
                <span className={`material-symbols-outlined text-sm ${saving ? 'animate-spin' : ''}`}>
                  {saving ? 'progress_activity' : 'save'}
                </span>
                {saving ? t.prof_saving : t.prof_save}
              </button>
            </>
          ) : (
            <button
              onClick={() => setEditMode(true)}
              className="px-4 py-2 bg-primary/5 text-primary text-xs font-bold rounded-xl border border-primary/15 hover:bg-primary/10 transition-colors flex items-center gap-1.5"
            >
              <span className="material-symbols-outlined text-sm">edit</span>
              {t.prof_edit}
            </button>
          )}
        </div>
      </header>

      <div className="flex-1 overflow-y-auto px-4 py-5 sm:px-6 sm:py-6 lg:px-8">
        {/* Loading */}
        {isLoading && (
          <div className="space-y-4">
            <div className="animate-pulse bg-surface-container-high/60 rounded-2xl h-48" />
            <div className="animate-pulse bg-surface-container-high/60 rounded-2xl h-64" />
          </div>
        )}

        {/* No portfolio */}
        {!isLoading && !portfolio && (
          <div className="flex flex-col items-center justify-center py-24 text-center">
            <div className="w-20 h-20 rounded-3xl bg-surface-container-high/40 flex items-center justify-center mb-6">
              <span className="material-symbols-outlined text-4xl text-on-surface-variant/50">person_off</span>
            </div>
            <h3 className="font-headline text-xl font-black text-on-surface mb-2">{t.prof_no_profile}</h3>
            <p className="text-sm text-on-surface-variant/70 max-w-sm leading-relaxed">
              {error
                ? `${t.prof_load_failed}: ${error.message}`
                : studentId
                  ? t.prof_loading_profile
                  : t.prof_no_profile_desc}
            </p>
            {studentId && (
              <button
                onClick={() => loadPortfolio(studentId)}
                className="mt-4 px-4 py-2 bg-primary/5 text-primary text-xs font-bold rounded-xl border border-primary/15 hover:bg-primary/10 transition-colors"
              >
                {t.prof_retry}
              </button>
            )}
          </div>
        )}

        {/* Profile content */}
        {!isLoading && portfolio && (
          <MotionStagger className="space-y-6" delay={0.02} stagger={0.08}>
            <MotionItem role="section">
              <div className="workspace-hero overflow-hidden p-6 sm:p-7 lg:p-8">
                <div className="grid gap-6 xl:grid-cols-[minmax(0,1.45fr)_minmax(320px,0.95fr)]">
                  <div className="space-y-6">
                    <div className="flex items-start gap-4">
                      <div className="flex h-16 w-16 shrink-0 items-center justify-center rounded-[1.35rem] bg-primary text-on-primary shadow-[0_20px_48px_rgba(0,64,161,0.24)]">
                        <span className="material-symbols-outlined text-3xl">person</span>
                      </div>
                      <div className="min-w-0 flex-1">
                        <div className="dashboard-kicker">{t.prof_subtitle}</div>
                        {editMode ? (
                          <DashboardInput
                            variant="hero"
                            className="mt-2"
                            value={p('name')}
                            onChange={(e) => setDraft((d) => ({ ...d, name: e.target.value }))}
                          />
                        ) : (
                          <h2 className="mt-2 font-headline text-3xl font-black tracking-tight text-on-surface">{portfolio.identity.name}</h2>
                        )}
                        <p className="mt-2 max-w-2xl text-sm leading-relaxed text-on-surface-variant/72">
                          {portfolio.academics.curriculum_type} • {t.prof_target_short} {portfolio.identity.target_year} • {portfolio.finance.need_financial_aid ? t.prof_need_aid : t.prof_no_aid}
                        </p>
                        <div className="mt-3 flex flex-wrap gap-2">
                          <span className="dashboard-inline-chip">
                            <span className="material-symbols-outlined text-sm text-primary">school</span>
                            {t.prof_gpa} {p('gpa')} / {p('gpa_scale')}
                          </span>
                          <span className="dashboard-inline-chip">
                            <span className="material-symbols-outlined text-sm text-tertiary">quiz</span>
                            {summaryTests}
                          </span>
                          <span className="dashboard-inline-chip">
                            <span className="material-symbols-outlined text-sm text-on-surface-variant">savings</span>
                            {summaryBudget}
                          </span>
                          {portfolio.completion.profile_completed && (
                            <span className="dashboard-inline-chip border-tertiary/15 bg-tertiary/8 text-tertiary">
                              <span className="material-symbols-outlined text-sm" style={{ fontVariationSettings: "'FILL' 1" }}>verified</span>
                              {t.common_overall} {completionPct}%
                            </span>
                          )}
                        </div>
                      </div>
                    </div>

                    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                      <Field label={t.prof_gpa} value={p('gpa')} editMode={editMode} type="number" onChange={(v) => setDraft((d) => ({ ...d, gpa: v }))} />
                      <Field label={t.prof_gpa_scale} value={p('gpa_scale')} editMode={editMode} onChange={(v) => setDraft((d) => ({ ...d, gpa_scale: v }))} />
                      <Field label={t.prof_target_year} value={p('target_year')} editMode={editMode} type="number" onChange={(v) => setDraft((d) => ({ ...d, target_year: v }))} />
                      <Field label={t.prof_curriculum} value={p('curriculum_type')} editMode={editMode} onChange={(v) => setDraft((d) => ({ ...d, curriculum_type: v }))} />
                    </div>
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2">
                    <ProfileMetric label={t.prof_test_scores} value={summaryTests} />
                    <ProfileMetric label={t.prof_budget} value={summaryBudget} accent="text-tertiary" />
                    <ProfileMetric label={t.prof_strategy} value={(portfolio.strategy.ed_preference || 'RD').toUpperCase()} accent="text-on-surface" />
                    <ProfileMetric label={t.common_overall} value={`${completionPct}%`} accent="text-primary" />
                  </div>
                </div>
              </div>
            </MotionItem>

            <MotionItem role="section">
              <MotionSurface className="p-6 sm:p-7">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-10 h-10 rounded-xl bg-secondary-fixed/30 flex items-center justify-center">
                  <span className="material-symbols-outlined text-on-secondary-fixed-variant text-xl">quiz</span>
                </div>
                <h3 className="font-headline text-base font-black text-on-surface">{t.prof_test_scores}</h3>
              </div>
              <div className="grid grid-cols-3 gap-6">
                <div className="text-center p-4 bg-surface-container-low/40 rounded-2xl border border-outline-variant/5">
                  <div className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-2">{t.prof_sat_total}</div>
                  {editMode ? (
                    <DashboardInput type="number" variant="metric" value={p('sat_total')} onChange={(e) => setDraft((d) => ({ ...d, sat_total: e.target.value }))} />
                  ) : (
                    <div className="text-2xl font-black text-on-surface">{portfolio.academics.sat_total ?? '\u2014'}</div>
                  )}
                </div>
                <div className="text-center p-4 bg-surface-container-low/40 rounded-2xl border border-outline-variant/5">
                  <div className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-2">{t.prof_act_composite}</div>
                  {editMode ? (
                    <DashboardInput type="number" variant="metric" value={p('act_composite')} onChange={(e) => setDraft((d) => ({ ...d, act_composite: e.target.value }))} />
                  ) : (
                    <div className="text-2xl font-black text-on-surface">{portfolio.academics.act_composite ?? '\u2014'}</div>
                  )}
                </div>
                <div className="text-center p-4 bg-surface-container-low/40 rounded-2xl border border-outline-variant/5">
                  <div className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-2">{t.prof_toefl_total}</div>
                  {editMode ? (
                    <DashboardInput type="number" variant="metric" value={p('toefl_total')} onChange={(e) => setDraft((d) => ({ ...d, toefl_total: e.target.value }))} />
                  ) : (
                    <div className="text-2xl font-black text-on-surface">{portfolio.academics.toefl_total ?? '\u2014'}</div>
                  )}
                </div>
              </div>
              </MotionSurface>
            </MotionItem>

            {/* Academic Interests */}
            <MotionItem role="section">
              <MotionSurface className="p-6 sm:p-7">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-10 h-10 rounded-xl bg-tertiary/10 flex items-center justify-center">
                  <span className="material-symbols-outlined text-tertiary text-xl">interests</span>
                </div>
                <h3 className="font-headline text-base font-black text-on-surface">{t.prof_interests}</h3>
              </div>
              <div className="space-y-5">
                <TagList label={t.prof_majors} items={draft.intended_majors ?? []} editMode={editMode} placeholder={t.prof_tag_placeholder} onChange={(v) => setDraft((d) => ({ ...d, intended_majors: v }))} />
                <TagList label={t.prof_ap} items={draft.ap_courses ?? []} editMode={editMode} placeholder={t.prof_tag_placeholder} onChange={(v) => setDraft((d) => ({ ...d, ap_courses: v }))} />
                <TagList label={t.prof_ecs} items={draft.extracurriculars ?? []} editMode={editMode} placeholder={t.prof_tag_placeholder} onChange={(v) => setDraft((d) => ({ ...d, extracurriculars: v }))} />
                <TagList label={t.prof_awards} items={draft.awards ?? []} editMode={editMode} placeholder={t.prof_tag_placeholder} onChange={(v) => setDraft((d) => ({ ...d, awards: v }))} />
              </div>
              </MotionSurface>
            </MotionItem>

            {/* Financial */}
            <MotionItem role="section">
              <MotionSurface className="p-6 sm:p-7">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center">
                  <span className="material-symbols-outlined text-primary text-xl">account_balance_wallet</span>
                </div>
                <h3 className="font-headline text-base font-black text-on-surface">{t.prof_financial}</h3>
              </div>
              <div className="grid grid-cols-2 gap-6">
                <Field
                  label={t.prof_budget}
                  value={p('budget_usd')}
                  editMode={editMode}
                  type="number"
                  onChange={(v) => setDraft((d) => ({ ...d, budget_usd: v }))}
                />
                <div>
                  <label className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-1 block">
                    {t.prof_need_aid}
                  </label>
                  {editMode ? (
                    <DashboardSegmentedGroup
                      type="single"
                      value={draft.need_financial_aid ? 'yes' : 'no'}
                      onValueChange={(value) => setDraft((d) => ({ ...d, need_financial_aid: value === 'yes' }))}
                      className="pt-1"
                    >
                      {[true, false].map((val) => (
                        <DashboardSegmentedItem
                          key={String(val)}
                          value={val ? 'yes' : 'no'}
                          accent="primary"
                          className="min-w-[5.5rem] justify-center"
                        >
                          {val ? t.common_yes : t.common_no}
                        </DashboardSegmentedItem>
                      ))}
                    </DashboardSegmentedGroup>
                  ) : (
                    <div className="text-sm font-bold text-on-surface py-2">{portfolio.finance.need_financial_aid ? t.common_yes : t.common_no}</div>
                  )}
                </div>
              </div>
              </MotionSurface>
            </MotionItem>

            {/* Application Strategy */}
            <MotionItem role="section">
              <MotionSurface className="p-6 sm:p-7">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-10 h-10 rounded-xl bg-secondary-fixed/30 flex items-center justify-center">
                  <span className="material-symbols-outlined text-on-secondary-fixed-variant text-xl">strategy</span>
                </div>
                <h3 className="font-headline text-base font-black text-on-surface">{t.prof_strategy}</h3>
              </div>
              <div>
                <label className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-1 block">
                  {t.prof_ed_pref}
                </label>
                {editMode ? (
                  <DashboardSegmentedGroup
                    type="single"
                    value={draft.ed_preference || ED_NONE_VALUE}
                    onValueChange={(value) => setDraft((d) => ({ ...d, ed_preference: value === ED_NONE_VALUE ? '' : value.toLowerCase() }))}
                  >
                    {['ED', 'EA', 'REA', 'RD', ''].map((opt) => (
                      <DashboardSegmentedItem
                        key={opt}
                        value={opt || ED_NONE_VALUE}
                        accent={opt ? 'primary' : 'neutral'}
                        className="min-w-[4.5rem] justify-center uppercase"
                      >
                        {opt || t.prof_none}
                      </DashboardSegmentedItem>
                    ))}
                  </DashboardSegmentedGroup>
                ) : (
                  <div className="text-sm font-bold text-on-surface py-2 uppercase">{portfolio.strategy.ed_preference || t.prof_not_set}</div>
                )}
              </div>
              </MotionSurface>
            </MotionItem>

            {/* Info banner */}
            <MotionItem role="section">
              <div className="dashboard-surface-soft flex items-start gap-4 p-6">
              <span className="material-symbols-outlined text-primary text-xl mt-0.5" style={{ fontVariationSettings: "'FILL' 1" }}>info</span>
              <div>
                <p className="text-xs text-on-surface/70 leading-relaxed">
                  {t.prof_info}
                </p>
              </div>
              </div>
            </MotionItem>
          </MotionStagger>
        )}

        <div className="h-12" />
      </div>
    </section>
    </AnimatedWorkspacePage>
  );
}
