import React, { useState, useEffect, useCallback } from 'react';
import { useStudent } from '../../hooks/useStudent';
import { useApp } from '../../context/AppContext';
import { studentsApi } from '../../lib/api/students';
import type { StudentResponse } from '../../lib/types';

function Field({ label, value, editMode, type = 'text', onChange }: {
  label: string;
  value: string;
  editMode: boolean;
  type?: string;
  onChange?: (v: string) => void;
}) {
  return (
    <div>
      <label className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-1 block">{label}</label>
      {editMode ? (
        <input
          type={type}
          className="w-full bg-surface-container-high/40 border border-outline-variant/20 rounded-xl px-4 py-2.5 text-sm text-on-surface outline-none focus:border-primary transition-colors"
          value={value}
          onChange={(e) => onChange?.(e.target.value)}
        />
      ) : (
        <div className="text-sm font-bold text-on-surface py-2">{value || '\u2014'}</div>
      )}
    </div>
  );
}

function TagList({ label, items, editMode, onChange }: {
  label: string;
  items: string[];
  editMode: boolean;
  onChange?: (items: string[]) => void;
}) {
  const [input, setInput] = useState('');

  return (
    <div>
      <label className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-1 block">{label}</label>
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
            <input
              className="bg-surface-container-high/40 border border-outline-variant/20 rounded-lg px-3 py-1 text-xs outline-none focus:border-primary w-32"
              placeholder="Add..."
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

interface ProfilePanelProps {
  studentId: string | null;
}

export function ProfilePanel({ studentId }: ProfilePanelProps) {
  const { t, setStudentName } = useApp();
  const { student, isLoading, error, fetchStudent } = useStudent();
  const [editMode, setEditMode] = useState(false);
  const [saving, setSaving] = useState(false);
  const [draft, setDraft] = useState<Record<string, any>>({});

  useEffect(() => {
    if (studentId) {
      fetchStudent(studentId);
    }
  }, [studentId, fetchStudent]);

  useEffect(() => {
    if (student) {
      setDraft({
        name: student.name,
        gpa: String(student.gpa),
        gpa_scale: student.gpa_scale,
        sat_total: String(student.sat_total ?? ''),
        act_composite: String(student.act_composite ?? ''),
        toefl_total: String(student.toefl_total ?? ''),
        curriculum_type: student.curriculum_type,
        intended_majors: student.intended_majors ?? [],
        ap_courses: student.ap_courses ?? [],
        extracurriculars: normalizeList(student.extracurriculars),
        awards: normalizeList(student.awards),
        budget_usd: String(student.budget_usd ?? ''),
        target_year: String(student.target_year),
        ed_preference: student.ed_preference ?? '',
        need_financial_aid: student.need_financial_aid,
      });
    }
  }, [student]);

  const handleSave = useCallback(async () => {
    if (!studentId) return;
    setSaving(true);
    try {
      await studentsApi.update(studentId, {
        name: draft.name,
        gpa: Number(draft.gpa) || 0,
        gpa_scale: draft.gpa_scale,
        sat_total: Number(draft.sat_total) || null,
        act_composite: Number(draft.act_composite) || null,
        toefl_total: Number(draft.toefl_total) || null,
        curriculum_type: draft.curriculum_type,
        intended_majors: draft.intended_majors,
        ap_courses: draft.ap_courses,
        budget_usd: Number(draft.budget_usd) || null,
        target_year: Number(draft.target_year) || 2027,
        ed_preference: draft.ed_preference || null,
        need_financial_aid: draft.need_financial_aid,
      });
      setStudentName(draft.name);
      setEditMode(false);
      await fetchStudent(studentId);
    } catch { /* ignore */ }
    setSaving(false);
  }, [studentId, draft, fetchStudent, setStudentName]);

  const p = (key: string): string => String(draft[key] ?? '');

  return (
    <section className="w-full bg-background flex flex-col h-full overflow-hidden font-body">
      <header className="h-16 px-10 flex items-center justify-between sticky top-0 bg-background/90 backdrop-blur-md z-20 border-b border-outline-variant/10">
        <div>
          <h1 className="font-headline text-lg font-black text-on-surface tracking-tight">
            {t.prof_title ?? 'My Profile'}
          </h1>
          <p className="text-[9px] text-on-surface-variant font-bold tracking-[0.1em] uppercase">
            {t.prof_subtitle ?? 'Academic Profile & Preferences'}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {editMode ? (
            <>
              <button
                onClick={() => { setEditMode(false); if (student) setDraft({ name: student.name, gpa: String(student.gpa), gpa_scale: student.gpa_scale, sat_total: String(student.sat_total ?? ''), act_composite: String(student.act_composite ?? ''), toefl_total: String(student.toefl_total ?? ''), curriculum_type: student.curriculum_type, intended_majors: student.intended_majors ?? [], ap_courses: student.ap_courses ?? [], budget_usd: String(student.budget_usd ?? ''), target_year: String(student.target_year), ed_preference: student.ed_preference ?? '', need_financial_aid: student.need_financial_aid }); }}
                className="px-4 py-2 text-on-surface-variant text-xs font-bold rounded-xl hover:bg-surface-container-high transition-colors"
              >
                {t.prof_cancel ?? 'Cancel'}
              </button>
              <button
                onClick={handleSave}
                disabled={saving}
                className="px-4 py-2 bg-primary text-on-primary text-xs font-bold rounded-xl hover:brightness-110 transition-all shadow-md disabled:opacity-50 flex items-center gap-1.5"
              >
                <span className={`material-symbols-outlined text-sm ${saving ? 'animate-spin' : ''}`}>
                  {saving ? 'progress_activity' : 'save'}
                </span>
                {saving ? (t.prof_saving ?? 'Saving...') : (t.prof_save ?? 'Save Profile')}
              </button>
            </>
          ) : (
            <button
              onClick={() => setEditMode(true)}
              className="px-4 py-2 bg-primary/5 text-primary text-xs font-bold rounded-xl border border-primary/15 hover:bg-primary/10 transition-colors flex items-center gap-1.5"
            >
              <span className="material-symbols-outlined text-sm">edit</span>
              {t.prof_edit ?? 'Edit Profile'}
            </button>
          )}
        </div>
      </header>

      <div className="flex-1 overflow-y-auto px-10 py-8 space-y-8">
        {/* Loading */}
        {isLoading && (
          <div className="space-y-4">
            <div className="animate-pulse bg-surface-container-high/60 rounded-2xl h-48" />
            <div className="animate-pulse bg-surface-container-high/60 rounded-2xl h-64" />
          </div>
        )}

        {/* No student */}
        {!isLoading && !student && (
          <div className="flex flex-col items-center justify-center py-24 text-center">
            <div className="w-20 h-20 rounded-3xl bg-surface-container-high/40 flex items-center justify-center mb-6">
              <span className="material-symbols-outlined text-4xl text-on-surface-variant/50">person_off</span>
            </div>
            <h3 className="font-headline text-xl font-black text-on-surface mb-2">{t.prof_no_profile ?? 'No Profile'}</h3>
            <p className="text-sm text-on-surface-variant/70 max-w-sm leading-relaxed">
              {error
                ? `Failed to load profile: ${error.message}`
                : studentId
                  ? 'Loading profile data...'
                  : (t.prof_no_profile_desc ?? 'Start a new session to create your student profile.')}
            </p>
            {studentId && (
              <button
                onClick={() => fetchStudent(studentId)}
                className="mt-4 px-4 py-2 bg-primary/5 text-primary text-xs font-bold rounded-xl border border-primary/15 hover:bg-primary/10 transition-colors"
              >
                Retry
              </button>
            )}
          </div>
        )}

        {/* Profile content */}
        {!isLoading && student && (
          <>
            {/* Identity */}
            <div className="bg-surface-container-lowest rounded-3xl p-8 border border-outline-variant/10">
              <div className="flex items-center gap-5 mb-8">
                <div className="w-16 h-16 bg-primary rounded-2xl flex items-center justify-center shadow-lg shadow-primary/20">
                  <span className="material-symbols-outlined text-on-primary text-3xl">person</span>
                </div>
                <div className="flex-1">
                  {editMode ? (
                    <input
                      className="font-headline text-2xl font-black text-on-surface bg-transparent border-b-2 border-primary outline-none w-full"
                      value={p('name')}
                      onChange={(e) => setDraft((d) => ({ ...d, name: e.target.value }))}
                    />
                  ) : (
                    <h2 className="font-headline text-2xl font-black text-on-surface">{student.name}</h2>
                  )}
                  <p className="text-xs text-on-surface-variant/60 mt-1">
                    {student.curriculum_type} &bull; Target {student.target_year}
                    {student.profile_completed && (
                      <span className="ml-2 inline-flex items-center gap-1 text-tertiary">
                        <span className="material-symbols-outlined text-xs" style={{ fontVariationSettings: "'FILL' 1" }}>verified</span>
                        Complete
                      </span>
                    )}
                  </p>
                </div>
              </div>

              <div className="grid grid-cols-4 gap-4">
                <Field label="GPA" value={p('gpa')} editMode={editMode} type="number" onChange={(v) => setDraft((d) => ({ ...d, gpa: v }))} />
                <Field label="GPA Scale" value={p('gpa_scale')} editMode={editMode} onChange={(v) => setDraft((d) => ({ ...d, gpa_scale: v }))} />
                <Field label="Target Year" value={p('target_year')} editMode={editMode} type="number" onChange={(v) => setDraft((d) => ({ ...d, target_year: v }))} />
                <Field label="Curriculum" value={p('curriculum_type')} editMode={editMode} onChange={(v) => setDraft((d) => ({ ...d, curriculum_type: v }))} />
              </div>
            </div>

            {/* Test Scores */}
            <div className="bg-surface-container-lowest rounded-3xl p-8 border border-outline-variant/10">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-10 h-10 rounded-xl bg-secondary-fixed/30 flex items-center justify-center">
                  <span className="material-symbols-outlined text-on-secondary-fixed-variant text-xl">quiz</span>
                </div>
                <h3 className="font-headline text-base font-black text-on-surface">{t.prof_test_scores ?? 'Test Scores'}</h3>
              </div>
              <div className="grid grid-cols-3 gap-6">
                <div className="text-center p-4 bg-surface-container-low/40 rounded-2xl border border-outline-variant/5">
                  <div className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-2">SAT Total</div>
                  {editMode ? (
                    <input type="number" className="w-full text-center text-2xl font-black bg-transparent outline-none border-b-2 border-primary" value={p('sat_total')} onChange={(e) => setDraft((d) => ({ ...d, sat_total: e.target.value }))} />
                  ) : (
                    <div className="text-2xl font-black text-on-surface">{student.sat_total ?? '\u2014'}</div>
                  )}
                </div>
                <div className="text-center p-4 bg-surface-container-low/40 rounded-2xl border border-outline-variant/5">
                  <div className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-2">ACT Composite</div>
                  {editMode ? (
                    <input type="number" className="w-full text-center text-2xl font-black bg-transparent outline-none border-b-2 border-primary" value={p('act_composite')} onChange={(e) => setDraft((d) => ({ ...d, act_composite: e.target.value }))} />
                  ) : (
                    <div className="text-2xl font-black text-on-surface">{student.act_composite ?? '\u2014'}</div>
                  )}
                </div>
                <div className="text-center p-4 bg-surface-container-low/40 rounded-2xl border border-outline-variant/5">
                  <div className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-2">TOEFL Total</div>
                  {editMode ? (
                    <input type="number" className="w-full text-center text-2xl font-black bg-transparent outline-none border-b-2 border-primary" value={p('toefl_total')} onChange={(e) => setDraft((d) => ({ ...d, toefl_total: e.target.value }))} />
                  ) : (
                    <div className="text-2xl font-black text-on-surface">{student.toefl_total ?? '\u2014'}</div>
                  )}
                </div>
              </div>
            </div>

            {/* Academic Interests */}
            <div className="bg-surface-container-lowest rounded-3xl p-8 border border-outline-variant/10">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-10 h-10 rounded-xl bg-tertiary/10 flex items-center justify-center">
                  <span className="material-symbols-outlined text-tertiary text-xl">interests</span>
                </div>
                <h3 className="font-headline text-base font-black text-on-surface">{t.prof_interests ?? 'Academic Interests'}</h3>
              </div>
              <div className="space-y-5">
                <TagList label={t.prof_majors ?? 'Intended Majors'} items={draft.intended_majors ?? []} editMode={editMode} onChange={(v) => setDraft((d) => ({ ...d, intended_majors: v }))} />
                <TagList label={t.prof_ap ?? 'AP Courses'} items={draft.ap_courses ?? []} editMode={editMode} onChange={(v) => setDraft((d) => ({ ...d, ap_courses: v }))} />
                <TagList label={t.prof_ecs ?? 'Extracurriculars'} items={draft.extracurriculars ?? []} editMode={editMode} onChange={(v) => setDraft((d) => ({ ...d, extracurriculars: v }))} />
                <TagList label={t.prof_awards ?? 'Awards'} items={draft.awards ?? []} editMode={editMode} onChange={(v) => setDraft((d) => ({ ...d, awards: v }))} />
              </div>
            </div>

            {/* Financial */}
            <div className="bg-surface-container-lowest rounded-3xl p-8 border border-outline-variant/10">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center">
                  <span className="material-symbols-outlined text-primary text-xl">account_balance_wallet</span>
                </div>
                <h3 className="font-headline text-base font-black text-on-surface">{t.prof_financial ?? 'Financial'}</h3>
              </div>
              <div className="grid grid-cols-2 gap-6">
                <Field
                  label={t.prof_budget ?? 'Annual Budget (USD)'}
                  value={p('budget_usd')}
                  editMode={editMode}
                  type="number"
                  onChange={(v) => setDraft((d) => ({ ...d, budget_usd: v }))}
                />
                <div>
                  <label className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-1 block">
                    {t.prof_need_aid ?? 'Need Financial Aid'}
                  </label>
                  {editMode ? (
                    <div className="flex gap-2 pt-1">
                      {[true, false].map((val) => (
                        <button
                          key={String(val)}
                          onClick={() => setDraft((d) => ({ ...d, need_financial_aid: val }))}
                          className={`px-4 py-2 rounded-xl text-xs font-bold transition-colors ${
                            draft.need_financial_aid === val
                              ? 'bg-primary/10 text-primary border border-primary/20'
                              : 'bg-surface-container-high/30 text-on-surface-variant border border-transparent'
                          }`}
                        >
                          {val ? 'Yes' : 'No'}
                        </button>
                      ))}
                    </div>
                  ) : (
                    <div className="text-sm font-bold text-on-surface py-2">{student.need_financial_aid ? 'Yes' : 'No'}</div>
                  )}
                </div>
              </div>
            </div>

            {/* Application Strategy */}
            <div className="bg-surface-container-lowest rounded-3xl p-8 border border-outline-variant/10">
              <div className="flex items-center gap-3 mb-6">
                <div className="w-10 h-10 rounded-xl bg-secondary-fixed/30 flex items-center justify-center">
                  <span className="material-symbols-outlined text-on-secondary-fixed-variant text-xl">strategy</span>
                </div>
                <h3 className="font-headline text-base font-black text-on-surface">{t.prof_strategy ?? 'Application Strategy'}</h3>
              </div>
              <div>
                <label className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mb-1 block">
                  {t.prof_ed_pref ?? 'ED/EA Preference'}
                </label>
                {editMode ? (
                  <div className="flex gap-2">
                    {['ED', 'EA', 'REA', 'RD', ''].map((opt) => (
                      <button
                        key={opt}
                        onClick={() => setDraft((d) => ({ ...d, ed_preference: opt.toLowerCase() || '' }))}
                        className={`px-4 py-2 rounded-xl text-xs font-bold uppercase transition-colors ${
                          (draft.ed_preference || '').toUpperCase() === opt || (!draft.ed_preference && !opt)
                            ? 'bg-primary/10 text-primary border border-primary/20'
                            : 'bg-surface-container-high/30 text-on-surface-variant border border-transparent'
                        }`}
                      >
                        {opt || 'None'}
                      </button>
                    ))}
                  </div>
                ) : (
                  <div className="text-sm font-bold text-on-surface py-2 uppercase">{student.ed_preference || 'Not set'}</div>
                )}
              </div>
            </div>

            {/* Info banner */}
            <div className="p-6 bg-surface-container-high/20 rounded-2xl border border-outline-variant/10 flex items-start gap-4">
              <span className="material-symbols-outlined text-primary text-xl mt-0.5" style={{ fontVariationSettings: "'FILL' 1" }}>info</span>
              <div>
                <p className="text-xs text-on-surface/70 leading-relaxed">
                  {t.prof_info ?? 'Your profile is used by the AI to generate personalized school recommendations and fit scores. Keep it up to date for the best results.'}
                </p>
              </div>
            </div>
          </>
        )}

        <div className="h-12" />
      </div>
    </section>
  );
}
