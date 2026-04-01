import React, { useState, useEffect } from 'react';
import { useSchools } from '../../hooks/useSchools';
import { useEvaluations } from '../../hooks/useEvaluations';
import { useApp } from '../../context/AppContext';
import { evaluationsApi } from '../../lib/api/evaluations';
import { schoolsApi } from '../../lib/api/schools';
import type { SchoolResponse } from '../../lib/types';

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return 'N/A';
  return `$${value.toLocaleString()}`;
}

const STATE_OPTIONS = [
  '', 'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
  'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
  'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
  'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
  'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC',
];

interface ResourcesPanelProps {
  studentId: string | null;
}

export function ResourcesPanel({ studentId }: ResourcesPanelProps) {
  const { t } = useApp();
  const { schools, total, isLoading, search } = useSchools();
  const { evaluations, refetch: refetchEvals } = useEvaluations(studentId);
  const [query, setQuery] = useState('');
  const [showFilters, setShowFilters] = useState(false);
  const [stateFilter, setStateFilter] = useState('');
  const [maxRank, setMaxRank] = useState('');
  const [lookupResult, setLookupResult] = useState<SchoolResponse | null>(null);
  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookupError, setLookupError] = useState(false);

  const evaluatedSchoolIds = new Set(evaluations.map((e) => e.school_id));

  useEffect(() => {
    search({ per_page: '20' });
  }, []);

  const handleSearch = () => {
    search({
      query: query || undefined,
      state: stateFilter || undefined,
      max_rank: maxRank || undefined,
      per_page: '20',
      page: '1',
    });
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') handleSearch();
  };

  const clearFilters = () => {
    setQuery('');
    setStateFilter('');
    setMaxRank('');
    search({ per_page: '20', page: '1' });
  };

  return (
    <section className="w-full bg-background flex flex-col h-full overflow-hidden font-body">
      <header className="h-16 px-10 flex items-center justify-between sticky top-0 bg-background/90 backdrop-blur-md z-20 border-b border-outline-variant/10">
        <div>
          <h1 className="font-headline text-lg font-black text-on-surface tracking-tight">{t.disc_title}</h1>
          <p className="text-[9px] text-on-surface-variant font-bold tracking-[0.1em] uppercase">
            {t.disc_subtitle} {total > 0 && `\u2022 ${total} Schools`}
          </p>
        </div>
        <button
          onClick={() => setShowFilters(!showFilters)}
          className={`w-10 h-10 rounded-full flex items-center justify-center transition-colors ${
            showFilters ? 'bg-primary/10 text-primary' : 'text-on-surface-variant hover:bg-surface-container-high'
          }`}
        >
          <span className="material-symbols-outlined text-[20px]">filter_list</span>
        </button>
      </header>

      <div className="flex-1 overflow-y-auto px-10 py-8 space-y-8">
        <div className="space-y-4">
          <div className="relative flex items-center bg-surface-container-lowest rounded-2xl px-6 py-3 border border-outline-variant/20 focus-within:border-primary-fixed-dim focus-within:ring-4 focus-within:ring-primary/5 transition-all">
            <span className="material-symbols-outlined text-on-surface-variant/50 text-xl mr-3">search</span>
            <input
              className="flex-1 bg-transparent border-none focus:ring-0 text-sm placeholder:text-on-surface-variant/60 py-1 outline-none"
              placeholder={t.disc_search_placeholder}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={handleKeyDown}
            />
            <button
              onClick={handleSearch}
              className="ml-3 px-4 py-2 bg-primary text-on-primary rounded-xl text-xs font-bold hover:brightness-110 transition-all"
            >
              {t.sl_search}
            </button>
          </div>

          {showFilters && (
            <div className="bg-surface-container-lowest rounded-2xl p-6 border border-outline-variant/10 space-y-4">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-on-surface uppercase tracking-widest">{t.disc_filters}</span>
                <button onClick={clearFilters} className="text-xs text-primary font-bold hover:underline">
                  {t.disc_clear_all}
                </button>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-1 block">{t.disc_state}</label>
                  <select
                    className="w-full bg-surface-container-high/40 border border-outline-variant/20 rounded-xl px-4 py-2.5 text-sm text-on-surface outline-none focus:border-primary transition-colors"
                    value={stateFilter}
                    onChange={(e) => setStateFilter(e.target.value)}
                  >
                    <option value="">{t.disc_all_states}</option>
                    {STATE_OPTIONS.filter(Boolean).map((s) => (
                      <option key={s} value={s}>{s}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-1 block">{t.disc_max_rank}</label>
                  <input
                    className="w-full bg-surface-container-high/40 border border-outline-variant/20 rounded-xl px-4 py-2.5 text-sm text-on-surface outline-none focus:border-primary transition-colors"
                    type="number"
                    placeholder="e.g. 50"
                    value={maxRank}
                    onChange={(e) => setMaxRank(e.target.value)}
                  />
                </div>
              </div>
              <button
                onClick={handleSearch}
                className="w-full py-2.5 bg-primary/5 text-primary rounded-xl text-xs font-bold uppercase tracking-widest hover:bg-primary/10 transition-colors border border-primary/15"
              >
                {t.disc_apply_filters}
              </button>
            </div>
          )}
        </div>

        {isLoading && (
          <div className="space-y-4">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="animate-pulse bg-surface-container-high/60 rounded-2xl h-32" />
            ))}
          </div>
        )}

        {!isLoading && schools.length === 0 && (
          <div className="flex flex-col items-center justify-center py-16 text-center">
            <div className="w-20 h-20 rounded-3xl bg-surface-container-high/40 flex items-center justify-center mb-6">
              <span className="material-symbols-outlined text-4xl text-on-surface-variant/50">search_off</span>
            </div>
            <h3 className="font-headline text-xl font-black text-on-surface mb-2">{t.disc_empty_title}</h3>
            <p className="text-sm text-on-surface-variant/70 max-w-sm leading-relaxed mb-6">{t.disc_empty_desc}</p>
            {query && (
              <button
                onClick={async () => {
                  setLookupLoading(true);
                  setLookupError(false);
                  setLookupResult(null);
                  try {
                    const school = await schoolsApi.lookup(query);
                    setLookupResult(school);
                  } catch {
                    setLookupError(true);
                  }
                  setLookupLoading(false);
                }}
                disabled={lookupLoading}
                className="px-5 py-2.5 bg-primary text-on-primary text-sm font-bold rounded-xl hover:brightness-110 transition-all shadow-md disabled:opacity-50 flex items-center gap-2"
              >
                <span className={`material-symbols-outlined text-sm ${lookupLoading ? 'animate-spin' : ''}`}>
                  {lookupLoading ? 'progress_activity' : 'travel_explore'}
                </span>
                {lookupLoading
                  ? (t.disc_agent_searching ?? 'Agent is searching...')
                  : (t.disc_agent_search ?? `Search for "${query}" with AI Agent`)}
              </button>
            )}
          </div>
        )}

        {/* Agent lookup result */}
        {lookupResult && (
          <div className="space-y-3">
            <div className="flex items-center gap-2 px-1">
              <span className="material-symbols-outlined text-tertiary text-base" style={{ fontVariationSettings: "'FILL' 1" }}>auto_awesome</span>
              <span className="text-xs font-bold text-tertiary uppercase tracking-widest">{t.disc_agent_found ?? 'AI Agent Found'}</span>
            </div>
            <SchoolCard
              school={lookupResult}
              isOnList={evaluatedSchoolIds.has(lookupResult.id)}
              studentId={studentId}
              onAdded={() => { refetchEvals(); setLookupResult(null); }}
              t={t}
            />
          </div>
        )}

        {lookupError && (
          <div className="text-center py-4 text-sm text-error/70">
            {t.disc_agent_error ?? 'Could not find this school. Try a different name.'}
          </div>
        )}

        {!isLoading && schools.length > 0 && (
          <div className="space-y-4">
            {schools.map((school) => (
              <SchoolCard
                key={school.id}
                school={school}
                isOnList={evaluatedSchoolIds.has(school.id)}
                studentId={studentId}
                onAdded={refetchEvals}
                t={t}
              />
            ))}
          </div>
        )}

        <div className="h-12" />
      </div>
    </section>
  );
}

function SchoolCard({ school, isOnList, studentId, onAdded, t }: {
  school: SchoolResponse;
  isOnList: boolean;
  studentId: string | null;
  onAdded: () => void;
  t: ReturnType<typeof useApp>['t'];
}) {
  const [adding, setAdding] = useState(false);
  const [added, setAdded] = useState(isOnList);

  const handleAdd = async () => {
    if (!studentId || added) return;
    setAdding(true);
    try {
      await evaluationsApi.evaluate(studentId, school.id);
      setAdded(true);
      onAdded();
    } catch { /* ignore */ }
    setAdding(false);
  };

  return (
    <div className="bg-surface-container-lowest rounded-2xl p-6 border border-outline-variant/10 hover:shadow-md transition-all group">
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-4">
          <div className={`w-12 h-12 rounded-xl flex items-center justify-center transition-colors ${
            added ? 'bg-tertiary/10' : 'bg-surface-container group-hover:bg-primary/10'
          }`}>
            <span className={`material-symbols-outlined text-2xl transition-colors ${
              added ? 'text-tertiary' : 'text-on-surface-variant/50 group-hover:text-primary'
            }`} style={added ? { fontVariationSettings: "'FILL' 1" } : undefined}>
              {added ? 'check_circle' : 'account_balance'}
            </span>
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h3 className="font-headline text-base font-black text-on-surface">{school.name}</h3>
              {added && (
                <span className="px-1.5 py-0.5 bg-tertiary/10 text-tertiary text-[8px] font-bold uppercase tracking-widest rounded">{t.disc_on_list}</span>
              )}
            </div>
            {school.name_cn && <p className="text-xs text-on-surface-variant/60">{school.name_cn}</p>}
            <div className="flex items-center gap-3 mt-1 text-on-surface-variant/70">
              <span className="flex items-center gap-1 text-[11px]">
                <span className="material-symbols-outlined text-xs">location_on</span>
                {school.city}, {school.state}
              </span>
              <span className="text-[11px]">{school.school_type}</span>
              <span className="text-[11px]">{school.size_category}</span>
            </div>
          </div>
        </div>
        {school.us_news_rank && (
          <div className="text-right">
            <div className="text-2xl font-headline font-black text-primary">#{school.us_news_rank}</div>
            <div className="text-[8px] font-bold text-on-surface-variant/60 uppercase tracking-widest">US News</div>
          </div>
        )}
      </div>

      <div className="grid grid-cols-4 gap-3">
        <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
          <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">{t.sl_acceptance}</div>
          <div className="text-sm font-black text-on-surface">{school.acceptance_rate != null ? `${Math.round(school.acceptance_rate <= 1 ? school.acceptance_rate * 100 : school.acceptance_rate)}%` : 'N/A'}</div>
        </div>
        <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
          <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">{t.sl_net_price}</div>
          <div className="text-sm font-black text-on-surface">{school.avg_net_price ? `$${(school.avg_net_price / 1000).toFixed(0)}K` : 'N/A'}</div>
        </div>
        <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
          <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">SAT Range</div>
          <div className="text-sm font-black text-on-surface">{school.sat_25 && school.sat_75 ? `${school.sat_25}–${school.sat_75}` : 'N/A'}</div>
        </div>
        <div className="bg-surface-container-low/40 px-3 py-2.5 rounded-xl border border-outline-variant/5">
          <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">{t.sl_graduation}</div>
          <div className="text-sm font-black text-on-surface">{school.graduation_rate_4yr != null ? `${Math.round(school.graduation_rate_4yr <= 1 ? school.graduation_rate_4yr * 100 : school.graduation_rate_4yr)}%` : 'N/A'}</div>
        </div>
      </div>

      {/* Footer: website + action */}
      <div className="mt-4 flex items-center justify-between">
        {school.website_url ? (
          <div className="flex items-center gap-2">
            <span className="material-symbols-outlined text-on-surface-variant/40 text-sm">link</span>
            <a
              href={school.website_url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-primary/70 hover:text-primary font-bold truncate transition-colors"
            >
              {school.website_url.replace(/^https?:\/\//, '')}
            </a>
          </div>
        ) : <div />}
        {!added && studentId && (
          <button
            onClick={handleAdd}
            disabled={adding}
            className="px-4 py-2 bg-primary/5 text-primary text-xs font-bold rounded-xl border border-primary/15 hover:bg-primary/10 transition-all disabled:opacity-50 flex items-center gap-1.5"
          >
            <span className={`material-symbols-outlined text-sm ${adding ? 'animate-spin' : ''}`}>{adding ? 'progress_activity' : 'add'}</span>
            {adding ? t.disc_evaluating : t.disc_evaluate_add}
          </button>
        )}
      </div>
    </div>
  );
}
