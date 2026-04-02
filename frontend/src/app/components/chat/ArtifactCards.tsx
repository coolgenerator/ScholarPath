import React from 'react';
import ReactMarkdown from 'react-markdown';
import type {
  InfoCardArtifact,
  OfferComparisonArtifact,
  StrategyPlanArtifact,
  WhatIfResultArtifact,
} from '../../lib/types';

function toDisplayValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? String(value) : '—';
  }
  if (typeof value === 'string') {
    return value;
  }
  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No';
  }
  return JSON.stringify(value);
}

function toFiniteNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function formatMoney(value: unknown): string {
  const numeric = toFiniteNumber(value);
  if (numeric == null) {
    return '—';
  }
  return `$${Math.round(numeric).toLocaleString()}`;
}

function formatOutcomeScore(value: unknown): string {
  const numeric = toFiniteNumber(value);
  if (numeric == null) {
    return '—';
  }
  if (numeric >= 0 && numeric <= 1) {
    return `${Math.round(numeric * 100)}%`;
  }
  return numeric.toFixed(2);
}

function formatDelta(value: unknown): string {
  const numeric = toFiniteNumber(value);
  if (numeric == null) {
    return '—';
  }
  const abs = Math.abs(numeric);
  if (abs <= 1.0) {
    return `${numeric >= 0 ? '+' : '-'}${Math.round(abs * 100)}%`;
  }
  return `${numeric >= 0 ? '+' : '-'}${abs.toFixed(2)}`;
}

export function OfferComparisonCard({ artifact }: { artifact: OfferComparisonArtifact }) {
  const rows = artifact.offers ?? [];
  const recommendation = (artifact.recommendation ?? '').trim();
  const recommendedSchools = new Set(
    rows
      .map((row) => String(row.school ?? '').trim())
      .filter((school) => school && recommendation.includes(school)),
  );

  return (
    <div className="mt-3 bg-white rounded-2xl border border-outline-variant/20 p-4 space-y-3">
      <div className="flex items-center gap-2">
        <span className="material-symbols-outlined text-tertiary text-lg">compare_arrows</span>
        <h4 className="font-headline text-xs font-black uppercase tracking-widest text-on-surface">Offer Comparison</h4>
      </div>
      {recommendation && (
        <div className="text-sm text-on-surface/80 leading-relaxed prose prose-sm max-w-none prose-p:my-1 prose-strong:text-on-surface prose-ul:my-1">
          <ReactMarkdown>{recommendation}</ReactMarkdown>
        </div>
      )}
      {rows.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-outline-variant/10">
                <th className="text-left py-2 pr-3 font-bold text-on-surface-variant uppercase tracking-widest">School</th>
                <th className="text-left py-2 pr-3 font-bold text-on-surface-variant uppercase tracking-widest">Net Cost</th>
                <th className="text-left py-2 pr-3 font-bold text-on-surface-variant uppercase tracking-widest">Aid</th>
                <th className="text-left py-2 pr-3 font-bold text-on-surface-variant uppercase tracking-widest">Academic</th>
                <th className="text-left py-2 pr-3 font-bold text-on-surface-variant uppercase tracking-widest">Career</th>
                <th className="text-left py-2 pr-3 font-bold text-on-surface-variant uppercase tracking-widest">Life</th>
                <th className="text-left py-2 pr-3 font-bold text-on-surface-variant uppercase tracking-widest">Deadline</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 8).map((row, idx) => {
                const school = String(row.school ?? '').trim() || '—';
                const causal = (row.causal_scores ?? {}) as Record<string, unknown>;
                const highlight = recommendedSchools.has(school);
                return (
                  <tr
                    key={idx}
                    className={`border-b border-outline-variant/5 last:border-b-0 ${highlight ? 'bg-tertiary/5' : ''}`}
                  >
                    <td className="py-2 pr-3 text-on-surface font-bold">
                      {school}
                      {highlight && (
                        <span className="ml-2 inline-flex items-center rounded-md bg-tertiary/15 px-1.5 py-0.5 text-[10px] font-black text-tertiary">
                          PICK
                        </span>
                      )}
                    </td>
                    <td className="py-2 pr-3 text-on-surface/80">{formatMoney(row.net_cost)}</td>
                    <td className="py-2 pr-3 text-on-surface/80">{formatMoney(row.total_aid)}</td>
                    <td className="py-2 pr-3 text-on-surface/80">{formatOutcomeScore(causal.academic_outcome)}</td>
                    <td className="py-2 pr-3 text-on-surface/80">{formatOutcomeScore(causal.career_outcome)}</td>
                    <td className="py-2 pr-3 text-on-surface/80">{formatOutcomeScore(causal.life_satisfaction)}</td>
                    <td className="py-2 pr-3 text-on-surface/80">{toDisplayValue(row.decision_deadline)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export function WhatIfResultCard({ artifact }: { artifact: WhatIfResultArtifact }) {
  const interventions = Object.entries(artifact.interventions ?? {});
  const deltas = Object.entries(artifact.deltas ?? {}).sort(
    (a, b) => Math.abs(toFiniteNumber(b[1]) ?? 0) - Math.abs(toFiniteNumber(a[1]) ?? 0),
  );

  return (
    <div className="mt-3 bg-white rounded-2xl border border-outline-variant/20 p-4 space-y-3">
      <div className="flex items-center gap-2">
        <span className="material-symbols-outlined text-secondary text-lg">experiment</span>
        <h4 className="font-headline text-xs font-black uppercase tracking-widest text-on-surface">What-if Result</h4>
      </div>
      {artifact.explanation && (
        <div className="text-sm text-on-surface/80 leading-relaxed prose prose-sm max-w-none prose-p:my-1 prose-ul:my-1">
          <ReactMarkdown>{artifact.explanation}</ReactMarkdown>
        </div>
      )}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 text-xs">
        <div className="bg-surface-container-high/30 rounded-xl p-3 border border-outline-variant/10">
          <div className="font-bold text-on-surface-variant uppercase tracking-widest text-[10px] mb-2">Interventions</div>
          {interventions.length === 0 ? (
            <p className="text-on-surface/60">—</p>
          ) : (
            <div className="flex flex-wrap gap-2">
              {interventions.map(([key, value]) => (
                <span
                  key={key}
                  className="inline-flex items-center gap-1 rounded-lg border border-secondary/20 bg-secondary/10 px-2 py-1 text-[11px] font-semibold text-secondary"
                >
                  {key.replace(/_/g, ' ')}
                  <span className="text-secondary/70">{formatOutcomeScore(value)}</span>
                </span>
              ))}
            </div>
          )}
        </div>
        <div className="bg-surface-container-high/30 rounded-xl p-3 border border-outline-variant/10">
          <div className="font-bold text-on-surface-variant uppercase tracking-widest text-[10px] mb-2">Outcome Delta</div>
          {deltas.length === 0 ? (
            <p className="text-on-surface/60">—</p>
          ) : (
            <div className="space-y-2">
              {deltas.map(([key, value]) => {
                const numeric = toFiniteNumber(value) ?? 0;
                const isUp = numeric >= 0;
                return (
                  <div key={key} className="flex items-center justify-between rounded-lg bg-white/70 border border-outline-variant/15 px-2 py-1.5">
                    <span className="text-on-surface/75">{key.replace(/_/g, ' ')}</span>
                    <span className={`inline-flex items-center gap-1 font-bold ${isUp ? 'text-tertiary' : 'text-error'}`}>
                      <span className="material-symbols-outlined text-[14px]">{isUp ? 'north_east' : 'south_east'}</span>
                      {formatDelta(value)}
                    </span>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export function InfoCard({ artifact }: { artifact: InfoCardArtifact }) {
  return (
    <div className="mt-3 bg-white rounded-2xl border border-outline-variant/20 p-4 space-y-2">
      <div className="flex items-center gap-2">
        <span className="material-symbols-outlined text-primary text-lg">info</span>
        <h4 className="font-headline text-sm font-black text-on-surface">{artifact.title}</h4>
      </div>
      <p className="text-sm text-on-surface/80">{artifact.summary}</p>
      {Object.keys(artifact.data || {}).length > 0 && (
        <pre className="text-xs bg-surface-container-high/30 rounded-xl p-3 overflow-x-auto whitespace-pre-wrap text-on-surface/80">
          {JSON.stringify(artifact.data, null, 2)}
        </pre>
      )}
    </div>
  );
}

export function StrategyPlanCard({ artifact }: { artifact: StrategyPlanArtifact }) {
  return (
    <div className="mt-3 bg-white rounded-2xl border border-outline-variant/20 p-4 space-y-3">
      <div className="flex items-center gap-2">
        <span className="material-symbols-outlined text-primary text-lg">strategy</span>
        <h4 className="font-headline text-xs font-black uppercase tracking-widest text-on-surface">Strategy Plan</h4>
      </div>
      <pre className="text-xs bg-surface-container-high/30 rounded-xl p-3 overflow-x-auto whitespace-pre-wrap text-on-surface/80">
        {JSON.stringify(artifact.strategy, null, 2)}
      </pre>
    </div>
  );
}
