import React, { useEffect, useState, useMemo, useCallback, useSyncExternalStore } from 'react';
import type { ClaimsGraphResponse, ClaimItem, Controversy } from '../../lib/types';
import { fetchClaimsGraph, getSchoolDataState, subscribe } from '../../lib/schoolDataCache';

// ---- Topic metadata ----

const TOPIC_META: Record<string, { icon: string; label: string; label_cn: string }> = {
  academic:    { icon: 'school',    label: 'Academic',    label_cn: '学术体验' },
  campus_life: { icon: 'apartment', label: 'Campus Life', label_cn: '校园生活' },
  career:      { icon: 'work',      label: 'Career',      label_cn: '就业前景' },
  financial:   { icon: 'savings',   label: 'Financial',   label_cn: '性价比' },
  vibe:        { icon: 'mood',      label: 'Vibe',        label_cn: '整体氛围' },
};

const SENTIMENT_ICON: Record<string, string> = {
  positive: '\uD83D\uDC4D',
  negative: '\uD83D\uDC4E',
  neutral: '\u2796',
};

const SENTIMENT_COLORS: Record<string, { bg: string; text: string; bar: string }> = {
  positive: { bg: 'bg-green-50',  text: 'text-green-700', bar: 'bg-green-500' },
  negative: { bg: 'bg-red-50',    text: 'text-red-700',   bar: 'bg-red-500' },
  neutral:  { bg: 'bg-slate-50',  text: 'text-slate-600', bar: 'bg-slate-400' },
};

const SEVERITY_BADGE: Record<string, { bg: string; text: string }> = {
  low:    { bg: 'bg-blue-100',   text: 'text-blue-700' },
  medium: { bg: 'bg-amber-100',  text: 'text-amber-700' },
  high:   { bg: 'bg-red-100',    text: 'text-red-700' },
};

// ---- Sub-components ----

function StrengthBar({ value }: { value: number }) {
  return (
    <div className="flex gap-0.5 items-center">
      {Array.from({ length: 10 }).map((_, i) => (
        <div
          key={i}
          className={`h-2 w-1.5 rounded-full transition-colors ${
            i < value ? 'bg-primary' : 'bg-outline-variant/15'
          }`}
        />
      ))}
      <span className="ml-1.5 text-[11px] font-black text-on-surface">{value}</span>
    </div>
  );
}

function ClaimRow({ claim }: { claim: ClaimItem }) {
  const [expanded, setExpanded] = useState(false);
  const sentiment = claim.sentiment || 'neutral';
  const colors = SENTIMENT_COLORS[sentiment] || SENTIMENT_COLORS.neutral;

  return (
    <div className="border-b border-outline-variant/8 last:border-b-0">
      <button
        className="flex w-full items-start gap-3 px-1 py-3 text-left transition hover:bg-surface-container-high/20"
        onClick={() => setExpanded(!expanded)}
      >
        <span className="text-base mt-0.5 shrink-0">{SENTIMENT_ICON[sentiment] || SENTIMENT_ICON.neutral}</span>
        <div className="flex-1 min-w-0">
          <p className="text-xs font-bold text-on-surface leading-relaxed">{claim.text_cn || claim.text}</p>
          {claim.text_cn && (
            <p className="text-[11px] text-on-surface-variant/60 mt-0.5 leading-relaxed">{claim.text}</p>
          )}
        </div>
        <div className="flex flex-col items-end gap-1.5 shrink-0">
          <StrengthBar value={claim.strength} />
          <div className="flex items-center gap-1.5">
            <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold uppercase ${colors.bg} ${colors.text}`}>
              {sentiment}
            </span>
            <span className="text-[10px] text-on-surface-variant/50 font-bold">{claim.source_count} src</span>
          </div>
        </div>
        <span
          className="material-symbols-outlined text-sm text-on-surface-variant/40 mt-1 transition-transform shrink-0"
          style={{ transform: expanded ? 'rotate(180deg)' : '' }}
        >
          expand_more
        </span>
      </button>
      {expanded && claim.evidence.length > 0 && (
        <div className="pb-3 pl-8 pr-2 space-y-2">
          {claim.evidence.map((ev, i) => {
            const isStructured = typeof ev === 'object' && ev !== null;
            const quote = isStructured ? (ev as any).quote : String(ev);
            const url = isStructured ? (ev as any).url : null;
            return (
              <div key={i} className="space-y-0.5">
                <div className="flex gap-2 text-[11px] text-on-surface-variant/55 italic">
                  <span className="text-primary/40 shrink-0">&ldquo;</span>
                  <span className="line-clamp-3">{quote}</span>
                </div>
                {url && (
                  <a
                    href={url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="ml-4 inline-flex items-center gap-1 text-[10px] text-primary/50 hover:text-primary transition"
                  >
                    <span className="material-symbols-outlined text-[10px]">open_in_new</span>
                    来源链接
                  </a>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function ControversyRow({ cont, claimsMap }: { cont: Controversy; claimsMap: Map<string, ClaimItem> }) {
  const claimA = claimsMap.get(cont.claim_a);
  const claimB = claimsMap.get(cont.claim_b);
  const severity = cont.severity || 'medium';
  const badge = SEVERITY_BADGE[severity] || SEVERITY_BADGE.medium;

  return (
    <div className="rounded-xl border border-outline-variant/10 bg-white p-4 space-y-3">
      <div className="flex items-center gap-2">
        <span className="material-symbols-outlined text-amber-500 text-lg">bolt</span>
        <span className="text-xs font-bold text-on-surface flex-1">{cont.aspect}</span>
        <span className={`px-2 py-0.5 rounded-full text-[9px] font-bold uppercase ${badge.bg} ${badge.text}`}>
          {severity}
        </span>
      </div>

      <div className="grid gap-3 sm:grid-cols-2">
        {/* Claim A */}
        <div className="rounded-lg bg-green-50/50 border border-green-200/30 p-3">
          <div className="flex items-center gap-1.5 mb-1.5">
            <span className="text-sm">{SENTIMENT_ICON.positive}</span>
            <span className="text-[9px] font-bold text-green-700 uppercase">
              {claimA ? `${claimA.source_count} sources` : ''}
            </span>
          </div>
          <p className="text-xs text-on-surface leading-relaxed">
            {claimA?.text_cn || claimA?.text || cont.claim_a}
          </p>
        </div>
        {/* Claim B */}
        <div className="rounded-lg bg-red-50/50 border border-red-200/30 p-3">
          <div className="flex items-center gap-1.5 mb-1.5">
            <span className="text-sm">{SENTIMENT_ICON.negative}</span>
            <span className="text-[9px] font-bold text-red-700 uppercase">
              {claimB ? `${claimB.source_count} sources` : ''}
            </span>
          </div>
          <p className="text-xs text-on-surface leading-relaxed">
            {claimB?.text_cn || claimB?.text || cont.claim_b}
          </p>
        </div>
      </div>

      {cont.analysis && (
        <p className="text-[11px] text-on-surface-variant/65 leading-relaxed border-l-2 border-amber-300/40 pl-3">
          {cont.analysis}
        </p>
      )}
    </div>
  );
}

// ---- Main component ----

interface ClaimsGraphCardProps {
  schoolId: string;
  /** Optional topic filter — only show claims matching these topics */
  relevantTopics?: string[];
}

export function ClaimsGraphCard({ schoolId, relevantTopics }: ClaimsGraphCardProps) {
  const [activeTab, setActiveTab] = useState<'claims' | 'controversies'>('claims');

  const subFn = useCallback((cb: () => void) => subscribe(schoolId, cb), [schoolId]);
  const state = useSyncExternalStore(subFn, () => getSchoolDataState(schoolId).claimsGraph);

  useEffect(() => { fetchClaimsGraph(schoolId); }, [schoolId]);

  const { status, progress, data } = state;

  if (status === 'idle') return null;

  if (status === 'loading') {
    return (
      <div className="rounded-2xl border border-outline-variant/10 bg-white p-5 shadow-sm">
        <div className="flex items-center gap-2 mb-3">
          <span className="material-symbols-outlined animate-spin text-primary text-lg">progress_activity</span>
          <span className="text-xs font-bold text-on-surface-variant/60">{progress}</span>
        </div>
        <div className="w-full h-1.5 bg-surface-container-high/40 rounded-full overflow-hidden">
          <div className="h-full bg-primary/60 rounded-full" style={{ width: '40%', animation: 'indeterminate 1.5s ease-in-out infinite' }} />
        </div>
        <style>{`@keyframes indeterminate { 0% { margin-left: 0; width: 30%; } 50% { margin-left: 30%; width: 50%; } 100% { margin-left: 70%; width: 30%; } }`}</style>
      </div>
    );
  }

  if (status === 'error' || !data) return null;

  const allClaims = data.claims || [];
  const claims = relevantTopics?.length
    ? allClaims.filter((c) => relevantTopics.includes(c.topic))
    : allClaims;
  const claimIds = new Set(claims.map((c) => c.id));
  const controversies = (data.controversies || []).filter(
    (ct) => claimIds.has(ct.claim_a) || claimIds.has(ct.claim_b),
  );
  const claimsMap = new Map(allClaims.map((c) => [c.id, c]));

  // Group claims by topic
  const grouped: Record<string, ClaimItem[]> = {};
  for (const claim of claims) {
    const topic = claim.topic || 'vibe';
    (grouped[topic] ??= []).push(claim);
  }

  return (
    <div className="rounded-2xl border border-outline-variant/10 bg-white p-5 shadow-sm">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <span className="material-symbols-outlined text-primary text-lg">hub</span>
          <h4 className="font-headline text-sm font-black text-on-surface">观点图谱</h4>
          <span className="text-[10px] font-bold text-on-surface-variant/50">
            {claims.length} claims / {controversies.length} controversies
          </span>
        </div>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 mb-4 p-0.5 bg-surface-container-high/30 rounded-xl">
        <button
          onClick={() => setActiveTab('claims')}
          className={`flex-1 py-2 text-xs font-bold rounded-lg transition-all ${
            activeTab === 'claims'
              ? 'bg-white text-on-surface shadow-sm'
              : 'text-on-surface-variant/60 hover:text-on-surface-variant'
          }`}
        >
          <span className="material-symbols-outlined text-sm mr-1 align-middle">list_alt</span>
          核心观点
        </button>
        <button
          onClick={() => setActiveTab('controversies')}
          className={`flex-1 py-2 text-xs font-bold rounded-lg transition-all ${
            activeTab === 'controversies'
              ? 'bg-white text-on-surface shadow-sm'
              : 'text-on-surface-variant/60 hover:text-on-surface-variant'
          }`}
        >
          <span className="material-symbols-outlined text-sm mr-1 align-middle">bolt</span>
          争议焦点
          {controversies.length > 0 && (
            <span className="ml-1 px-1.5 py-0.5 bg-amber-100 text-amber-700 text-[9px] font-bold rounded-full">
              {controversies.length}
            </span>
          )}
        </button>
      </div>

      {/* Claims tab */}
      {activeTab === 'claims' && (
        <div className="space-y-4">
          {Object.entries(TOPIC_META).map(([topicKey, meta]) => {
            const topicClaims = grouped[topicKey];
            if (!topicClaims || topicClaims.length === 0) return null;
            return (
              <div key={topicKey}>
                <div className="flex items-center gap-2 mb-2 px-1">
                  <span className="material-symbols-outlined text-on-surface-variant/50 text-base">{meta.icon}</span>
                  <span className="text-[10px] font-bold text-on-surface-variant/70 uppercase tracking-widest">
                    {meta.label_cn}
                  </span>
                  <span className="text-[9px] text-on-surface-variant/40 font-bold">{topicClaims.length}</span>
                </div>
                <div className="bg-surface-container-lowest/50 rounded-xl border border-outline-variant/5 px-3">
                  {topicClaims.map((claim) => (
                    <ClaimRow key={claim.id} claim={claim} />
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Controversies tab */}
      {activeTab === 'controversies' && (
        <div className="space-y-3">
          {controversies.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-8 text-center">
              <span className="material-symbols-outlined text-on-surface-variant/20 text-3xl mb-2">check_circle</span>
              <p className="text-xs text-on-surface-variant/50">No significant controversies found</p>
            </div>
          ) : (
            controversies.map((cont, i) => (
              <ControversyRow key={i} cont={cont} claimsMap={claimsMap} />
            ))
          )}
        </div>
      )}
    </div>
  );
}
