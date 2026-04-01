import React from 'react';
import ReactMarkdown from 'react-markdown';
import type { RecommendationData, RecommendedSchool } from '../../lib/types';

interface Props {
  data: RecommendationData;
}

const TIER_CONFIG = {
  reach: {
    label: 'Reach',
    labelCn: '冲刺',
    icon: 'rocket_launch',
    bgClass: 'bg-red-50',
    headerBg: 'bg-red-100/80',
    headerText: 'text-red-800',
    barColor: 'bg-red-400',
    barHex: '#f87171',
    badgeBg: 'bg-red-500',
    borderColor: 'border-red-200/60',
    pillPositive: 'bg-red-50 text-red-700 border-red-200',
    pillNegative: 'bg-red-50/50 text-red-500 border-red-100',
  },
  target: {
    label: 'Target',
    labelCn: '匹配',
    icon: 'target',
    bgClass: 'bg-emerald-50',
    headerBg: 'bg-emerald-100/80',
    headerText: 'text-emerald-800',
    barColor: 'bg-emerald-400',
    barHex: '#34d399',
    badgeBg: 'bg-emerald-500',
    borderColor: 'border-emerald-200/60',
    pillPositive: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    pillNegative: 'bg-emerald-50/50 text-emerald-500 border-emerald-100',
  },
  safety: {
    label: 'Safety',
    labelCn: '保底',
    icon: 'shield',
    bgClass: 'bg-blue-50',
    headerBg: 'bg-blue-100/80',
    headerText: 'text-blue-800',
    barColor: 'bg-blue-400',
    barHex: '#60a5fa',
    badgeBg: 'bg-blue-500',
    borderColor: 'border-blue-200/60',
    pillPositive: 'bg-blue-50 text-blue-700 border-blue-200',
    pillNegative: 'bg-blue-50/50 text-blue-500 border-blue-100',
  },
  likely: {
    label: 'Likely',
    labelCn: '很有把握',
    icon: 'check_circle',
    bgClass: 'bg-gray-50',
    headerBg: 'bg-gray-100/80',
    headerText: 'text-gray-700',
    barColor: 'bg-gray-400',
    barHex: '#9ca3af',
    badgeBg: 'bg-gray-500',
    borderColor: 'border-gray-200/60',
    pillPositive: 'bg-gray-50 text-gray-700 border-gray-200',
    pillNegative: 'bg-gray-50/50 text-gray-500 border-gray-100',
  },
} as const;

const SUB_SCORE_LABELS: Record<string, { label: string; icon: string }> = {
  academic: { label: 'Academic', icon: 'school' },
  financial: { label: 'Financial', icon: 'payments' },
  career: { label: 'Career', icon: 'work' },
  life: { label: 'Life', icon: 'favorite' },
};

function formatPrice(price: number): string {
  if (price >= 1000) {
    return `$${Math.round(price / 1000)}K`;
  }
  return `$${price.toLocaleString()}`;
}

function formatPercent(value: number): string {
  return `${Math.round(value * 100)}%`;
}

function SchoolCard({ school, tierKey }: { school: RecommendedSchool; tierKey: keyof typeof TIER_CONFIG }) {
  const config = TIER_CONFIG[tierKey];

  return (
    <div className={`rounded-2xl border ${config.borderColor} bg-white shadow-sm hover:shadow-md transition-shadow duration-200 overflow-hidden`}>
      {/* Header with name and rank */}
      <div className="p-4 pb-3">
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0 flex-1">
            <h4 className="font-headline text-sm font-bold text-on-surface truncate">{school.school_name}</h4>
            {school.school_name_cn && (
              <p className="text-[11px] text-on-surface-variant/60 mt-0.5 truncate">{school.school_name_cn}</p>
            )}
          </div>
          {school.rank && (
            <span className={`${config.badgeBg} text-white text-[10px] font-bold px-2 py-0.5 rounded-lg flex-shrink-0`}>
              #{school.rank}
            </span>
          )}
        </div>

        {/* Overall score bar */}
        <div className="mt-3">
          <div className="flex items-center justify-between mb-1">
            <span className="text-[10px] font-bold text-on-surface-variant/50 uppercase tracking-wider">Score</span>
            <span className="text-xs font-bold text-on-surface">{formatPercent(school.overall_score)}</span>
          </div>
          <div className="h-2 bg-outline-variant/10 rounded-full overflow-hidden">
            <div
              className={`h-full ${config.barColor} rounded-full transition-all duration-500`}
              style={{ width: `${school.overall_score * 100}%` }}
            />
          </div>
        </div>

        {/* Key stats */}
        <div className="mt-3 grid grid-cols-2 gap-2">
          <div className="text-center p-1.5 bg-surface-container-highest/30 rounded-lg">
            <div className="text-[10px] text-on-surface-variant/50 font-bold uppercase tracking-wider">Admit</div>
            <div className="text-sm font-bold text-on-surface mt-0.5">{formatPercent(school.admission_probability)}</div>
          </div>
          {school.net_price != null ? (
            <div className="text-center p-1.5 bg-surface-container-highest/30 rounded-lg">
              <div className="text-[10px] text-on-surface-variant/50 font-bold uppercase tracking-wider">Net $</div>
              <div className="text-sm font-bold text-on-surface mt-0.5">{formatPrice(school.net_price)}</div>
            </div>
          ) : school.acceptance_rate != null ? (
            <div className="text-center p-1.5 bg-surface-container-highest/30 rounded-lg">
              <div className="text-[10px] text-on-surface-variant/50 font-bold uppercase tracking-wider">Acc. Rate</div>
              <div className="text-sm font-bold text-on-surface mt-0.5">{formatPercent(school.acceptance_rate)}</div>
            </div>
          ) : (
            <div />
          )}
        </div>

        {/* Key reasons pills */}
        {school.key_reasons.length > 0 && (
          <div className="mt-3 flex flex-col gap-1">
            {school.key_reasons.slice(0, 2).map((reason, idx) => {
              const isPositive = reason.startsWith('+') || !reason.startsWith('-');
              const displayReason = reason.replace(/^[+-]\s*/, '');
              return (
                <span
                  key={idx}
                  className={`inline-flex items-center gap-1 px-2 py-0.5 text-[10px] font-bold rounded-lg border truncate ${
                    isPositive ? config.pillPositive : config.pillNegative
                  }`}
                >
                  <span className="material-symbols-outlined text-[12px]" style={{ fontVariationSettings: "'FILL' 1" }}>
                    {isPositive ? 'add_circle' : 'remove_circle'}
                  </span>
                  <span className="truncate">{displayReason}</span>
                </span>
              );
            })}
          </div>
        )}

        {/* Sub-scores */}
        {Object.keys(school.sub_scores).length > 0 && (
          <div className="mt-3 space-y-1.5">
            {Object.entries(SUB_SCORE_LABELS).map(([key, meta]) => {
              const value = school.sub_scores[key];
              if (value == null) return null;
              return (
                <div key={key} className="flex items-center gap-2">
                  <span className="material-symbols-outlined text-[12px] text-on-surface-variant/40" style={{ fontVariationSettings: "'FILL' 1" }}>
                    {meta.icon}
                  </span>
                  <span className="text-[9px] font-bold text-on-surface-variant/50 w-[46px] uppercase tracking-wider">{meta.label}</span>
                  <div className="flex-1 h-1.5 bg-outline-variant/10 rounded-full overflow-hidden">
                    <div
                      className="h-full rounded-full transition-all duration-500"
                      style={{ width: `${value * 100}%`, backgroundColor: config.barHex, opacity: 0.7 }}
                    />
                  </div>
                  <span className="text-[9px] font-bold text-on-surface-variant/60 w-[24px] text-right">{formatPercent(value)}</span>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

function TierSection({ tierKey, schools }: { tierKey: keyof typeof TIER_CONFIG; schools: RecommendedSchool[] }) {
  if (schools.length === 0) return null;
  const config = TIER_CONFIG[tierKey];

  return (
    <div className="mt-4 first:mt-0">
      {/* Tier header */}
      <div className={`flex items-center gap-2 px-4 py-2.5 ${config.headerBg} rounded-xl mb-3`}>
        <span
          className={`material-symbols-outlined text-[18px] ${config.headerText}`}
          style={{ fontVariationSettings: "'FILL' 1" }}
        >
          {config.icon}
        </span>
        <span className={`font-headline text-xs font-extrabold ${config.headerText} uppercase tracking-wider`}>
          {config.label}
        </span>
        <span className={`text-[10px] font-bold ${config.headerText}/60`}>
          {config.labelCn}
        </span>
        <span className={`ml-auto text-[10px] font-bold ${config.headerText}/50`}>
          {schools.length} {schools.length === 1 ? 'school' : 'schools'}
        </span>
      </div>

      {/* Horizontally scrollable school cards */}
      <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3 pb-2">
        {schools.map((school, idx) => (
          <SchoolCard key={idx} school={school} tierKey={tierKey} />
        ))}
      </div>
    </div>
  );
}

export function RecommendationCard({ data }: Props) {
  const tiersOrder: (keyof typeof TIER_CONFIG)[] = ['reach', 'target', 'safety', 'likely'];

  const schoolsByTier = tiersOrder.reduce(
    (acc, tier) => {
      acc[tier] = data.schools.filter((s) => s.tier === tier);
      return acc;
    },
    {} as Record<string, RecommendedSchool[]>,
  );

  return (
    <div className="mt-3 bg-white rounded-3xl border border-outline-variant/20 shadow-lg shadow-primary/5 overflow-hidden transition-all duration-300">
      {/* Narrative */}
      {data.narrative && (
        <div className="px-5 pt-5 pb-3">
          <div className="flex items-start gap-3">
            <span
              className="material-symbols-outlined text-primary text-[20px] mt-0.5 flex-shrink-0"
              style={{ fontVariationSettings: "'FILL' 1" }}
            >
              auto_awesome
            </span>
            <div className="text-sm text-on-surface/80 leading-relaxed prose prose-sm max-w-none prose-p:my-1 prose-strong:text-on-surface prose-strong:font-bold">
              <ReactMarkdown>{data.narrative}</ReactMarkdown>
            </div>
          </div>
        </div>
      )}

      {/* Divider */}
      <div className="mx-5 border-t border-outline-variant/10" />

      {/* Tier sections */}
      <div className="p-5 space-y-5">
        {tiersOrder.map((tier) => (
          <TierSection key={tier} tierKey={tier} schools={schoolsByTier[tier]} />
        ))}
      </div>

      {/* Strategy section */}
      {(data.ed_recommendation || data.ea_recommendations.length > 0 || data.strategy_summary) && (
        <>
          <div className="mx-5 border-t border-outline-variant/10" />
          <div className="px-5 py-4">
            <div className="flex items-center gap-2 mb-3">
              <span
                className="material-symbols-outlined text-tertiary text-[18px]"
                style={{ fontVariationSettings: "'FILL' 1" }}
              >
                strategy
              </span>
              <span className="font-headline text-xs font-extrabold text-on-surface uppercase tracking-wider">
                Application Strategy
              </span>
            </div>

            <div className="flex flex-wrap gap-2">
              {data.ed_recommendation && (
                <div className="flex items-center gap-1.5">
                  <span className="px-2 py-1 text-[10px] font-extrabold text-white bg-red-500 rounded-lg uppercase tracking-wider shadow-sm">
                    ED
                  </span>
                  <span className="text-xs font-bold text-on-surface">{data.ed_recommendation}</span>
                </div>
              )}

              {data.ea_recommendations.length > 0 && (
                <div className="flex items-center gap-1.5 flex-wrap">
                  <span className="px-2 py-1 text-[10px] font-extrabold text-white bg-emerald-500 rounded-lg uppercase tracking-wider shadow-sm">
                    EA
                  </span>
                  {data.ea_recommendations.map((school, idx) => (
                    <span
                      key={idx}
                      className="text-xs font-bold text-on-surface bg-emerald-50 border border-emerald-200 px-2 py-0.5 rounded-lg"
                    >
                      {school}
                    </span>
                  ))}
                </div>
              )}
            </div>

            {data.strategy_summary && (
              <p className="text-[11px] text-on-surface-variant/60 mt-2 leading-relaxed">{data.strategy_summary}</p>
            )}
          </div>
        </>
      )}
    </div>
  );
}
