import React, { useMemo, useState } from 'react';
import { useApp } from '../../context/AppContext';
import type { RecommendationData, RecommendedSchool } from '../../lib/types';
import { ExpandableMarkdown } from './ExpandableMarkdown';
import {
  StructuredCardHeader,
  StructuredCardSection,
  StructuredCardShell,
} from './StructuredCardPrimitives';

interface Props {
  data: RecommendationData;
}

const TIER_CONFIG = {
  reach: {
    label: { en: 'Reach', zh: '冲刺' },
    icon: 'rocket_launch',
    accentClass: 'structured-tier-pill-reach',
    barClass: 'bg-rose-500',
  },
  target: {
    label: { en: 'Target', zh: '匹配' },
    icon: 'target',
    accentClass: 'structured-tier-pill-target',
    barClass: 'bg-emerald-500',
  },
  safety: {
    label: { en: 'Safety', zh: '保底' },
    icon: 'shield',
    accentClass: 'structured-tier-pill-safety',
    barClass: 'bg-sky-500',
  },
  likely: {
    label: { en: 'Likely', zh: '稳妥' },
    icon: 'verified',
    accentClass: 'structured-tier-pill-likely',
    barClass: 'bg-slate-500',
  },
} as const;

const SCORE_META = {
  academic: { en: 'Academic', zh: '学术', icon: 'school' },
  financial: { en: 'Financial', zh: '财务', icon: 'payments' },
  career: { en: 'Career', zh: '职业', icon: 'work' },
  life: { en: 'Life', zh: '生活', icon: 'favorite' },
} as const;

function formatPrice(price: number | null | undefined): string {
  if (price == null) return '—';
  if (price >= 1000) return `$${Math.round(price / 1000)}K`;
  return `$${price.toLocaleString()}`;
}

function formatPercent(value: number | null | undefined): string {
  if (value == null) return '—';
  return `${Math.round(value * 100)}%`;
}

function SchoolEditorialRow({
  school,
  isCn,
}: {
  school: RecommendedSchool;
  isCn: boolean;
}) {
  const tier = TIER_CONFIG[school.tier as keyof typeof TIER_CONFIG] ?? TIER_CONFIG.target;
  const topReasons = school.key_reasons
    .slice(0, 3)
    .map((reason) => reason.replace(/^[+-]\s*/, '').trim())
    .filter(Boolean);

  return (
    <article className="structured-school-row">
      <div className="structured-school-row-copy">
        <div className="structured-school-row-heading">
          <span className={`structured-tier-pill ${tier.accentClass}`}>
            <span className="material-symbols-outlined text-[14px]" style={{ fontVariationSettings: "'FILL' 1" }}>
              {tier.icon}
            </span>
            {isCn ? tier.label.zh : tier.label.en}
          </span>
          {school.prefilter_tag === 'eligible' ? (
            <span className="rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-bold text-emerald-700">
              {isCn ? '预算内' : 'In Budget'}
            </span>
          ) : null}
          {school.prefilter_tag === 'stretch' ? (
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-bold text-amber-700">
              {isCn ? '冲刺位' : 'Stretch'}
            </span>
          ) : null}
          {typeof school.rank_delta === 'number' && school.rank_delta !== 0 ? (
            <span className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${school.rank_delta > 0 ? 'bg-sky-100 text-sky-700' : 'bg-rose-100 text-rose-700'}`}>
              {school.rank_delta > 0 ? (isCn ? `↑${school.rank_delta}` : `+${school.rank_delta}`) : (isCn ? `${school.rank_delta}` : `${school.rank_delta}`)}
            </span>
          ) : null}
          {school.rank != null ? (
            <span className="structured-school-row-rank">#{school.rank}</span>
          ) : null}
        </div>

        <div>
          <h4 className="structured-school-row-title">{school.school_name}</h4>
          {school.school_name_cn ? (
            <p className="structured-school-row-subtitle">{school.school_name_cn}</p>
          ) : null}
        </div>

        {topReasons.length > 0 ? (
          <div className="structured-inline-reasons">
            {topReasons.map((reason) => (
              <span key={reason} className="structured-reason-pill">
                {reason}
              </span>
            ))}
          </div>
        ) : null}

        {Object.keys(school.sub_scores ?? {}).length > 0 ? (
          <div className="structured-score-strip">
            {Object.entries(SCORE_META).map(([key, meta]) => {
              const score = school.sub_scores?.[key];
              if (score == null) return null;
              return (
                <div key={key} className="structured-score-chip">
                  <div className="structured-score-chip-head">
                    <span className="material-symbols-outlined text-[14px] text-on-surface-variant/55">{meta.icon}</span>
                    <span>{isCn ? meta.zh : meta.en}</span>
                    <span className="structured-score-chip-value">{formatPercent(score)}</span>
                  </div>
                  <div className="structured-score-bar">
                    <div className={tier.barClass} style={{ width: `${score * 100}%` }} />
                  </div>
                </div>
              );
            })}
          </div>
        ) : null}
      </div>

      <div className="structured-school-row-meta">
        <div className="structured-school-kpi">
          <div className="structured-school-kpi-label">{isCn ? '综合分' : 'Score'}</div>
          <div className="structured-school-kpi-value">{formatPercent(school.overall_score)}</div>
        </div>
        <div className="structured-school-microgrid">
          <div className="structured-school-microstat">
            <div className="structured-school-microstat-label">{isCn ? '录取概率' : 'Admit'}</div>
            <div className="structured-school-microstat-value">{formatPercent(school.admission_probability)}</div>
          </div>
          <div className="structured-school-microstat">
            <div className="structured-school-microstat-label">
              {school.net_price != null ? (isCn ? '净价' : 'Net Price') : (isCn ? '录取率' : 'Acceptance')}
            </div>
            <div className="structured-school-microstat-value">
              {school.net_price != null ? formatPrice(school.net_price) : formatPercent(school.acceptance_rate)}
            </div>
          </div>
        </div>
      </div>
    </article>
  );
}

function TierCluster({
  tierKey,
  schools,
  isCn,
}: {
  tierKey: keyof typeof TIER_CONFIG;
  schools: RecommendedSchool[];
  isCn: boolean;
}) {
  const tier = TIER_CONFIG[tierKey];
  if (schools.length === 0) return null;

  return (
    <section className="space-y-3">
      <div className="structured-tier-heading">
        <div className="flex items-center gap-2">
          <span className={`structured-tier-pill ${tier.accentClass}`}>
            <span className="material-symbols-outlined text-[14px]" style={{ fontVariationSettings: "'FILL' 1" }}>
              {tier.icon}
            </span>
            {isCn ? tier.label.zh : tier.label.en}
          </span>
          <span className="structured-tier-heading-meta">
            {schools.length} {isCn ? '所学校' : schools.length === 1 ? 'school' : 'schools'}
          </span>
        </div>
      </div>
      <div className="structured-editorial-list">
        {schools.map((school) => (
          <SchoolEditorialRow key={`${tierKey}-${school.school_name}`} school={school} isCn={isCn} />
        ))}
      </div>
    </section>
  );
}

export function RecommendationCard({ data }: Props) {
  const { locale } = useApp();
  const isCn = locale === 'zh';
  const tierOrder: (keyof typeof TIER_CONFIG)[] = ['reach', 'target', 'safety', 'likely'];
  const scenarioTabs = useMemo(() => {
    const tabs: Array<{ id: string; label: string; schools: RecommendedSchool[] }> = [
      {
        id: 'baseline',
        label: isCn ? '基线' : 'Baseline',
        schools: data.scenario_pack?.baseline?.length ? data.scenario_pack.baseline : data.schools,
      },
    ];
    for (const item of data.scenario_pack?.scenarios ?? []) {
      tabs.push({
        id: item.id,
        label: item.label || item.id,
        schools: item.schools,
      });
    }
    return tabs;
  }, [data.scenario_pack, data.schools, isCn]);
  const [activeScenarioId, setActiveScenarioId] = useState<string>('baseline');
  const activeTab = scenarioTabs.find((item) => item.id === activeScenarioId) ?? scenarioTabs[0];
  const activeSchools = activeTab?.schools ?? data.schools;

  const schoolsByTier = tierOrder.reduce(
    (acc, tier) => {
      acc[tier] = activeSchools.filter((school) => school.tier === tier);
      return acc;
    },
    {} as Record<keyof typeof TIER_CONFIG, RecommendedSchool[]>,
  );

  const summaryStats = tierOrder.map((tier) => ({
    label: isCn ? TIER_CONFIG[tier].label.zh : TIER_CONFIG[tier].label.en,
    value: schoolsByTier[tier].length,
  }));

  return (
    <StructuredCardShell className="mt-3 overflow-hidden">
      <StructuredCardHeader
        kicker={isCn ? '顾问建议' : 'Advisor Suggestions'}
        title={isCn ? '选校建议' : 'School Recommendations'}
        description={isCn ? '把推荐结论、学校分层和申请动作收进一张更易扫读的建议卡。' : 'A tighter editorial view of the recommendation narrative, school tiers, and application moves.'}
        badge={`${data.schools.length} ${isCn ? '所学校' : 'schools'}`}
      />

      {data.prefilter_meta?.budget_cap_used ? (
        <StructuredCardSection className="pt-0" title={isCn ? '预算门槛' : 'Budget Gate'}>
          <div className="flex flex-wrap items-center gap-2 text-xs text-on-surface-variant/75">
            <span className="rounded-full bg-primary/8 px-2.5 py-1 font-semibold text-primary">
              {isCn ? `预算上限 $${Number(data.prefilter_meta.budget_cap_used).toLocaleString()}` : `Budget cap $${Number(data.prefilter_meta.budget_cap_used).toLocaleString()}`}
            </span>
            <span>{isCn ? `预算内 ${data.prefilter_meta.eligible_count ?? 0}` : `Eligible ${data.prefilter_meta.eligible_count ?? 0}`}</span>
            <span>{isCn ? `冲刺 ${data.prefilter_meta.stretch_count ?? 0}` : `Stretch ${data.prefilter_meta.stretch_count ?? 0}`}</span>
            <span>{isCn ? `剔除 ${data.prefilter_meta.excluded_count ?? 0}` : `Excluded ${data.prefilter_meta.excluded_count ?? 0}`}</span>
          </div>
        </StructuredCardSection>
      ) : null}

      {scenarioTabs.length > 1 ? (
        <StructuredCardSection className="pt-0" title={isCn ? '多场景视图' : 'Scenario Views'}>
          <div className="flex flex-wrap gap-2">
            {scenarioTabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveScenarioId(tab.id)}
                className={`rounded-full px-3 py-1 text-xs font-bold transition-colors ${tab.id === activeTab.id ? 'bg-primary text-on-primary' : 'bg-surface-container-high/50 text-on-surface-variant hover:bg-surface-container-high'}`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </StructuredCardSection>
      ) : null}

      <StructuredCardSection
        className="structured-summary-section"
        title={isCn ? '核心判断' : 'Topline Read'}
      >
        <div className="structured-summary-grid">
          <div className="structured-summary-panel">
            {data.narrative ? (
              <ExpandableMarkdown className="structured-card-markdown" content={data.narrative} />
            ) : (
              <p className="structured-card-description">
                {isCn ? '根据当前档案与偏好，系统已整理出最值得优先看的学校组合。' : 'The current profile and preferences point to this school mix as the most actionable shortlist.'}
              </p>
            )}
          </div>

          <div className="structured-summary-panel structured-summary-panel-secondary">
            <div className="structured-summary-stat-grid">
              {summaryStats.map((stat) => (
                <div key={stat.label} className="structured-summary-stat">
                  <div className="structured-summary-stat-label">{stat.label}</div>
                  <div className="structured-summary-stat-value">{stat.value}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </StructuredCardSection>

      <div className="space-y-6 px-4 pb-4 sm:px-5 sm:pb-5">
        {tierOrder.map((tier) => (
          <TierCluster key={tier} tierKey={tier} schools={schoolsByTier[tier]} isCn={isCn} />
        ))}
      </div>

      {(data.ed_recommendation || data.ea_recommendations.length > 0 || data.strategy_summary) ? (
        <StructuredCardSection
          className="structured-card-footer"
          title={isCn ? '申请策略' : 'Application Strategy'}
        >
          <div className="structured-strategy-list">
            {data.ed_recommendation ? (
              <span className="structured-strategy-pill structured-strategy-pill-ed">
                <span className="structured-strategy-pill-label">ED</span>
                <span>{data.ed_recommendation}</span>
              </span>
            ) : null}
            {data.ea_recommendations.map((school) => (
              <span key={school} className="structured-strategy-pill structured-strategy-pill-ea">
                <span className="structured-strategy-pill-label">EA</span>
                <span>{school}</span>
              </span>
            ))}
          </div>
          {data.strategy_summary ? (
            <p className="mt-3 text-sm leading-relaxed text-on-surface-variant/72">{data.strategy_summary}</p>
          ) : null}
        </StructuredCardSection>
      ) : null}
    </StructuredCardShell>
  );
}
