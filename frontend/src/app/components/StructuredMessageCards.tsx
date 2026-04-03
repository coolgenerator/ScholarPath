import React from 'react';
import ReactMarkdown from 'react-markdown';
import { useApp } from '../../context/AppContext';
import type {
  ComparisonMetricKey,
  OfferCompareSchool,
  OfferCompareViewModel,
  WhatIfDeltaItem,
  WhatIfViewModel,
} from '../../lib/types';
import { ExpandableMarkdown } from './ExpandableMarkdown';
import {
  StructuredCardHeader,
  StructuredCardSection,
  StructuredCardShell,
} from './StructuredCardPrimitives';

const METRIC_META: Record<
  ComparisonMetricKey,
  {
    label: { en: string; zh: string };
    better: 'higher' | 'lower' | 'truthy';
    format: 'currency' | 'percent' | 'boolean';
  }
> = {
  net_cost: {
    label: { en: 'Net Cost', zh: '年净费用' },
    better: 'lower',
    format: 'currency',
  },
  total_aid: {
    label: { en: 'Total Aid', zh: '总资助' },
    better: 'higher',
    format: 'currency',
  },
  tuition: {
    label: { en: 'Tuition', zh: '学费' },
    better: 'lower',
    format: 'currency',
  },
  total_cost: {
    label: { en: 'Total Cost', zh: '总成本' },
    better: 'lower',
    format: 'currency',
  },
  merit_scholarship: {
    label: { en: 'Merit Scholarship', zh: '奖学金' },
    better: 'higher',
    format: 'currency',
  },
  career_outlook: {
    label: { en: 'Career Outlook', zh: '职业前景' },
    better: 'higher',
    format: 'percent',
  },
  academic_fit: {
    label: { en: 'Academic Fit', zh: '学术匹配' },
    better: 'higher',
    format: 'percent',
  },
  life_satisfaction: {
    label: { en: 'Life Satisfaction', zh: '生活体验' },
    better: 'higher',
    format: 'percent',
  },
  honors_program: {
    label: { en: 'Honors Program', zh: '荣誉项目' },
    better: 'truthy',
    format: 'boolean',
  },
};

const WHAT_IF_META: Record<string, { en: string; zh: string }> = {
  admission_probability: { en: 'Admission Probability', zh: '录取概率' },
  academic_outcome: { en: 'Academic Outcome', zh: '学术结果' },
  career_outcome: { en: 'Career Outcome', zh: '职业结果' },
  life_satisfaction: { en: 'Life Satisfaction', zh: '生活满意度' },
  phd_probability: { en: 'PhD Probability', zh: '博士走向' },
};

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return '—';
  return `$${value.toLocaleString()}`;
}

function formatPercent(value: number | null | undefined): string {
  if (value == null) return '—';
  return `${Math.round(value * 100)}%`;
}

function formatMetricValue(
  key: ComparisonMetricKey,
  value: number | boolean | null | undefined,
  isCn: boolean,
): string {
  if (value == null) return '—';
  if (METRIC_META[key].format === 'currency') return formatCurrency(value as number);
  if (METRIC_META[key].format === 'percent') return formatPercent(value as number);
  return value ? (isCn ? '是' : 'Yes') : (isCn ? '否' : 'No');
}

function getMetricWinners(schools: OfferCompareSchool[], key: ComparisonMetricKey): Set<string> {
  const values = schools
    .map((school) => ({ id: school.id, value: school.metrics[key] }))
    .filter((item) => item.value != null);
  if (values.length === 0) return new Set();

  const better = METRIC_META[key].better;
  let winnerValue: number | boolean | null = values[0].value ?? null;
  values.forEach((item) => {
    if (item.value == null || winnerValue == null) return;
    if (better === 'lower' && typeof item.value === 'number' && typeof winnerValue === 'number' && item.value < winnerValue) {
      winnerValue = item.value;
    }
    if (better === 'higher' && typeof item.value === 'number' && typeof winnerValue === 'number' && item.value > winnerValue) {
      winnerValue = item.value;
    }
    if (better === 'truthy' && Boolean(item.value)) {
      winnerValue = true;
    }
  });

  return new Set(
    values
      .filter((item) => item.value === winnerValue)
      .map((item) => item.id),
  );
}

function localizeWhatIfLabel(key: string, isCn: boolean): string {
  const meta = WHAT_IF_META[key];
  if (meta) return isCn ? meta.zh : meta.en;
  return key.replace(/_/g, ' ');
}

function localizeOfferBadge(badge: string, isCn: boolean): string {
  const normalized = badge.trim().toLowerCase();
  if (!isCn) {
    return normalized === 'honors' ? 'Honors' : badge;
  }

  if (normalized === 'admitted') return '已录取';
  if (normalized === 'committed') return '已确认';
  if (normalized === 'waitlisted') return '候补';
  if (normalized === 'deferred') return '延期';
  if (normalized === 'denied') return '拒录';
  if (normalized === 'honors') return '荣誉项目';
  return badge;
}

function resolveOfferCompareDescription(
  description: string | undefined,
  source: OfferCompareViewModel['source'],
  isCn: boolean,
): string {
  const text = description ?? '';
  const isGenericAdvisorSummary = /structured side-by-side comparison extracted from the advisor response\.?/i.test(text);
  const isGenericOffersOverview = /financial and package overview across all admitted offers currently on file\.?/i.test(text);

  if (isCn) {
    if (!text || isGenericAdvisorSummary) {
      return source === 'chat'
        ? '把顾问回复里的成本、资助和结果差异重新编排成一张适合横向决策的比较卡。'
        : '把已录取结果中的成本、资助和项目条件压成一张更利于决策的比较卡。';
    }
    if (isGenericOffersOverview) {
      return '把已录取结果中的成本、资助和项目条件压成一张更利于决策的比较卡。';
    }
    return text;
  }

  if (!text || isGenericAdvisorSummary) {
    return source === 'chat'
      ? 'A tighter comparison surface for cost, aid, and outcome tradeoffs pulled from the advisor response.'
      : 'A cleaner decision surface for comparing cost, aid, and offer-level conditions across your admitted options.';
  }
  if (isGenericOffersOverview) {
    return 'A cleaner decision surface for comparing cost, aid, and offer-level conditions across your admitted options.';
  }
  return text;
}

function buildLocalizedSuggestions(data: WhatIfViewModel, isCn: boolean): string[] {
  if (data.suggestions.length > 0) return data.suggestions;
  const ordered = [...data.deltas].sort((a, b) => Math.abs(b.value) - Math.abs(a.value));
  if (ordered.length === 0) return [];

  const strongest = ordered[0];
  const positives = ordered.filter((item) => item.value > 0);
  const negatives = ordered.filter((item) => item.value < 0);

  if (isCn) {
    const suggestions = [
      strongest.value > 0
        ? `优先把 ${localizeWhatIfLabel(strongest.key, true)} 的提升兑现成真实动作，它是这次变化里最显著的正向信号。`
        : `先处理 ${localizeWhatIfLabel(strongest.key, true)} 的下滑，它是当前情景里最明显的风险来源。`,
      positives.length > 0 && negatives.length > 0
        ? '这个情景同时带来收益和代价，执行时要把正向增益和负向副作用一起管理。'
        : '如果你准备沿这个方向推进，建议把当前情景拆成 1 到 2 个最容易落地的行动。',
      /scholarship|financial aid|奖学金|资助/i.test(data.explanation ?? '')
        ? '可以优先准备奖学金谈判、补充财务证明或争取额外资助。'
        : '执行前再结合真实申请约束复核一次，确认模拟增益能否在现实里成立。',
    ];
    return suggestions;
  }

  return [
    strongest.value > 0
      ? `Prioritize the uplift in ${localizeWhatIfLabel(strongest.key, false)} first; it is the clearest upside in this scenario.`
      : `Address the drop in ${localizeWhatIfLabel(strongest.key, false)} first; it is the biggest downside in this scenario.`,
    positives.length > 0 && negatives.length > 0
      ? 'This scenario has both upside and tradeoffs, so plan for the gain and the downside together.'
      : 'If you want to act on this scenario, convert the strongest shift into one or two concrete next steps.',
    /scholarship|financial aid|奖学金|资助/i.test(data.explanation ?? '')
      ? 'A scholarship appeal or stronger aid conversation is the most actionable next move.'
      : 'Before committing to the scenario, validate that the simulated gain is realistic under your real constraints.',
  ];
}

function DeltaPill({ delta, isCn }: { delta: WhatIfDeltaItem; isCn: boolean }) {
  const positive = delta.value > 0;
  const label = localizeWhatIfLabel(delta.key, isCn);
  return (
    <div className={`structured-delta-pill ${positive ? 'structured-delta-pill-positive' : 'structured-delta-pill-negative'}`}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="structured-delta-pill-kicker">
            {isCn ? '变化项' : 'Delta'}
          </div>
          <div className="structured-delta-pill-title">{label}</div>
        </div>
        <div className={`structured-delta-pill-value ${positive ? 'text-emerald-700' : 'text-rose-700'}`}>
          <div className="text-2xl font-black tracking-tight">{positive ? '+' : ''}{Math.round(delta.value * 100)}%</div>
          <div className="text-[10px] font-bold uppercase tracking-[0.12em]">
            {positive ? (isCn ? '提升' : 'Up') : (isCn ? '下滑' : 'Down')}
          </div>
        </div>
      </div>
    </div>
  );
}

export function OfferCompareCard({ data }: { data: OfferCompareViewModel }) {
  const { locale } = useApp();
  const isCn = locale === 'zh';
  const localizedDescription = resolveOfferCompareDescription(data.description, data.source, isCn);

  return (
    <StructuredCardShell className="mt-3 overflow-hidden">
      <StructuredCardHeader
        kicker={data.source === 'chat' ? (isCn ? '顾问整理' : 'Advisor Summary') : (isCn ? '录取整理' : 'Offers Overview')}
        title={isCn ? 'Offer 横向比较' : 'Offer Comparison'}
        description={localizedDescription}
        badge={`${data.schools.length} ${isCn ? '所学校' : 'schools'}`}
        aside={
          <div className="structured-card-inline-note sm:hidden">
            {isCn ? '左右滑动查看更多学校' : 'Swipe to compare more schools'}
          </div>
        }
      />

      <StructuredCardSection
        className="pb-3"
        title={isCn ? '本轮比较对象' : 'Schools in this comparison'}
      >
        <div className="structured-compare-school-strip">
          {data.schools.map((school) => (
            <div key={school.id} className="structured-compare-school-tile">
              <div className="structured-compare-school-title">{school.schoolName}</div>
              {(school.badges ?? []).length > 0 ? (
                <div className="structured-compare-school-badges">
                  {(school.badges ?? []).map((badge) => (
                    <span key={badge} className="structured-compare-school-badge">
                      {localizeOfferBadge(badge, isCn)}
                    </span>
                  ))}
                </div>
              ) : null}
            </div>
          ))}
        </div>
      </StructuredCardSection>

      <div className="offer-compare-scroll-wrap">
        <div className="offer-compare-fade-left" />
        <div className="offer-compare-fade-right" />
        <div className="offer-compare-scroll">
        <div className="offer-compare-table" style={{ '--offer-column-count': data.schools.length } as React.CSSProperties}>
          <div className="offer-compare-row offer-compare-header">
            <div className="offer-compare-metric-cell">
              {isCn ? '比较维度' : 'Metric'}
            </div>
            {data.schools.map((school) => (
              <div key={school.id} className="offer-compare-school-head">
                <div className="offer-compare-school-name">{school.schoolName}</div>
                {(school.badges ?? []).length > 0 && (
                  <div className="offer-compare-school-badges">
                    {(school.badges ?? []).map((badge) => (
                      <span key={badge} className="offer-compare-school-badge">
                        {localizeOfferBadge(badge, isCn)}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>

          {data.metricOrder.map((metricKey) => {
            const winners = getMetricWinners(data.schools, metricKey);
            return (
              <div key={metricKey} className="offer-compare-row">
                <div className="offer-compare-metric-cell">
                  {isCn ? METRIC_META[metricKey].label.zh : METRIC_META[metricKey].label.en}
                </div>
                {data.schools.map((school) => {
                  const value = school.metrics[metricKey];
                  const highlighted = winners.has(school.id) && value != null;
                  return (
                    <div key={school.id} className={`offer-compare-value-cell ${highlighted ? 'offer-compare-value-best' : ''}`}>
                      {highlighted ? (
                        <span className="offer-compare-best-pill">
                          {isCn ? '最佳' : 'Best'}
                        </span>
                      ) : null}
                      <span className="offer-compare-value-main">{formatMetricValue(metricKey, value, isCn)}</span>
                    </div>
                  );
                })}
              </div>
            );
          })}
        </div>
      </div>
      </div>

      {data.summary && (
        <StructuredCardSection className="structured-card-footer" title={
          <span className="structured-card-kicker">
            {isCn ? '推荐结论' : 'Recommendation'}
          </span>
        }>
          <div className="structured-card-markdown">
            <ReactMarkdown>{data.summary}</ReactMarkdown>
          </div>
        </StructuredCardSection>
      )}
    </StructuredCardShell>
  );
}

interface WhatIfDeltaCardProps {
  data: WhatIfViewModel;
  kicker?: { en: string; zh: string };
  title?: { en: string; zh: string };
  description?: { en: string; zh: string };
  showSuggestions?: boolean;
}

export function WhatIfDeltaCard({
  data,
  kicker,
  title,
  description,
  showSuggestions = true,
}: WhatIfDeltaCardProps) {
  const { locale } = useApp();
  const isCn = locale === 'zh';
  const suggestions = buildLocalizedSuggestions(data, isCn);
  const localizedKicker = kicker ? (isCn ? kicker.zh : kicker.en) : (isCn ? '顾问模拟' : 'Scenario Analysis');
  const localizedTitle = title ? (isCn ? title.zh : title.en) : (isCn ? '情景影响' : 'What-If Impact');
  const localizedDescription = description
    ? (isCn ? description.zh : description.en)
    : (isCn ? '聚焦关键变化、原因解释与下一步动作。' : 'Track the most important deltas, why they move, and what to do next.');

  return (
    <StructuredCardShell className="mt-3">
      <StructuredCardHeader
        kicker={localizedKicker}
        title={localizedTitle}
        description={localizedDescription}
        badge={`${data.deltas.length} ${isCn ? '项变化' : 'deltas'}`}
      />

      <StructuredCardSection title={isCn ? '变化概览' : 'Delta Overview'}>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          {data.deltas
            .slice()
            .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
            .map((delta) => (
              <DeltaPill key={delta.key} delta={delta} isCn={isCn} />
            ))}
        </div>
      </StructuredCardSection>

      {data.explanation && (
        <StructuredCardSection title={isCn ? '影响解释' : 'Impact Explanation'}>
          <ExpandableMarkdown className="structured-card-markdown" content={data.explanation} />
        </StructuredCardSection>
      )}

      {showSuggestions && suggestions.length > 0 && (
        <StructuredCardSection title={isCn ? '行动建议' : 'Action Suggestions'}>
          <div className="structured-suggestion-list">
            {suggestions.map((suggestion) => (
              <div key={suggestion} className="structured-suggestion-item">
                {suggestion}
              </div>
            ))}
          </div>
        </StructuredCardSection>
      )}
    </StructuredCardShell>
  );
}
