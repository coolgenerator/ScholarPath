import React from 'react';
import ReactMarkdown from 'react-markdown';
import { useApp } from '../../context/AppContext';
import type {
  AnswerSynthesisPayload,
  ComparisonMetricKey,
  OfferCompareSchool,
  OfferCompareViewModel,
  ProfilePatchProposalPayload,
  ProfilePatchResultPayload,
  ProfileSnapshotPayload,
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

function localizeSynthesisAngle(angle: string, isCn: boolean): string {
  const normalized = angle.trim().toLowerCase();
  if (normalized === 'recommendation') return isCn ? '选校结论' : 'Recommendation';
  if (normalized === 'comparison') return isCn ? '对比视角' : 'Comparison';
  if (normalized === 'scenario') return isCn ? '情景变化' : 'Scenario';
  if (normalized === 'timeline') return isCn ? '节奏策略' : 'Timeline';
  if (normalized === 'school_facts') return isCn ? '学校事实' : 'School Facts';
  if (normalized === 'profile') return isCn ? '档案约束' : 'Profile';
  if (normalized === 'intake') return isCn ? '信息补齐' : 'Intake';
  return isCn ? '综合视角' : 'General';
}

function localizeActionPriority(priority: string, isCn: boolean): string {
  const normalized = priority.trim().toLowerCase();
  if (normalized === 'high') return isCn ? '高优先级' : 'High';
  if (normalized === 'medium') return isCn ? '中优先级' : 'Medium';
  if (normalized === 'low') return isCn ? '低优先级' : 'Low';
  return priority;
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

export function AnswerSynthesisCard({ data }: { data: AnswerSynthesisPayload }) {
  const { locale } = useApp();
  const isCn = locale === 'zh';
  const perspectives = Array.isArray(data.perspectives) ? data.perspectives : [];
  const actions = Array.isArray(data.actions) ? data.actions : [];
  const risks = Array.isArray(data.risks_missing) ? data.risks_missing : [];
  const degraded = data.degraded ?? { has_degraded: false, caps: [], reason_codes: [], retry_hint: '' };

  return (
    <StructuredCardShell className="mt-3">
      <StructuredCardHeader
        kicker={isCn ? '综合答案' : 'Answer Synthesis'}
        title={isCn ? '本轮主结论' : 'Primary Answer'}
        description={data.summary}
        badge={isCn ? '卡片优先' : 'Card-first'}
      />

      <StructuredCardSection title={isCn ? '结论' : 'Conclusion'}>
        <div className="rounded-xl border border-outline-variant/10 bg-white/90 px-3 py-2 text-sm leading-6 text-on-surface">
          {data.conclusion}
        </div>
      </StructuredCardSection>

      <StructuredCardSection title={isCn ? '多角度依据' : 'Perspectives'}>
        <div className="grid grid-cols-1 gap-2">
          {perspectives.length > 0 ? perspectives.map((item, index) => (
            <div key={`${item.angle}-${index}`} className="rounded-xl border border-outline-variant/10 bg-white/90 px-3 py-2.5">
              <div className="mb-1 flex flex-wrap items-center gap-2">
                <span className="rounded-full border border-primary/12 bg-primary/5 px-2 py-0.5 text-[10px] font-bold uppercase tracking-[0.1em] text-primary">
                  {localizeSynthesisAngle(item.angle, isCn)}
                </span>
                <span className="text-[10px] font-semibold text-on-surface-variant/60">
                  {isCn ? '置信度' : 'Confidence'} {Math.round((item.confidence ?? 0) * 100)}%
                </span>
              </div>
              <div className="text-sm font-semibold text-on-surface">{item.claim}</div>
              <div className="mt-1 text-xs leading-5 text-on-surface-variant/75">{item.evidence}</div>
            </div>
          )) : (
            <div className="text-sm text-on-surface-variant/65">
              {isCn ? '暂无可展示的角度依据。' : 'No perspective evidence available.'}
            </div>
          )}
        </div>
      </StructuredCardSection>

      <StructuredCardSection title={isCn ? '行动清单' : 'Action Plan'}>
        <div className="grid grid-cols-1 gap-2">
          {actions.length > 0 ? actions.map((action, index) => (
            <div key={`${action.step}-${index}`} className="rounded-xl border border-outline-variant/10 bg-white/90 px-3 py-2.5">
              <div className="mb-1 flex flex-wrap items-center gap-2">
                <span className="rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[10px] font-bold uppercase tracking-[0.1em] text-emerald-700">
                  {localizeActionPriority(action.priority, isCn)}
                </span>
              </div>
              <div className="text-sm font-semibold text-on-surface">{action.step}</div>
              {action.rationale && (
                <div className="mt-1 text-xs leading-5 text-on-surface-variant/75">{action.rationale}</div>
              )}
            </div>
          )) : (
            <div className="text-sm text-on-surface-variant/65">
              {isCn ? '暂无行动建议。' : 'No action suggestions.'}
            </div>
          )}
        </div>
      </StructuredCardSection>

      <StructuredCardSection title={isCn ? '风险与缺失' : 'Risks & Missing'}>
        <div className="flex flex-wrap gap-2">
          {risks.length > 0 ? risks.map((risk) => (
            <span key={risk} className="rounded-full border border-amber-300 bg-amber-50 px-3 py-1 text-xs font-semibold text-amber-800">
              {risk}
            </span>
          )) : (
            <span className="text-sm text-on-surface-variant/65">—</span>
          )}
        </div>
      </StructuredCardSection>

      {degraded.has_degraded && (
        <StructuredCardSection title={isCn ? '降级提示（可折叠）' : 'Degraded Nodes (Collapsed)'}>
          <details className="rounded-xl border border-outline-variant/12 bg-surface-container-lowest/70 px-3 py-2">
            <summary className="cursor-pointer text-sm font-semibold text-on-surface">
              {isCn ? `本轮有 ${degraded.caps.length} 个节点降级` : `${degraded.caps.length} node(s) degraded this turn`}
            </summary>
            <div className="mt-2 space-y-2 text-xs leading-5 text-on-surface-variant/75">
              <div>{isCn ? '降级能力' : 'Capabilities'}：{(degraded.caps ?? []).join(', ') || '—'}</div>
              <div>{isCn ? '原因码' : 'Reason codes'}：{(degraded.reason_codes ?? []).join(', ') || '—'}</div>
              {degraded.retry_hint ? (
                <div>{isCn ? '重试建议' : 'Retry hint'}：{degraded.retry_hint}</div>
              ) : null}
            </div>
          </details>
        </StructuredCardSection>
      )}
    </StructuredCardShell>
  );
}

export function ErrorStateCard({ message }: { message: string }) {
  const { locale } = useApp();
  const isCn = locale === 'zh';
  const fallback = isCn ? '本轮执行失败，请稍后重试。' : 'This turn failed. Please try again.';
  const safeMessage = message.trim() || fallback;

  return (
    <StructuredCardShell className="mt-3 border-rose-200/70 bg-rose-50/85">
      <StructuredCardHeader
        kicker={isCn ? '执行回滚' : 'Turn Rollback'}
        title={isCn ? '请求执行失败' : 'Request Failed'}
        description={isCn ? '本轮未保留部分结果，请调整后重试。' : 'No partial results were kept for this turn.'}
        badge={isCn ? '错误' : 'Error'}
      />
      <StructuredCardSection title={isCn ? '错误详情' : 'Error Details'}>
        <div className="rounded-xl border border-rose-200/70 bg-white/90 px-3 py-2 text-sm text-rose-900">
          {safeMessage}
        </div>
      </StructuredCardSection>
    </StructuredCardShell>
  );
}

export function ProfileSnapshotCard({ data }: { data: ProfileSnapshotPayload }) {
  const { locale } = useApp();
  const isCn = locale === 'zh';
  const portfolio = data.portfolio;
  const completion = data.completion ?? portfolio?.completion;
  const majors = portfolio?.academics?.intended_majors ?? [];

  return (
    <StructuredCardShell className="mt-3">
      <StructuredCardHeader
        kicker={isCn ? '档案快照' : 'Profile Snapshot'}
        title={isCn ? '当前申请画像' : 'Current Student Profile'}
        description={isCn ? '用于本轮推荐与策略推理的档案上下文。' : 'Portfolio context used by this turn.'}
        badge={completion?.profile_completed ? (isCn ? '已完整' : 'Complete') : (isCn ? '待完善' : 'Incomplete')}
      />
      <StructuredCardSection title={isCn ? '关键字段' : 'Key Fields'}>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
          <div className="rounded-xl border border-outline-variant/10 bg-white/90 px-3 py-2 text-sm">
            <div className="text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant/60">{isCn ? '姓名' : 'Name'}</div>
            <div className="mt-1 font-semibold text-on-surface">{portfolio?.identity?.name ?? '—'}</div>
          </div>
          <div className="rounded-xl border border-outline-variant/10 bg-white/90 px-3 py-2 text-sm">
            <div className="text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant/60">GPA</div>
            <div className="mt-1 font-semibold text-on-surface">{portfolio?.academics?.gpa ?? '—'} / {portfolio?.academics?.gpa_scale ?? '—'}</div>
          </div>
          <div className="rounded-xl border border-outline-variant/10 bg-white/90 px-3 py-2 text-sm">
            <div className="text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant/60">SAT</div>
            <div className="mt-1 font-semibold text-on-surface">{portfolio?.academics?.sat_total ?? '—'}</div>
          </div>
          <div className="rounded-xl border border-outline-variant/10 bg-white/90 px-3 py-2 text-sm">
            <div className="text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant/60">{isCn ? '预算' : 'Budget'}</div>
            <div className="mt-1 font-semibold text-on-surface">
              {typeof portfolio?.finance?.budget_usd === 'number' ? `$${portfolio.finance.budget_usd.toLocaleString()}` : '—'}
            </div>
          </div>
        </div>
      </StructuredCardSection>
      <StructuredCardSection title={isCn ? '目标专业' : 'Intended Majors'}>
        <div className="flex flex-wrap gap-2">
          {majors.length > 0 ? majors.map((major) => (
            <span key={major} className="rounded-full border border-outline-variant/10 bg-surface-container-low px-3 py-1 text-xs font-semibold text-on-surface">
              {major}
            </span>
          )) : (
            <span className="text-sm text-on-surface-variant/60">—</span>
          )}
        </div>
      </StructuredCardSection>
    </StructuredCardShell>
  );
}

interface ProfilePatchProposalCardProps {
  data: ProfilePatchProposalPayload;
  onConfirm: (command: string) => void;
  onReedit: (command: string) => void;
}

export function ProfilePatchProposalCard({ data, onConfirm, onReedit }: ProfilePatchProposalCardProps) {
  const { locale } = useApp();
  const isCn = locale === 'zh';
  const fieldPaths = Object.entries(data.patch ?? {}).flatMap(([group, fields]) => {
    if (!fields || typeof fields !== 'object') return [];
    return Object.keys(fields as Record<string, unknown>).map((field) => `${group}.${field}`);
  });
  const missingFields = Array.isArray(data.missing_fields)
    ? data.missing_fields.filter((item) => String(item).trim().length > 0)
    : [];

  return (
    <StructuredCardShell className="mt-3">
      <StructuredCardHeader
        kicker={isCn ? '待确认修改' : 'Pending Patch'}
        title={isCn ? 'Profile 修改提案' : 'Profile Update Proposal'}
        description={data.summary}
        badge={`ID ${data.proposal_id.slice(0, 8)}`}
      />
      <StructuredCardSection title={isCn ? '拟修改字段' : 'Proposed Field Changes'}>
        <div className="flex flex-wrap gap-2">
          {fieldPaths.length > 0 ? fieldPaths.map((field) => (
            <span key={field} className="rounded-full border border-primary/15 bg-primary/5 px-3 py-1 text-xs font-semibold text-primary">
              {field}
            </span>
          )) : (
            <span className="text-sm text-on-surface-variant/60">{isCn ? '未识别到可修改字段' : 'No editable fields detected'}</span>
          )}
        </div>
      </StructuredCardSection>
      {missingFields.length > 0 && (
        <StructuredCardSection title={isCn ? '仍需补充' : 'Still Needed'}>
          <div className="flex flex-wrap gap-2">
            {missingFields.map((field) => (
              <span key={field} className="rounded-full border border-amber-300 bg-amber-50 px-3 py-1 text-xs font-semibold text-amber-800">
                {field}
              </span>
            ))}
          </div>
        </StructuredCardSection>
      )}
      <StructuredCardSection title={isCn ? '操作' : 'Actions'}>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => onConfirm(data.confirm_command)}
            className="rounded-xl bg-primary px-3 py-2 text-xs font-bold text-on-primary shadow-sm hover:brightness-110"
          >
            {isCn ? '确认提交' : 'Confirm Apply'}
          </button>
          <button
            type="button"
            onClick={() => onReedit(data.reedit_command)}
            className="rounded-xl border border-outline-variant/20 bg-white px-3 py-2 text-xs font-bold text-on-surface hover:bg-surface-container-low"
          >
            {isCn ? '重新编辑' : 'Re-edit'}
          </button>
        </div>
      </StructuredCardSection>
    </StructuredCardShell>
  );
}

export function ProfilePatchResultCard({ data }: { data: ProfilePatchResultPayload }) {
  const { locale } = useApp();
  const isCn = locale === 'zh';

  return (
    <StructuredCardShell className="mt-3">
      <StructuredCardHeader
        kicker={isCn ? '修改结果' : 'Patch Result'}
        title={isCn ? 'Profile 更新完成' : 'Profile Updated'}
        description={isCn ? '已应用确认后的档案修改。' : 'Confirmed patch was applied.'}
        badge={data.applied ? (isCn ? '成功' : 'Applied') : (isCn ? '未应用' : 'Not Applied')}
      />
      <StructuredCardSection title={isCn ? '变更字段' : 'Changed Fields'}>
        <div className="flex flex-wrap gap-2">
          {(data.changed_fields ?? []).length > 0 ? data.changed_fields.map((field) => (
            <span key={field} className="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-800">
              {field}
            </span>
          )) : (
            <span className="text-sm text-on-surface-variant/60">—</span>
          )}
        </div>
      </StructuredCardSection>
    </StructuredCardShell>
  );
}
